// Package jdbc_test 对 JDBC 参考实现做集成测试（MySQL / PostgreSQL 双库可跑）。
//
// 用法：go test ./repository/jdbc/           # 默认 MySQL（开发服务器）
//
//	JEFFLOW_DB_DRIVER=pgx JEFFLOW_DB_DSN='postgres://postgres:pwd@host:5432/jeeflow' go test ./repository/jdbc/
//
// 前置条件：
//   - 目标库已建 5 张 wf_* 表（建表 SQL 各语言自带：repository/jdbc/schema/schema-<db>.sql；
//     维护者改 jeeflow-java 仓 resources 后用 scripts/sync-schema.sh 分发）
//   - 连接信息用环境变量覆盖（使用者指向自己的库），默认开发服务器 MySQL
//
// 测试数据用固定 define ID=900001，可重复执行（开头清理）。
package jdbc_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

const defineID = int64(900001)

// driver / dsn 环境变量可覆盖（默认开发服务器 MySQL）
func testDriver() string {
	if d := os.Getenv("JEFFLOW_DB_DRIVER"); d != "" {
		return d
	}
	return "mysql"
}

func testDSN() string {
	if d := os.Getenv("JEFFLOW_DB_DSN"); d != "" {
		return d
	}
	return "root:8Eli#gr#AUk@tcp(192.168.1.160:3306)/jeeflow?parseTime=true&charset=utf8mb4"
}

// ph 测试直查 SQL 占位符转换（与 jdbc.ConvertPlaceholder 同一约定）
func ph(sql string) string {
	style := "?"
	if testDriver() == "pgx" {
		style = "$n"
	}
	return jdbc.ConvertPlaceholder(sql, style)
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(testDriver(), testDSN())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db(%s jeeflow): %v", testDriver(), err)
	}
	return db
}

// cleanup 删除本测试的固定数据（幂等）
func cleanup(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		"DELETE FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?))",
		"DELETE FROM wf_process_cc_instance WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?)",
		"DELETE FROM wf_process_task WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?)",
		"DELETE FROM wf_process_instance WHERE process_define_id = ?",
		"DELETE FROM wf_process_define WHERE id = ?",
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, ph(s), defineID); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
}

// loadFlow 读取 jeeflow-java 共享测试流程 JSON
func loadFlow(t *testing.T, name string) []byte {
	t.Helper()
	candidates := []string{
		"../../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
		"../../../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
		"../../../../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err == nil {
			return data
		}
	}
	t.Fatalf("flow json not found: %s", name)
	return nil
}

func insertDefine(t *testing.T, db *sql.DB, name string, content []byte) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	var display, typ string
	var raw map[string]interface{}
	if err := json.Unmarshal(content, &raw); err == nil {
		display, _ = raw["displayName"].(string)
		typ, _ = raw["type"].(string)
	}
	if _, err := db.ExecContext(ctx, ph(
		"INSERT INTO wf_process_define (id, name, display_name, type, state, content, version, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,1,?,1,?,?,?,?)"),
		defineID, name, display, typ, content, now, "go-test", now, "go-test"); err != nil {
		t.Fatalf("insert define: %v", err)
	}
}

func newEngine(t *testing.T, repo spi.ProcessRepository) *engine.EngineImpl {
	t.Helper()
	userProv := &noopUserProvider{}
	idGen := &tsIDGen{base: time.Now().UnixMilli() * 1000}
	return engine.New(repo, userProv, idGen, nil)
}

// ─── 测试用 SPI 实现 ────────────────────────────────────────────────────────────

type noopUserProvider struct{}

func (*noopUserProvider) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: userID}, nil
}

// tsIDGen 时间戳+序号生成器（测试用）
type tsIDGen struct {
	base int64
	seq  int
}

func (g *tsIDGen) NextID() int64 {
	g.seq++
	return g.base + int64(g.seq)
}

// ─── 主链路测试：启动→apply→task1→结束，全程走 MySQL ─────────────────────────

func TestFlowSimpleEndToEnd(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)

	// ① 启动：start → apply（applicant）
	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "zhangsan", map[string]interface{}{"amount": "1000", "BUSINESS_NO": "BIZ-GO-001"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if inst == nil || inst.State != model.InstanceStateDoing {
		t.Fatalf("instance state = %v, want doing", inst.State)
	}
	if inst.BusinessNo == "" {
		t.Fatalf("businessNo should be generated")
	}
	doing, err := repo.FindDoingTasks(ctx, inst.ID, nil)
	if err != nil {
		t.Fatalf("find doing: %v", err)
	}
	if len(doing) != 1 || doing[0].TaskName != "apply" {
		t.Fatalf("doing tasks = %+v, want [apply]", doing)
	}
	if len(doing[0].ActorIDs) != 1 || doing[0].ActorIDs[0] != "zhangsan" {
		t.Fatalf("apply actors = %v, want [zhangsan](applicant→发起人)", doing[0].ActorIDs)
	}

	// ② 完成 apply（startAndExecute 语义）→ task1（leader）
	applyTask := doing[0]
	inst, err = eng.ExecuteProcessTask(ctx, applyTask.ID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("complete apply: %v", err)
	}
	done, _ := repo.FindDoneTasks(ctx, inst.ID, nil)
	if len(done) != 1 || done[0].TaskName != "apply" || done[0].ActorID != "zhangsan" {
		t.Fatalf("done tasks = %+v, want [apply by zhangsan]", done)
	}
	if done[0].FinishTime == nil {
		t.Fatalf("apply finishTime should not be nil")
	}
	doing, _ = repo.FindDoingTasks(ctx, inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("doing tasks = %+v, want [task1]", doing)
	}
	if !doing[0].IsAllowed("leader") || doing[0].IsAllowed("zhangsan") {
		t.Fatalf("task1 actor check failed: %v", doing[0].ActorIDs)
	}

	// ③ 完成 task1 → end → 实例完成
	inst, err = eng.ExecuteProcessTask(ctx, doing[0].ID, "leader", map[string]interface{}{"comment": "ok"})
	if err != nil {
		t.Fatalf("complete task1: %v", err)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("instance state = %v, want done", inst.State)
	}

	// ④ 重新连接验证持久化（绕过内存缓存，直查数据库）
	db2 := openDB(t)
	defer db2.Close()
	var state int
	if err := db2.QueryRowContext(ctx, ph("SELECT state FROM wf_process_instance WHERE id = ?"), inst.ID).Scan(&state); err != nil {
		t.Fatalf("persist instance: %v", err)
	}
	if state != int(model.InstanceStateDone) {
		t.Fatalf("persisted state = %d, want 20", state)
	}
	var taskCnt, actorCnt int
	_ = db2.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id = ?"), inst.ID).Scan(&taskCnt)
	_ = db2.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id = ?)"), inst.ID).Scan(&actorCnt)
	if taskCnt != 2 || actorCnt != 2 {
		t.Fatalf("persisted task=%d actor=%d, want 2/2", taskCnt, actorCnt)
	}

	// ⑤ 重新加载实例：任务齐全、变量完整
	repo2 := jdbc.New(db2)
	inst2, err := repo2.FindInstanceByID(ctx, inst.ID)
	if err != nil {
		t.Fatalf("reload instance: %v", err)
	}
	if inst2 == nil || inst2.State != model.InstanceStateDone || inst2.BusinessNo == "" {
		t.Fatalf("reloaded instance wrong: %+v", inst2)
	}
	if v, ok := inst2.Variables["amount"].(string); !ok || v != "1000" {
		t.Fatalf("variables lost amount: %v", inst2.Variables)
	}
	allTasks, err := repo2.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		t.Fatalf("history tasks: %v", err)
	}
	if len(allTasks) != 2 {
		t.Fatalf("history tasks = %d, want 2", len(allTasks))
	}
	if len(allTasks[0].ActorIDs) == 0 {
		t.Fatalf("actor relation not persisted: %+v", allTasks[0])
	}
}

// ─── 权限校验：非参与者操作任务被拒（负向） ────────────────────────────────────

func TestPermissionDenied(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)

	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(ctx, inst.ID, nil)
	// applicant 任务被非参与者完成 → 报错
	if _, err := eng.ExecuteProcessTask(ctx, doing[0].ID, "hacker", nil); err == nil {
		t.Fatalf("expect permission error, got nil")
	}
	// 任务状态未变
	reloaded, _ := repo.FindTaskByID(ctx, doing[0].ID)
	if reloaded.TaskState != model.TaskStateDoing {
		t.Fatalf("task state changed after denied op: %v", reloaded.TaskState)
	}
}

// ─── 事务（spec §7.4）：绑定连接 + 回滚 ───────────────────────────────────────

func TestWithTx(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)

	// ① 事务内提交：实例 + 抄送同一连接落库
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		inst := &model.ProcessInstance{
			ID: 900002, DefineID: defineID, State: model.InstanceStateDoing, Operator: "zhangsan",
			BusinessNo: "TXN-001", Variables: map[string]interface{}{"k": "v"},
			CreateTime: time.Now(), UpdateTime: time.Now(), CreateUser: "t", UpdateUser: "t",
		}
		if err := repo.SaveInstance(ctx, inst); err != nil {
			return err
		}
		if err := repo.CreateCcInstance(ctx, 900002, "zhangsan", "lisi", "wangwu"); err != nil {
			return err
		}
		// 事务内可读（连接绑定生效）
		got, err := repo.FindInstanceByID(ctx, 900002)
		if err != nil || got == nil {
			return fmt.Errorf("tx readback: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tx commit: %v", err)
	}
	var n int
	_ = db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_instance WHERE id = 900002")).Scan(&n)
	if n != 1 {
		t.Fatalf("tx commit not persisted, n=%d", n)
	}
	_ = db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = 900002")).Scan(&n)
	if n != 2 {
		t.Fatalf("cc rows = %d, want 2", n)
	}

	// ② 事务回滚：回调报错 → 全部回滚
	err = repo.WithTx(ctx, func(ctx context.Context) error {
		inst := &model.ProcessInstance{
			ID: 900003, DefineID: defineID, State: model.InstanceStateDoing, Operator: "zhangsan",
			CreateTime: time.Now(), UpdateTime: time.Now(), CreateUser: "t", UpdateUser: "t",
		}
		if err := repo.SaveInstance(ctx, inst); err != nil {
			return err
		}
		if err := repo.CreateCcInstance(ctx, 900003, "zhangsan", "lisi"); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatalf("expect tx error, got nil")
	}
	_ = db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_instance WHERE id = 900003")).Scan(&n)
	if n != 0 {
		t.Fatalf("rollback failed, instance rows = %d", n)
	}
	_ = db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = 900003")).Scan(&n)
	if n != 0 {
		t.Fatalf("rollback failed, cc rows = %d", n)
	}
	_ = db.QueryRowContext(ctx, ph("DELETE FROM wf_process_instance WHERE id = 900002")).Scan(&n)
	_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_cc_instance WHERE process_instance_id = 900002"))
}
