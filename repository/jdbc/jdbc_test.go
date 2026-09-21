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
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/internal/flowsutil"
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

// loadFlow 读取本仓 flows/ 测试流程 JSON（flowsutil 已在维护者机器上镜像 Java 源）
func loadFlow(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(flowsutil.Dir(), name))
	if err != nil {
		t.Fatalf("flow json not found: %s", name)
	}
	return data
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

// ─── issues/110：SQL 仓 FindInstanceByID 水合任务 → detail 任务列表非空 ─────────
//
// 修复前：JDBC FindInstanceByID 只查 wf_process_instance 单表，Tasks 零值，
// 门面 processInstance/detail 的 tasks/activeTaskList 恒为空数组（L2-12 门禁真根因）。
// 对齐 Java findTasksByInstanceId / PHP PdoProcessRepository / C# issues/89 聚合水合。
func TestFindInstanceByIDHydratesTasks(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)

	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "zhangsan", map[string]interface{}{"BUSINESS_NO": "BIZ-GO-110"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// ① 仓储层：FindInstanceByID 水合任务 + ActorIDs
	loaded, err := repo.FindInstanceByID(ctx, inst.ID)
	if err != nil || loaded == nil {
		t.Fatalf("find instance by id: %v", err)
	}
	if len(loaded.Tasks) == 0 {
		t.Fatalf("FindInstanceByID tasks empty (issues/110), want non-empty")
	}
	for _, tk := range loaded.Tasks {
		if len(tk.ActorIDs) == 0 {
			t.Fatalf("hydrated task missing actorIds: %+v", tk)
		}
	}

	// ② 门面层：detail 的 tasks / activeTaskList 非空
	f := facade.New(eng, repo, nil)
	r := f.Flow("processInstance/detail", map[string]interface{}{"id": inst.ID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("detail code = %v, want 0: %v", r["code"], r["msg"])
	}
	data, _ := r["data"].(map[string]interface{})
	tasks, _ := data["tasks"].([]interface{})
	if len(tasks) == 0 {
		t.Fatalf("detail tasks empty (issues/110): %v", data["tasks"])
	}
	active, _ := data["activeTaskList"].([]interface{})
	if len(active) == 0 {
		t.Fatalf("detail activeTaskList empty (issues/110): %v", data["activeTaskList"])
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

// ─── 定义写操作 SPI（v1.0.1，集成反馈①） ────────────────────────────────────

func TestDefineCrud(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	ctx := context.Background()
	repo := jdbc.New(db)

	// save：ID 由仓储生成
	def := &model.ProcessDefine{Name: "go-crud", DisplayName: "CRUD 流程", Type: "test", State: 1, Version: 1, Content: []byte("{}"), UpdateUser: "tester"}
	if err := repo.SaveDefine(ctx, def); err != nil {
		t.Fatalf("save define: %v", err)
	}
	if def.ID == 0 {
		t.Fatalf("save define should assign id")
	}
	loaded, err := repo.FindDefineByID(ctx, def.ID)
	if err != nil || loaded == nil || loaded.Name != "go-crud" {
		t.Fatalf("find define: %+v err=%v", loaded, err)
	}

	// update
	loaded.DisplayName = "CRUD 流程 v2"
	loaded.Content = []byte(`{"v":2}`)
	if err := repo.UpdateDefine(ctx, loaded); err != nil {
		t.Fatalf("update define: %v", err)
	}
	updated, _ := repo.FindDefineByID(ctx, def.ID)
	if updated.DisplayName != "CRUD 流程 v2" {
		t.Fatalf("update define not persisted: %+v", updated)
	}

	// state（启用/禁用）
	if err := repo.UpdateDefineState(ctx, def.ID, 0); err != nil {
		t.Fatalf("update define state: %v", err)
	}
	if got, _ := repo.FindDefineByID(ctx, def.ID); got.State != 0 {
		t.Fatalf("state not updated: %d", got.State)
	}

	// remove
	if err := repo.RemoveDefine(ctx, def.ID); err != nil {
		t.Fatalf("remove define: %v", err)
	}
	if got, _ := repo.FindDefineByID(ctx, def.ID); got != nil {
		t.Fatalf("define should be removed")
	}
}

// ─── updateInstance 级联持久化任务状态（v1.0.1，集成反馈②） ─────────────────

func TestUpdateInstanceCascadesTasks(t *testing.T) {
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

	// 加载实例（含任务），修改任务状态后 updateInstance
	reloaded, err := repo.FindInstanceByID(ctx, inst.ID)
	if err != nil || reloaded == nil {
		t.Fatalf("reload instance: %v", err)
	}
	tasks, err := repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil || len(tasks) == 0 {
		t.Fatalf("no tasks in instance")
	}
	reloaded.Tasks = tasks
	for _, task := range tasks {
		task.TaskState = model.TaskStateAbandoned
	}
	if err := repo.UpdateInstance(ctx, reloaded); err != nil {
		t.Fatalf("update instance: %v", err)
	}

	// 重新加载验证任务状态已落库
	after, err := repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		t.Fatalf("reload after: %v", err)
	}
	for _, task := range after {
		if task.TaskState != model.TaskStateAbandoned {
			t.Fatalf("task state not cascaded: %+v", task)
		}
	}
}

// TestWithdrawPersistsTaskState30 issues/113：门面撤回须把全部进行中任务以 30（WITHDRAW）**落库**。
// 改前 Go 撤回路径写 Abandon(99)，而既有断言只验"doing 清空"——30/99 两种码值都满足，SQL 层无人验。
func TestWithdrawPersistsTaskState30(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	f := facade.New(eng, repo, nil)

	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if doing, _ := repo.FindDoingTasks(ctx, inst.ID, nil); len(doing) == 0 {
		t.Fatalf("撤回前应有 doing 任务")
	}

	r := f.Flow("processInstance/withdraw", map[string]interface{}{"id": inst.ID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("withdraw: %v", r)
	}

	// 直查库表：绕开内存对象别名，确证落库值
	rows, err := db.QueryContext(ctx, ph("SELECT task_state FROM wf_process_task WHERE process_instance_id = ?"), inst.ID)
	if err != nil {
		t.Fatalf("query task_state: %v", err)
	}
	defer rows.Close()
	states := []int{}
	for rows.Next() {
		var s int
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		states = append(states, s)
	}
	if len(states) == 0 {
		t.Fatalf("撤回后库里无任务行")
	}
	for _, s := range states {
		if s != int(model.TaskStateWithdraw) {
			t.Fatalf("撤回后库内任务态 = %d, want 30（WITHDRAW；99 是废弃码，两码不得混用）", s)
		}
	}

	if got, _ := repo.FindDoingTasks(ctx, inst.ID, nil); len(got) != 0 {
		t.Fatalf("撤回后仍有 %d 条 doing 任务", len(got))
	}
	loaded, _ := repo.FindInstanceByID(ctx, inst.ID)
	if loaded == nil || loaded.State != model.InstanceStateWithdraw {
		t.Fatalf("实例态应=Withdraw(30): %+v", loaded)
	}
}

// queryRowState 直查一行的状态列 + update_user（NULL → 空串），sqlStr 供报错定位
func queryRowState(t *testing.T, db *sql.DB, sqlStr string, args ...interface{}) (int, string) {
	t.Helper()
	var state int
	var updateUser sql.NullString
	err := db.QueryRowContext(context.Background(), ph(sqlStr), args...).Scan(&state, &updateUser)
	if err != nil {
		t.Fatalf("%s 直查失败: %v", sqlStr, err)
	}
	return state, updateUser.String
}

// facadeStartSimple 门面发起 01-simple：apply 自动完成(20) → task1 进行中（参与者 leader）
func facadeStartSimple(t *testing.T, f *facade.Facade) int64 {
	t.Helper()
	r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	id, err := strconv.ParseInt(fmt.Sprint(data["processInstanceId"]), 10, 64)
	if err != nil {
		t.Fatalf("processInstanceId 解析失败: %v (%v)", err, data["processInstanceId"])
	}
	return id
}

// taskIDOfNode 实例下指定节点的任务 id（进行中）
func taskIDOfNode(t *testing.T, repo spi.ProcessRepository, instanceID int64, node string) int64 {
	t.Helper()
	doing, err := repo.FindDoingTasks(context.Background(), instanceID, nil)
	if err != nil {
		t.Fatalf("FindDoingTasks: %v", err)
	}
	for _, tk := range doing {
		if tk.TaskName == node {
			return tk.ID
		}
	}
	t.Fatalf("实例 %d 下无进行中的 %s 任务", instanceID, node)
	return 0
}

// TestWithdrawAuthzPersistsUpdateUser issues/114（spec 08 用例 24）：撤回鉴权与 update_user 回写须**落库**。
// 直查 wf_process_instance / wf_process_task 断持久值——含"已完成(20) 的任务行不被改写"，
// 以及"缺 operator 不得回落 user1"（回落缺陷的持久证据就是库里 update_user=user1）。
func TestWithdrawAuthzPersistsUpdateUser(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	f := facade.New(eng, repo, nil)
	instID := facadeStartSimple(t, f)
	taskID := taskIDOfNode(t, repo, instID, "task1")
	const applySQL = "SELECT task_state, update_user FROM wf_process_task WHERE process_instance_id = ? AND task_name = 'apply'"
	const taskSQL = "SELECT task_state, update_user FROM wf_process_task WHERE id = ?"
	const instSQL = "SELECT state, update_user FROM wf_process_instance WHERE id = ?"

	// 负向①：operator 缺失 / 空串 → 99999999 + msg，库里原状不动
	for _, args := range []map[string]interface{}{
		{"id": instID},
		{"id": instID, "operator": "   "},
	} {
		r := f.Flow("processInstance/withdraw", args)
		if code, _ := r["code"].(int); code != 99999999 {
			t.Fatalf("withdraw %v 应失败 99999999, got %v", args, r)
		}
		if msg, _ := r["msg"].(string); !strings.Contains(msg, "operator 必填") {
			t.Fatalf("withdraw %v msg = %q, want 含「operator 必填」", args, msg)
		}
	}
	// 负向②：无关第三人 → 无权限撤回该流程实例
	r := f.Flow("processInstance/withdraw", map[string]interface{}{"id": instID, "operator": "stranger"})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("第三人撤回应失败, got %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "无权限撤回该流程实例") {
		t.Fatalf("第三人撤回 msg = %q, want 含「无权限撤回该流程实例」", msg)
	}
	if s, u := queryRowState(t, db, instSQL, instID); s != 10 || u == "user1" {
		t.Fatalf("报错后实例库内 = state %d update_user %q, want 仍 DOING(10) 且不被记成 user1", s, u)
	}
	if s, u := queryRowState(t, db, taskSQL, taskID); s != 10 || u == "user1" {
		t.Fatalf("报错后 task1 库内 = state %d update_user %q, want 仍 DOING(10)", s, u)
	}

	// 正向：进行中任务的参与者（leader，非发起人）撤整单
	if r := f.Flow("processInstance/withdraw", map[string]interface{}{"id": instID, "operator": "leader"}); r["code"].(int) != 0 {
		t.Fatalf("参与者撤回应成功: %v", r)
	}
	if s, u := queryRowState(t, db, instSQL, instID); s != int(model.InstanceStateWithdraw) || u != "leader" {
		t.Fatalf("实例落库 = state %d update_user %q, want 30 + leader", s, u)
	}
	if s, u := queryRowState(t, db, taskSQL, taskID); s != int(model.TaskStateWithdraw) || u != "leader" {
		t.Fatalf("task1 落库 = state %d update_user %q, want 30（不是 99）+ leader", s, u)
	}
	// 已完成(20) 的 apply 行：状态与 update_user 都不得被撤回改写
	if s, u := queryRowState(t, db, applySQL, instID); s != int(model.TaskStateDone) || u != "zhangsan" {
		t.Fatalf("已完成任务被改写 = state %d update_user %q, want 20 + zhangsan", s, u)
	}
}

// TestTransferPersistsActorSwapAndRecord issues/115（spec 08 用例 25）：转办须**落库**——
// 只摘 fromActor 那一行 actor（加签来的 coworker 不动）、toActor 追加、同一 taskId 不新建任务、
// submitType=7 留痕与 tf_transferTo/tf_transferReason 进 task.variable、update_user 记操作人；
// 契约 06 §transfer 留痕⚠️：actor_id/operator 列**严禁覆写**（进行中任务该列恒无值是既有不变量）。
func TestTransferPersistsActorSwapAndRecord(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	f := facade.New(eng, repo, nil)
	instID := facadeStartSimple(t, f)
	taskID := taskIDOfNode(t, repo, instID, "task1")

	// 加签（只追加）：让任务有两个参与人，才能证明转办"只摘 fromActor 那一行"
	if r := f.Flow("processTask/surrogate", map[string]interface{}{
		"processTaskId": taskID, "actorIds": []string{"coworker"},
	}); r["code"].(int) != 0 {
		t.Fatalf("加签: %v", r)
	}
	// 负向：目标人已是参与者 → 明确报错且库里一行不动
	if r := f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": taskID, "fromActor": "leader", "toActor": "coworker", "operator": "leader",
	}); r["code"].(int) != 99999999 || !strings.Contains(fmt.Sprint(r["msg"]), "目标人已是该任务参与人") {
		t.Fatalf("目标人已是参与者应明确报错, got %v", r)
	}
	if n := countActorsOf(t, db, taskID); n != 2 {
		t.Fatalf("报错后参与人行数 = %d, want 2（零副作用）", n)
	}

	// 正向：leader 把自己那一行摘掉，newcomer 接手
	if r := f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer",
		"reason": "出差三天", "operator": "leader",
	}); r["code"].(int) != 0 {
		t.Fatalf("转办: %v", r)
	}
	// 参与人读回（直查关系表）：coworker 保留、leader 摘走、newcomer 追加
	if has := actorIDsOf(t, db, taskID); !containsStr(has, "newcomer") || !containsStr(has, "coworker") || containsStr(has, "leader") {
		t.Fatalf("转办后库内参与人 = %v, want {coworker, newcomer}", has)
	}
	// 任务不新建：实例下仍是 apply + task1 两行，且 task1 仍进行中(10)
	var taskCnt int
	if err := db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id = ?"), instID).Scan(&taskCnt); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCnt != 2 {
		t.Fatalf("转办后任务行数 = %d, want 2（沿用同一 taskId，不新建）", taskCnt)
	}
	// 留痕落库：task_state 仍 10、update_user=操作人、operator 列恒无值（契约 ⚠️ 严禁覆写）、
	// variable 里 submitType=7 + tf_transferTo
	var state int
	var operator, updateUser sql.NullString
	var variable []byte
	if err := db.QueryRowContext(ctx, ph(
		"SELECT task_state, operator, update_user, variable FROM wf_process_task WHERE id = ?"), taskID).
		Scan(&state, &operator, &updateUser, &variable); err != nil {
		t.Fatalf("query task: %v", err)
	}
	if state != int(model.TaskStateDoing) {
		t.Fatalf("转办后任务态 = %d, want 仍 DOING(10)", state)
	}
	if operator.String != "" {
		t.Fatalf("转办后落库 operator 列 = %q, want 恒无值（写被摘走的人会致撤回单冒进其「我已办」）", operator.String)
	}
	if updateUser.String != "leader" {
		t.Fatalf("转办留痕 update_user = %q, want 操作人 leader", updateUser.String)
	}
	var vars map[string]interface{}
	if err := json.Unmarshal(variable, &vars); err != nil {
		t.Fatalf("variable JSON 解析失败: %v (%s)", err, variable)
	}
	if num, _ := vars["submitType"].(float64); int(num) != int(model.SubmitTypeTransfer) {
		t.Fatalf("落库留痕 submitType = %v, want 7（TRANSFER）", vars["submitType"])
	}
	if vars["tf_transferTo"] != "newcomer" {
		t.Fatalf("落库 tf_transferTo = %v, want newcomer", vars["tf_transferTo"])
	}
	if vars["tf_transferReason"] != "出差三天" {
		t.Fatalf("落库 tf_transferReason = %v, want 出差三天", vars["tf_transferReason"])
	}
	// 待办随参与人挪窝（SQL JOIN 判据）：leader 查不到、newcomer 查得到同一 taskId
	if ids := todoIDsViaFacade(t, f, "leader"); containsI64(ids, taskID) {
		t.Fatalf("转办后原办理人待办仍含该任务: %v", ids)
	}
	if ids := todoIDsViaFacade(t, f, "newcomer"); !containsI64(ids, taskID) {
		t.Fatalf("转办后接手人待办应含同一 taskId %d: %v", taskID, ids)
	}
}

// TestTransferWithdrawDoesNotPolluteDoneList 契约 06 §transfer 留痕⚠️（Node 实测复现的缺陷形态，
// SQL 落库版）：转办严禁覆写 operator 列——一旦写入被摘走的人，该单撤回后离开 DOING 但列值仍在，
// pageDoneTasks（state <> 10 AND operator = ?）会让他从「我已办」里看到从没办过的单。
// 断言全落**直查库列 + 门面读回值**。
func TestTransferWithdrawDoesNotPolluteDoneList(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	f := facade.New(eng, repo, nil)
	instID := facadeStartSimple(t, f)
	taskID := taskIDOfNode(t, repo, instID, "task1")

	if r := f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer",
		"reason": "出差三天", "operator": "leader",
	}); r["code"].(int) != 0 {
		t.Fatalf("转办: %v", r)
	}
	const opColSQL = "SELECT operator FROM wf_process_task WHERE id = ?"
	assertOpColEmpty(t, db, opColSQL, taskID, "转办后")

	// 发起人撤回：task1 离开 DOING（→30），operator 列仍无值
	if r := f.Flow("processInstance/withdraw", map[string]interface{}{"id": instID, "operator": "zhangsan"}); r["code"].(int) != 0 {
		t.Fatalf("撤回: %v", r)
	}
	var state int
	if err := db.QueryRowContext(ctx, ph("SELECT task_state FROM wf_process_task WHERE id = ?"), taskID).Scan(&state); err != nil {
		t.Fatalf("query state: %v", err)
	}
	if state != int(model.TaskStateWithdraw) {
		t.Fatalf("撤回后任务态 = %d, want 30（撤回未生效则本用例失去意义）", state)
	}
	assertOpColEmpty(t, db, opColSQL, taskID, "撤回后")

	// 被摘走的 leader 与未办的 newcomer：「我已办」都不含该单（冒单即缺陷实证）
	for _, who := range []string{"leader", "newcomer"} {
		if got := doneIDsViaFacade(t, f, who); containsI64(got, taskID) {
			t.Fatalf("转办→撤回后 %s 的已办列表冒入该单（他从没办过）: %v", who, got)
		}
	}
}

// assertOpColEmpty 直查 task.operator 列断言无值（NULL 或空串皆算无值）
func assertOpColEmpty(t *testing.T, db *sql.DB, selSQL string, taskID int64, label string) {
	t.Helper()
	var op sql.NullString
	if err := db.QueryRowContext(context.Background(), ph(selSQL), taskID).Scan(&op); err != nil {
		t.Fatalf("query operator: %v", err)
	}
	if op.Valid && op.String != "" {
		t.Fatalf("%s落库 operator 列 = %q, want 恒无值", label, op.String)
	}
}

// doneIDsViaFacade 走门面已办分页取任务 id（pageDoneTasks 判据 state <> 10 AND operator = ?）
func doneIDsViaFacade(t *testing.T, f *facade.Facade, operator string) []int64 {
	t.Helper()
	r := f.Flow("processTask/doneList", map[string]interface{}{"operator": operator, "pageSize": 100})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("doneList(%s): %v", operator, r)
	}
	rows, _ := r["data"].(map[string]interface{})["rows"].([]interface{})
	ids := make([]int64, 0, len(rows))
	for _, m := range rows {
		n, _ := strconv.ParseInt(fmt.Sprint(m.(map[string]interface{})["id"]), 10, 64)
		ids = append(ids, n)
	}
	return ids
}

// transferTimeLayout tf_transferHistory.time 的落地格式（与内存路断言同串：字符串而非时刻对象，
// 跨 JSON 往返形状稳定，七栈对齐按同一格式写入）
const transferTimeLayout = "2006-01-02 15:04:05"

// taskVarsOf 直查任务 variable 列并解 JSON（落库判据：不经仓储对象，看库里真实形状）
func taskVarsOf(t *testing.T, db *sql.DB, taskID int64) map[string]interface{} {
	t.Helper()
	var raw []byte
	if err := db.QueryRowContext(context.Background(), ph(
		"SELECT variable FROM wf_process_task WHERE id = ?"), taskID).Scan(&raw); err != nil {
		t.Fatalf("query variable: %v", err)
	}
	var vars map[string]interface{}
	if err := json.Unmarshal(raw, &vars); err != nil {
		t.Fatalf("variable JSON 解析失败: %v (%s)", err, raw)
	}
	return vars
}

// ledgerOf 归一 tf_transferHistory 的落库形状：variable 列 JSON 里必须是**数组套对象**
// （SQL 反序列化即 []interface{} of map[string]interface{}）；形状不合规直接 Fail。
func ledgerOf(t *testing.T, v interface{}) []map[string]interface{} {
	t.Helper()
	list, ok := v.([]interface{})
	if !ok {
		if v == nil {
			return nil
		}
		t.Fatalf("tf_transferHistory 落库形状异常: %T, want JSON 数组", v)
	}
	out := make([]map[string]interface{}, 0, len(list))
	for i, e := range list {
		m, ok := e.(map[string]interface{})
		if !ok {
			t.Fatalf("tf_transferHistory[%d] 落库形状异常: %T, want JSON 对象", i, e)
		}
		out = append(out, m)
	}
	return out
}

// numVal 取 JSON 回读的数字（SQL 路一律 float64，内存路 int——判据不区分来源）
func numVal(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	}
	return -1
}

// checkHop 逐字段核对落库账本里的一跳
func checkHop(t *testing.T, idx int, rec map[string]interface{}, from, to, reason, op string) {
	t.Helper()
	if got := numVal(rec["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("第 %d 跳 submitType = %v, want 7（TRANSFER）", idx, rec["submitType"])
	}
	for _, kv := range []struct{ key, want string }{
		{"fromActor", from}, {"toActor", to}, {"reason", reason}, {"operator", op},
	} {
		if s, _ := rec[kv.key].(string); s != kv.want {
			t.Fatalf("第 %d 跳 %s = %v, want %q", idx, kv.key, rec[kv.key], kv.want)
		}
	}
	s, _ := rec["time"].(string)
	if _, err := time.Parse(transferTimeLayout, s); err != nil {
		t.Fatalf("第 %d 跳 time = %v, want「%s」格式: %v", idx, rec["time"], transferTimeLayout, err)
	}
}

// TestTransferHistoryLedgerPersists spec 06 §processTask/transfer 留痕②③（契约 fc0883a 改约三件）
// 的落库支路：A→B、B→C 两跳后 C 办结（submitType=1），直查 wf_process_task.variable JSON——
// tf_transferHistory 仍是两条且逐字段正确（追加式账本跨跳、跨办结存活），末跳槽位被 1 覆盖属预期。
func TestTransferHistoryLedgerPersists(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)

	content := loadFlow(t, "01-simple.json")
	insertDefine(t, db, "go-simple", content)

	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	f := facade.New(eng, repo, nil)
	instID := facadeStartSimple(t, f)
	taskID := taskIDOfNode(t, repo, instID, "task1")

	transfer := func(from, to, reason, operator string) {
		t.Helper()
		if r := f.Flow("processTask/transfer", map[string]interface{}{
			"processTaskId": taskID, "fromActor": from, "toActor": to,
			"reason": reason, "operator": operator,
		}); r["code"].(int) != 0 {
			t.Fatalf("转办 %s→%s: %v", from, to, r)
		}
	}
	transfer("leader", "newcomer", "出差三天", "leader")
	transfer("newcomer", "third", "不熟悉该业务", "newcomer")

	// ── 两跳落库：账本两条（第二跳的 append 没抹掉第一跳），当前槽位仍 submitType=7 ──
	hopped := taskVarsOf(t, db, taskID)
	recs := ledgerOf(t, hopped["tf_transferHistory"])
	if len(recs) != 2 {
		t.Fatalf("两跳后落库 tf_transferHistory = %d 条, want 2: %v", len(recs), hopped["tf_transferHistory"])
	}
	checkHop(t, 1, recs[0], "leader", "newcomer", "出差三天", "leader")
	checkHop(t, 2, recs[1], "newcomer", "third", "不熟悉该业务", "newcomer")
	if got := numVal(hopped["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("办结前落库槽位 submitType = %v, want 7", hopped["submitType"])
	}
	if s, _ := hopped["tf_approvalComment"].(string); s != "newcomer 转办给 third（不熟悉该业务）" {
		t.Fatalf("落库末跳文案 tf_approvalComment = %v, want「newcomer 转办给 third（不熟悉该业务）」", hopped["tf_approvalComment"])
	}

	// ── C 办结（自填审批意见）：JSON 往返后第二跳仍读得到旧账，落库槽位被 1 覆盖、账本仍在 ──
	if r := f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": taskID, "operator": "third", "submitType": 1,
		"tf_approvalComment": "已核对，同意",
	}); r["code"].(int) != 0 {
		t.Fatalf("C 办结: %v", r)
	}
	done := taskVarsOf(t, db, taskID)
	if got := numVal(done["submitType"]); got != int64(model.SubmitTypeAgree) {
		t.Fatalf("C 办结后落库 submitType = %v, want 1（末跳槽位被办理参数覆盖属预期）", done["submitType"])
	}
	kept := ledgerOf(t, done["tf_transferHistory"])
	if len(kept) != 2 {
		t.Fatalf("办结后落库 tf_transferHistory = %d 条, want 2（转办事实整体消失即审计断链）: %v", len(kept), done["tf_transferHistory"])
	}
	checkHop(t, 1, kept[0], "leader", "newcomer", "出差三天", "leader")
	checkHop(t, 2, kept[1], "newcomer", "third", "不熟悉该业务", "newcomer")
	if s, _ := done["tf_approvalComment"].(string); s != "已核对，同意" {
		t.Fatalf("落库 tf_approvalComment = %v, want「已核对，同意」（本次提交参数优先级最高）", done["tf_approvalComment"])
	}
	// 仓储读回同一判据（引擎下一跳 append 看到的就是这个形状）
	tk, err := repo.FindTaskByID(ctx, taskID)
	if err != nil || tk == nil {
		t.Fatalf("FindTaskByID: %v", err)
	}
	if n := len(ledgerOf(t, tk.Variables["tf_transferHistory"])); n != 2 {
		t.Fatalf("读回 tf_transferHistory = %d 条, want 2", n)
	}
	// 任务不新建：全程 apply + task1 两行
	var taskCnt int
	if err := db.QueryRowContext(ctx, ph(
		"SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id = ?"), instID).Scan(&taskCnt); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCnt != 2 {
		t.Fatalf("两跳+办结后任务行数 = %d, want 2（沿用同一 taskId）", taskCnt)
	}
}

// countActorsOf 直查任务参与人行数
func countActorsOf(t *testing.T, db *sql.DB, taskID int64) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), ph(
		"SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id = ?"), taskID).Scan(&n); err != nil {
		t.Fatalf("count actors: %v", err)
	}
	return n
}

// actorIDsOf 直查任务参与人清单
func actorIDsOf(t *testing.T, db *sql.DB, taskID int64) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), ph(
		"SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?"), taskID)
	if err != nil {
		t.Fatalf("query actors: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan actor: %v", err)
		}
		out = append(out, a)
	}
	return out
}

// todoIDsViaFacade 走门面待办分页取任务 id（SQL JOIN 判据的用户视角）
func todoIDsViaFacade(t *testing.T, f *facade.Facade, operator string) []int64 {
	t.Helper()
	r := f.Flow("processTask/todoList", map[string]interface{}{"operator": operator, "pageSize": 100})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("todoList(%s): %v", operator, r)
	}
	rows, _ := r["data"].(map[string]interface{})["rows"].([]interface{})
	ids := make([]int64, 0, len(rows))
	for _, m := range rows {
		n, _ := strconv.ParseInt(fmt.Sprint(m.(map[string]interface{})["id"]), 10, 64)
		ids = append(ids, n)
	}
	return ids
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func containsI64(list []int64, v int64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
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
