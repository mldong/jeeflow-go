// audit8 R9 145-3（spec/12 §12.4 三档行为判据）· Go 栈真库档。
//
// 与 jdbc_test.go 同一连接口径（默认 160 MySQL jeeflow 库，JEFFLOW_DB_DSN 可覆盖）：
//
//	go test ./repository/jdbc/ -run 'TestTxTier145' -v
//
// 覆盖六格：
//  1. 空转档——事务内写：同事务连接读得到 / 独立连接提交前读不到 / 回滚后不存在
//  2. 原子档——引擎入口（ExecuteProcessTask；真实发起作前置）：任务完成写（写#1）之后
//     抄送漏斗（写#2）注入失败 ⇒ 任务行回到 DOING＋实例仍 DOING＋cc 零残留（动作内写整体回滚）
//  3. 动作档——门面复合动作（startAndExecute：发起 → 发起腿抄送 → 自动办理）：
//     发起腿抄送漏斗注入失败 ⇒ 整次 dispatch 回滚（发起拍的实例/任务行一并消失）；
//     ⚠ 裸拼对照（不经门面、两次引擎入口各一拍）：第一拍已提交、第二拍失败 ⇒ 半完成留存
//     = "门面不包"的修前形态（红样形态本身，不是缺陷）
//  4. 归属档——嵌套并入（外层回滚连坐引擎入口写）＋并发切开（A 回滚 B 留存，互不串）
//  5. ext 腿——主仓+扩展仓同 ctx 同事务：回滚双消失 / 提交双落库
//  6. persist 腿（C99 ⑥）——SYNC 流 + 真 PersistPostInterceptor：writer 注 TxFromCtx ⇒ 业务行随
//     动作回滚（并入）；不注 ⇒ 行逃逸留存（§12.1 条 4 允许降级运行态＝集成方必须显式接线的理由）
//
// 注入点＝tier145FlakyRepo 两处（都位于"真中途"且错误真传播）：
//   - 抄送漏斗 CreateCcInstanceIfAbsent（②③）：任务完成写之后、persist 的 fire 之前；
//   - 实例终结写 UpdateInstance（⑥）：办理拍的 persist 已写业务行之后（end 节点 Finish 写）。
//
// 不注入引擎 SaveTask 的原因：saveNewTask 的调用点全仓吞错，动作不会报失败，做不出判据
// （吞错属另一族缺陷，不在本切片手术面）。
//
// 阳性对照（当轮逐处摘除并已字节级还原；红样原文）：
//
//	摘 WithTx 并入支 ⇒ ③④ 红：「动作档…实例行必须随动作整体回滚（实得 1）」
//	  「归属档：外层回滚必须连坐嵌套动作（实例行实得 1）」；
//	摘引擎入口包裹（InTx 直通）⇒ ②③⑥ 红：「原子档：动作失败后任务行必须回到 DOING(10)，实得 20」
//	  「动作档…实例行必须随动作整体回滚（实得 1）」「persist 腿：并入事务的业务行必须随动作回滚（实得 1）」；
//	摘门面 inTx ⇒ ③ 红：「动作档…实例行必须随动作整体回滚（实得 1）」；
//	摘 writer 的 TxFromCtx 命中 ⇒ ①④⑥ 红：「空转档：独立连接提交前必须读不到（实得 1）」
//	  「归属档：A 回滚后其行必须不在（实得 1）」「persist 腿：…必须随动作回滚（实得 1）」。
package jdbc_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/persist"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

const tier145TagPrefix = "gotier-"

// tier145EnsureTable 夹具表自建（幂等）——biz_tx_probe 不在引擎 schema 内，跑测前保证存在，
// 不依赖某台机器手工建过。列口径＝测试实际消费面：process_instance_id 是拦截器 Exists/Update 的业务键，
// tag 承接 f_tag，id 走 PrimaryKeyGenerator（无自增）。
func tier145EnsureTable(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), ph(
		"CREATE TABLE IF NOT EXISTS biz_tx_probe (id bigint NOT NULL, tag varchar(64), process_instance_id bigint, PRIMARY KEY(id))")); err != nil {
		t.Fatalf("tier 夹具表自建: %v", err)
	}
}

// tier145Cleanup 按 defineID（复用 jdbc_test 的 900001）+ 专用 tag/operator 清场（进也清、出也清）。
func tier145Cleanup(t *testing.T, db *sql.DB) {
	t.Helper()
	tier145EnsureTable(t, db)
	ctx := context.Background()
	cleanup(t, db)
	for _, q := range []string{
		"DELETE FROM biz_tx_probe WHERE tag LIKE 'gotier-%'",
		"DELETE FROM wf_process_surrogate WHERE operator = 'tier145'",
	} {
		if _, err := db.ExecContext(ctx, ph(q)); err != nil {
			t.Fatalf("tier 清场 %q: %v", q, err)
		}
	}
}

// tier145Count 独立连接（池）反查计数。
func tier145Count(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), ph(q), args...).Scan(&n); err != nil {
		t.Fatalf("池反查失败 %q: %v", q, err)
	}
	return n
}

// tier145Scalar 独立连接单值反查（状态列等）。
func tier145Scalar(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), ph(q), args...).Scan(&n); err != nil {
		t.Fatalf("池单值反查失败 %q: %v", q, err)
	}
	return n
}

// tier145CountTx 同事务连接反查（走 ctx 绑定的事务；未绑定直接判红——连接绑定是本档的承重面）。
func tier145CountTx(t *testing.T, ctx context.Context, q string, args ...interface{}) int {
	t.Helper()
	tx, ok := jdbc.TxFromContext(ctx)
	if !ok {
		t.Fatalf("ctx 未绑定事务（WithTx 连接绑定失效）: %q", q)
	}
	var n int
	if err := tx.QueryRowContext(ctx, ph(q), args...).Scan(&n); err != nil {
		t.Fatalf("同事务连接反查失败 %q: %v", q, err)
	}
	return n
}

// tier145AssertNoActionResidue 动作失败后：实例/任务/参与者/cc 四表必须全 0（独立连接反查）。
func tier145AssertNoActionResidue(t *testing.T, db *sql.DB, why string) {
	t.Helper()
	checks := []struct {
		name string
		q    string
	}{
		{"实例", "SELECT COUNT(*) FROM wf_process_instance WHERE process_define_id = ?"},
		{"任务", "SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"},
		{"参与者", "SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?))"},
		{"cc行", "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id IN (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"},
	}
	for _, c := range checks {
		if n := tier145Count(t, db, c.q, defineID); n != 0 {
			t.Fatalf("%s：%s 行必须随动作整体回滚（实得 %d）", why, c.name, n)
		}
	}
}

// tier145FlakyRepo 注入失败（策略：第 N 次抄送漏斗写报错）——判据是"失败了就必须整体不存在"。
// 注入点选 cc 漏斗（`CreateCcInstanceIfAbsent`）的原因：① 它位于"任务完成写（UpdateTask）之后"，
// 是真正的中途（≥1 条写已发生）；② 这条腿的错误在引擎（prepareExecuteTask 的 onTaskUpdated 钩子）
// 与门面（发起腿 HandleCcActors）两处都真传播——而引擎的 SaveTask 调用点全仓吞错（saveNewTask
// 返回值各调用点丢弃），注入它动作不会报失败，做不出判据。吞错属另一族缺陷，不在本切片手术面。
// 嵌入真仓储：其余方法透传；WithTx 随嵌入提升（引擎 InTx 的类型断言仍命中）。
type tier145FlakyRepo struct {
	*jdbc.Repository
	failCcN    int
	ccCalls    int
	failFinish bool // 实例终结写（state 离开 DOING 的 UpdateInstance）注入失败
	finishHits int
}

func (r *tier145FlakyRepo) CreateCcInstanceIfAbsent(ctx context.Context, instanceID int64, creator string, actorIDs ...string) ([]string, error) {
	r.ccCalls++
	if r.failCcN > 0 && r.ccCalls == r.failCcN {
		return nil, fmt.Errorf("injected: createCc failed mid-action")
	}
	return r.Repository.CreateCcInstanceIfAbsent(ctx, instanceID, creator, actorIDs...)
}

// UpdateInstance 实例终结写注入（按 state 语义判、不数调用序）：end 节点的 Finish/Reject 写失败。
// 该写点位于"办理拍的 persist 拦截器已写业务行之后"，是 ⑥ 格需要的中途断点。
func (r *tier145FlakyRepo) UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error {
	if r.failFinish && inst.State != model.InstanceStateDoing {
		r.finishHits++
		if r.finishHits == 1 {
			return fmt.Errorf("injected: finish write failed mid-action")
		}
	}
	return r.Repository.UpdateInstance(ctx, inst)
}

// tier145NewWriter 事务感知 writer（集成侧的标准接线：TxFromCtx = jdbc.TxFromContext）。
// pkGen 生成非自增主键（biz_tx_probe.id 无 auto_increment）。
func tier145NewWriter(db *sql.DB, txAware bool, pkGen func() int64) *persist.JdbcDynamicTableWriter {
	w := persist.NewJdbcDynamicTableWriter(db)
	if txAware {
		w.TxFromCtx = jdbc.TxFromContext
	}
	w.PrimaryKeyGenerator = func(string) interface{} { return pkGen() }
	return w
}

// tier145SyncFlow SYNC 入库流：start → apply → end，业务表 biz_tx_probe（f_tag → tag 列）。
const tier145SyncFlow = `{
  "name": "tier-sync",
  "displayName": "R9 事务档 SYNC 流",
  "type": "approval",
  "persistMode": "SYNC",
  "relTableName": "biz_tx_probe",
  "nodes": [
    {"id": "start", "type": "snaker:start", "x": 100, "y": 200,
     "properties": {"width": 50, "height": 50}, "text": {"value": "开始"}},
    {"id": "apply", "type": "snaker:task", "x": 200, "y": 200,
     "properties": {"width": 100, "height": 50, "assignee": "applicant", "taskType": 0, "performType": 0},
     "text": {"value": "发起申请"}},
    {"id": "end", "type": "snaker:end", "x": 500, "y": 200,
     "properties": {"width": 50, "height": 50}, "text": {"value": "结束"}}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "end", "properties": {}}
  ]
}`

// ─── ① 空转档 ──────────────────────────────────────────────────────────────────

func TestTxTier145IdleConnBinding(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	ctx := context.Background()
	repo := jdbc.New(db)
	pk := int64(920145101)
	writer := tier145NewWriter(db, true, func() int64 { pk++; return pk })

	tag := tier145TagPrefix + "idle"
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if _, err := writer.Insert(ctx, "biz_tx_probe", map[string]interface{}{"id": int64(920145100), "tag": tag}); err != nil {
			return err
		}
		// ㈠ 提交之前：同事务连接读得到（连接绑定生效——每条语句各取新连接的形状在此必红）
		if n := tier145CountTx(t, ctx, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tag); n != 1 {
			t.Fatalf("空转档：同事务连接提交前必须读得到（实得 %d）", n)
		}
		// ㈡ 提交之前：独立连接读不到（未提交）
		if n := tier145Count(t, db, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tag); n != 0 {
			t.Fatalf("空转档：独立连接提交前必须读不到（实得 %d）", n)
		}
		return errors.New("rollback on purpose")
	})
	if err == nil {
		t.Fatalf("回调报错必须外抛")
	}
	// ㈢ 回滚后：独立连接也读不到
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tag); n != 0 {
		t.Fatalf("空转档：回滚后行必须不存在（实得 %d）", n)
	}
}

// ─── ② 原子档（引擎入口） ──────────────────────────────────────────────────────

func TestTxTier145AtomicEngineEntry(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	insertDefine(t, db, "go-simple", loadFlow(t, "01-simple.json"))
	ctx := context.Background()

	// 前置：干净仓走一次真实发起（实例+任务已提交，成为动作前状态）
	repo0 := jdbc.New(db)
	eng0 := newEngine(t, repo0)
	inst, err := eng0.StartProcessInstanceByID(ctx, defineID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("前置发起应成功: %v", err)
	}
	doing, err := repo0.FindDoingTasks(ctx, inst.ID, nil)
	if err != nil || len(doing) == 0 {
		t.Fatalf("前置应有 apply 待办: %v", err)
	}

	// 被注入：任务行完成写（写#1）之后、抄送漏斗（写#2）失败 ⇒ 整次动作回滚。
	repo := &tier145FlakyRepo{Repository: jdbc.New(db), failCcN: 1}
	eng := newEngine(t, repo)
	if _, err := eng.ExecuteProcessTask(ctx, doing[0].ID, "zhangsan",
		map[string]interface{}{"submitType": 0, "tf_ccActors": []string{"lisi"}}); err == nil {
		t.Fatalf("中途注入失败后必须报错")
	}
	// 写#1（任务 → FINISHED）必须随动作回滚：任务行回到 DOING；零 cc 行残留
	if got := tier145Scalar(t, db, "SELECT task_state FROM wf_process_task WHERE id = ?", doing[0].ID); got != int(model.TaskStateDoing) {
		t.Fatalf("原子档：动作失败后任务行必须回到 DOING(%d)，实得 %d —— 动作内写未整体回滚（引擎入口未包裹）",
			model.TaskStateDoing, got)
	}
	if got := tier145Scalar(t, db, "SELECT state FROM wf_process_instance WHERE id = ?", inst.ID); got != int(model.InstanceStateDoing) {
		t.Fatalf("原子档：动作失败后实例行必须仍是 DOING(%d)，实得 %d", model.InstanceStateDoing, got)
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", inst.ID); n != 0 {
		t.Fatalf("原子档：cc 行必须随动作回滚（实得 %d）", n)
	}
}

// ─── ③ 动作档（门面复合）＋裸拼对照 ────────────────────────────────────────────

func TestTxTier145ActionFacadeCompositeAndBareContrast(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	insertDefine(t, db, "go-simple", loadFlow(t, "01-simple.json"))
	ctx := context.Background()

	// ③-a 门面复合：startAndExecute（发起 → 发起腿抄送 → 自动办理），发起腿抄送漏斗注入失败 ⇒
	// 整次 dispatch 回滚（发起拍的实例/任务行一并消失）。
	repoA := &tier145FlakyRepo{Repository: jdbc.New(db), failCcN: 1}
	engA := newEngine(t, repoA)
	fA := facade.New(engA, repoA, nil)
	r := fA.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "f_ccActors": []string{"lisi"},
	})
	if r["code"].(int) == 0 {
		t.Fatalf("中途失败的动作不得报成功: %v", r)
	}
	tier145AssertNoActionResidue(t, db, "动作档（门面整次 dispatch 包裹）")

	// ③-b 裸拼对照：不经门面（两次引擎入口＝两拍）。第一拍提交、第二拍（办理腿抄送漏斗）失败 ⇒
	// 半完成留存（实例行=1 存活、任务行停在 DOING）——"门面不包"的修前形态，证明 ③-a 那格不是恒真。
	repoB := &tier145FlakyRepo{Repository: jdbc.New(db), failCcN: 1}
	engB := newEngine(t, repoB)
	inst, err := engB.StartProcessInstanceByID(ctx, defineID, "zhangsan",
		map[string]interface{}{"BUSINESS_NO": "BIZ-TIER145-BARE"})
	if err != nil {
		t.Fatalf("裸拼第一拍应成功: %v", err)
	}
	doing, err := repoB.FindDoingTasks(ctx, inst.ID, nil)
	if err != nil || len(doing) == 0 {
		t.Fatalf("裸拼第一拍后应有 apply 待办: %v", err)
	}
	if _, err := engB.ExecuteProcessTask(ctx, doing[0].ID, "zhangsan",
		map[string]interface{}{"submitType": 0, "tf_ccActors": []string{"lisi"}}); err == nil {
		t.Fatalf("裸拼第二拍必须失败（注入点）")
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_instance WHERE process_define_id = ?", defineID); n != 1 {
		t.Fatalf("裸拼对照：第一拍已提交 ⇒ 实例行必须留存 1（两拍各一事务的边界证据），实得 %d", n)
	}
	if got := tier145Scalar(t, db, "SELECT task_state FROM wf_process_task WHERE id = ?", doing[0].ID); got != int(model.TaskStateDoing) {
		t.Fatalf("裸拼对照：第二拍自身回滚 ⇒ 任务行停在 DOING(%d)，实得 %d", model.TaskStateDoing, got)
	}
}

// ─── ④ 归属档（嵌套并入＋并发切开） ────────────────────────────────────────────

func TestTxTier145OwnershipNestedAndConcurrent(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	insertDefine(t, db, "go-simple", loadFlow(t, "01-simple.json"))
	ctx := context.Background()
	repo := jdbc.New(db)
	eng := newEngine(t, repo)

	// ④-a 嵌套并入：外层事务内调引擎入口（入口自己的包裹必须并入最外层——不双开、不自行提交），
	// 外层回滚 ⇒ 引擎写的实例行连坐消失。未并入的形状（入口另开连接自行 COMMIT）此处必红。
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if _, err := eng.StartProcessInstanceByID(ctx, defineID, "zhangsan",
			map[string]interface{}{"BUSINESS_NO": "BIZ-TIER145-NEST"}); err != nil {
			return err
		}
		if n := tier145CountTx(t, ctx, "SELECT COUNT(*) FROM wf_process_instance WHERE process_define_id = ?", defineID); n != 1 {
			t.Fatalf("归属档：嵌套事务内（同连接）应读到引擎写的实例行（实得 %d）", n)
		}
		return errors.New("outer rollback")
	})
	if err == nil {
		t.Fatalf("外层报错必须外抛")
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_instance WHERE process_define_id = ?", defineID); n != 0 {
		t.Fatalf("归属档：外层回滚必须连坐嵌套动作（实例行实得 %d）", n)
	}

	// ④-b 并发切开：A 开事务写行后让出，B 在窗口内自开事务写并成功提交 ⇒ A 回滚行不在、B 行在。
	pk := int64(920145201)
	writer := tier145NewWriter(db, true, func() int64 { pk++; return pk })
	tagA, tagB := tier145TagPrefix+"conc-a", tier145TagPrefix+"conc-b"
	aInTx := make(chan struct{})
	bCommitted := make(chan struct{})
	aDone := make(chan error, 1)
	go func() {
		aDone <- repo.WithTx(ctx, func(ctx context.Context) error {
			if _, err := writer.Insert(ctx, "biz_tx_probe", map[string]interface{}{"id": int64(920145200), "tag": tagA}); err != nil {
				return err
			}
			close(aInTx)
			<-bCommitted // 让出窗口：等 B 提交完再回滚
			return errors.New("A rollback")
		})
	}()
	<-aInTx
	if err := repo.WithTx(ctx, func(ctx context.Context) error {
		_, err := writer.Insert(ctx, "biz_tx_probe", map[string]interface{}{"id": int64(920145201), "tag": tagB})
		return err
	}); err != nil {
		t.Fatalf("B 应独立提交成功: %v", err)
	}
	close(bCommitted)
	if err := <-aDone; err == nil {
		t.Fatalf("A 必须回滚报错")
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tagA); n != 0 {
		t.Fatalf("归属档：A 回滚后其行必须不在（实得 %d）", n)
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tagB); n != 1 {
		t.Fatalf("归属档：B 已提交的行必须在（并发作用域不得互盖，实得 %d）", n)
	}
}

// ─── ⑤ ext 腿（主仓+扩展仓同 ctx 同事务） ─────────────────────────────────────

func TestTxTier145ExtSharesTx(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	ctx := context.Background()
	repo := jdbc.New(db)
	ext := jdbc.NewExt(db)
	now := time.Now()
	sur := func(name string) *model.ProcessSurrogate {
		return &model.ProcessSurrogate{
			Operator: "tier145", Surrogate: "lisi", ProcessName: name, Enabled: 1,
			CreateTime: now, UpdateTime: now, CreateUser: "t", UpdateUser: "t",
		}
	}

	// ⑤-a 回滚：主仓 cc 行 + 扩展仓委托行同事务双消失
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if err := repo.CreateCcInstance(ctx, 920145301, "zhangsan", "tier-cc-a"); err != nil {
			return err
		}
		if err := ext.SaveSurrogate(ctx, sur("tier-ext-rollback")); err != nil {
			return err
		}
		// 事务内两仓都可见（同一 txKey 载体）
		if n := tier145CountTx(t, ctx, "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", int64(920145301)); n != 1 {
			t.Fatalf("ext 腿：事务内主仓 cc 行应可见（实得 %d）", n)
		}
		if n := tier145CountTx(t, ctx, "SELECT COUNT(*) FROM wf_process_surrogate WHERE operator = 'tier145' AND process_name = ?", "tier-ext-rollback"); n != 1 {
			t.Fatalf("ext 腿：事务内扩展仓委托行应可见（实得 %d）", n)
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatalf("回调报错必须外抛")
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", int64(920145301)); n != 0 {
		t.Fatalf("ext 腿：回滚后主仓 cc 行必须不在（实得 %d）", n)
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_surrogate WHERE operator = 'tier145' AND process_name = ?", "tier-ext-rollback"); n != 0 {
		t.Fatalf("ext 腿：回滚后扩展仓委托行必须不在（实得 %d）——扩展仓不是事务豁免面", n)
	}

	// ⑤-b 提交：同形写入成功路径双落库
	if err := repo.WithTx(ctx, func(ctx context.Context) error {
		if err := repo.CreateCcInstance(ctx, 920145302, "zhangsan", "tier-cc-b"); err != nil {
			return err
		}
		return ext.SaveSurrogate(ctx, sur("tier-ext-commit"))
	}); err != nil {
		t.Fatalf("提交腿失败: %v", err)
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", int64(920145302)); n != 1 {
		t.Fatalf("ext 腿：提交后主仓 cc 行应在（实得 %d）", n)
	}
	if n := tier145Count(t, db, "SELECT COUNT(*) FROM wf_process_surrogate WHERE operator = 'tier145' AND process_name = ?", "tier-ext-commit"); n != 1 {
		t.Fatalf("ext 腿：提交后扩展仓委托行应在（实得 %d）", n)
	}
}

// ─── ⑥ persist 腿（C99 ⑥） ────────────────────────────────────────────────────

func TestTxTier145PersistWriterJoinsActionTx(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	tier145Cleanup(t, db)
	defer tier145Cleanup(t, db)

	insertDefine(t, db, "tier-sync", []byte(tier145SyncFlow))
	pk := int64(920145401)
	tagJoin, tagEscape := tier145TagPrefix+"persist-join", tier145TagPrefix+"persist-escape"

	run := func(tag string, txAware bool) int {
		t.Helper()
		// 实例终结写注入失败＝中途断点（业务行已由办理拍的 persist 拦截器写入之后；
		// persist 的 fire 只在办理腿 executeProcessTaskInner:165，任务创建腿不 fire 节点拦截器）
		repo := &tier145FlakyRepo{Repository: jdbc.New(db), failFinish: true}
		eng := newEngine(t, repo)
		writer := tier145NewWriter(db, txAware, func() int64 { pk++; return pk })
		eng.SetExtensions(&engine.Extensions{
			Interceptors: []engine.FlowInterceptor{persist.NewPersistPostInterceptor(writer, repo.FindDefineByID)},
		})
		f := facade.New(eng, repo, nil)
		r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
			"processDefineId": defineID, "operator": "zhangsan", "f_tag": tag,
		})
		if r["code"].(int) == 0 {
			t.Fatalf("动作应报失败（终结写注入）: %v", r)
		}
		return tier145Count(t, db, "SELECT COUNT(*) FROM biz_tx_probe WHERE tag = ?", tag)
	}

	// ⑥-a 注入 TxFromCtx（集成侧标准接线）⇒ 业务表写并入动作事务，随失败整体回滚
	if n := run(tagJoin, true); n != 0 {
		t.Fatalf("persist 腿：并入事务的业务行必须随动作回滚（实得 %d）——"+
			"C99 ⑥ 判据：writer 未并入同一条连接", n)
	}
	// ⑥-b 不注入（降级运行态）⇒ 行逃逸留存：锁定对照，证明 ⑥-a 那格测的不是空气
	if n := run(tagEscape, false); n != 1 {
		t.Fatalf("persist 腿：未注入 TxFromCtx 时行应逃逸留存 1（降级运行态红样），实得 %d", n)
	}
}
