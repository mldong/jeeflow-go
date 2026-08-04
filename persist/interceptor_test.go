package persist_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/persist"
)

// 用 01-simple 流程注入 relTableName（与 Java 集成测试同构）
func registerPersistFlow(repo *memory.Repository, withRelTable bool) *model.ProcessDefine {
	data, err := os.ReadFile("../../jeeflow-java/jeeflow-core/src/test/resources/flows/01-simple.json")
	if err != nil {
		panic(err.Error())
	}
	content := string(data)
	if withRelTable {
		content = replaceFirst(content, `"type": "approval"`, `"type": "approval", "relTableName": "biz_leave"`)
	}
	def := &model.ProcessDefine{ID: 1, Name: "simple", DisplayName: "01-simple.json", Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return def
}

func replaceFirst(s, old, new string) string {
	i := indexOf(s, old)
	if i < 0 {
		return s
	}
	return s[:i] + new + s[i+len(old):]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// 跑完流程：启动 → apply（applicant）→ task1（leader，同意/拒绝）
func runFlow(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, defineID int64, agree bool) *model.ProcessInstance {
	t.Helper()
	args := map[string]interface{}{
		"f_title": "年假申请",
		"f_amount": 800.0,
		"u_deptId": "D01",
	}
	inst, err := eng.StartProcessInstanceByID(context.Background(), defineID, "user1", args)
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "apply" {
		t.Fatalf("expected apply task, got %v", doing)
	}
	// apply（applicant 自动执行）
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeApply)}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("expected task1, got %v", doing)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	st := model.SubmitTypeAgree
	if !agree {
		st = model.SubmitTypeReject
	}
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader",
		map[string]interface{}{engine.KeySubmitType: int(st)}); err != nil {
		t.Fatalf("task1 failed: %v", err)
	}
	return inst
}

func countRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(1) FROM biz_leave").Scan(&n); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	return n
}

func newWriter(t *testing.T) (*sql.DB, *persist.JdbcDynamicTableWriter) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE biz_leave (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT,
		amount REAL,
		start_time TEXT,
		process_instance_id INTEGER,
		apply_user_id TEXT,
		apply_dept_id TEXT,
		create_time TEXT,
		create_user TEXT,
		update_time TEXT,
		update_user TEXT,
		is_deleted INTEGER
	)`)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	return db, persist.NewJdbcDynamicTableWriter(db)
}

// ① 流程结束同意 → 业务表落库（f_ 去前缀 + 系统字段 + 流程上下文）
func TestFlowFinishPersist(t *testing.T) {
	repo := memory.New()
	db, w := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, true)
	inst := runFlow(t, eng, repo, def.ID, true)

	var title, applyUser, applyDept, createUser string
	var pi int64
	var amount float64
	var deleted int
	err := db.QueryRow("SELECT title, amount, process_instance_id, apply_user_id, apply_dept_id, create_user, is_deleted FROM biz_leave").
		Scan(&title, &amount, &pi, &applyUser, &applyDept, &createUser, &deleted)
	if err != nil {
		t.Fatalf("query biz_leave failed: %v", err)
	}
	if title != "年假申请" || amount != 800.0 || pi != inst.ID || applyUser != "user1" ||
		applyDept != "D01" || createUser != "user1" || deleted != 0 {
		t.Fatalf("row mismatch: title=%s amount=%v pi=%d applyUser=%s applyDept=%s createUser=%s deleted=%d",
			title, amount, pi, applyUser, applyDept, createUser, deleted)
	}
	if countRows(t, db) != 1 {
		t.Fatal("expected exactly 1 row")
	}
}

// ② 不同意/退回 → 不入库
func TestRejectNoPersist(t *testing.T) {
	repo := memory.New()
	db, w := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, true)
	runFlow(t, eng, repo, def.ID, false)

	if n := countRows(t, db); n != 0 {
		t.Fatalf("reject should not persist, rows=%d", n)
	}
}

// ③ 未注入 writer → 静默跳过（不落库、不报错）
func TestNoWriterSkip(t *testing.T) {
	repo := memory.New()
	db, _ := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(nil, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, true)
	runFlow(t, eng, repo, def.ID, true)

	if n := countRows(t, db); n != 0 {
		t.Fatalf("no writer should skip, rows=%d", n)
	}
}

// ④ 未配置 relTableName → 缺省回落流程 name（simple）→ 表不存在 → 显性报错（panic，配置错误快速失败）
func TestNoTableNameSkip(t *testing.T) {
	repo := memory.New()
	db, w := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, false)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("table not found should panic (config error)")
		}
	}()
	runFlow(t, eng, repo, def.ID, true)
	if n := countRows(t, db); n != 0 {
		t.Fatalf("no table name should not persist, rows=%d", n)
	}
}

// ⑤ 幂等：同实例重复触发不重复插（拦截器手动再跑一次）
func TestIdempotentFlow(t *testing.T) {
	repo := memory.New()
	db, w := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, true)
	inst := runFlow(t, eng, repo, def.ID, true)

	// 模拟同链重复触发：结束节点再调一次拦截器（共享 inst.Variables）
	ic.PostHandle(&model.FlowNode{Type: model.TypeEnd}, inst)
	if n := countRows(t, db); n != 1 {
		t.Fatalf("idempotent failed, rows=%d", n)
	}
}

// ⑥ BIGINT 用户列（issues/19）：create_user 为 BIGINT 存 userId，operator 数字时插入不报类型错误
func TestBigintUserColumn(t *testing.T) {
	repo := memory.New()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE biz_settle (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT,
		process_instance_id INTEGER,
		apply_user_id INTEGER,
		create_user INTEGER,
		update_user INTEGER,
		is_deleted INTEGER
	)`)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	w := persist.NewJdbcDynamicTableWriter(db)
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	// 流程 content 注入 relTableName=biz_settle
	data, err := os.ReadFile("../../jeeflow-java/jeeflow-core/src/test/resources/flows/01-simple.json")
	if err != nil {
		t.Fatalf("read flow failed: %v", err)
	}
	content := replaceFirst(string(data), `"type": "approval"`, `"type": "approval", "relTableName": "biz_settle"`)
	def := &model.ProcessDefine{ID: 1, Name: "simple", Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)

	// operator 用数字 userId（BIGINT 列场景）
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "123",
		map[string]interface{}{"f_title": "结算单", "u_deptId": "D01"})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"123"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "123")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "123",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeApply)}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeAgree)}); err != nil {
		t.Fatalf("task1 failed: %v", err)
	}

	var createUser, applyUser int64
	err = db.QueryRow("SELECT create_user, apply_user_id FROM biz_settle").Scan(&createUser, &applyUser)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if createUser != 123 || applyUser != 123 {
		t.Fatalf("BIGINT user column mismatch: create_user=%d apply_user_id=%d", createUser, applyUser)
	}
}

// ─── 1.8.0 SYNC 同步演进 ───────────────────────────────────────────────────────

// registerSyncFlow 注入 SYNC 模式：persistMode + task1 字段权限 + 结束节点改名 finish
func registerSyncFlow(repo *memory.Repository, tableName string) *model.ProcessDefine {
	data, err := os.ReadFile("../../jeeflow-java/jeeflow-core/src/test/resources/flows/01-simple.json")
	if err != nil {
		panic(err.Error())
	}
	content := string(data)
	content = strings.ReplaceAll(content, `"type": "approval"`,
		`"type": "approval", "relTableName": "`+tableName+`", "persistMode": "SYNC"`)
	content = strings.ReplaceAll(content, `"assignee": "leader"`,
		`"assignee": "leader", "field": {"PERMISSION_f_title": 1, "PERMISSION_amount": 2}`)
	content = strings.ReplaceAll(content, `"id": "end"`, `"id": "finish"`)
	content = strings.ReplaceAll(content, `"targetNodeId": "end"`, `"targetNodeId": "finish"`)
	def := &model.ProcessDefine{ID: 2, Name: "simple", DisplayName: "01-simple.json", Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return def
}

func newSyncDB(t *testing.T) (*sql.DB, *persist.JdbcDynamicTableWriter) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE biz_sync (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT,
		amount REAL,
		opinion TEXT,
		apply INTEGER,
		task1 INTEGER,
		finish INTEGER,
		process_instance_id INTEGER,
		apply_user_id TEXT,
		apply_dept_id TEXT,
		create_time TEXT,
		create_user TEXT,
		update_time TEXT,
		update_user TEXT,
		is_deleted INTEGER
	)`)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	return db, persist.NewJdbcDynamicTableWriter(db)
}

// ⑥ SYNC 全链路：发起 INSERT → apply 推进 → task1（权限过滤 + tf_ + 状态）→ 结束定稿
func TestSyncModeFullCycle(t *testing.T) {
	repo := memory.New()
	db, w := newSyncDB(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})
	registerSyncFlow(repo, "biz_sync")

	// ① 发起 → INSERT（title/amount）
	inst, err := eng.StartProcessInstanceByID(context.Background(), 2, "user1",
		map[string]interface{}{"f_title": "年假申请", "f_amount": 800.0, "u_deptId": "D01"})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	// ② apply 完成 → UPDATE（apply 状态=10）
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"user1"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "user1")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeApply)}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	var apply int
	if err := db.QueryRow("SELECT apply FROM biz_sync").Scan(&apply); err != nil {
		t.Fatalf("query apply failed: %v", err)
	}
	if apply != 10 {
		t.Fatalf("apply state mismatch: %d", apply)
	}
	// ③ task1（leader）→ UPDATE：title 只读不更新 / amount 可编辑更新 / opinion(tf_) / task1=10 / finish=20
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader",
		map[string]interface{}{
			engine.KeySubmitType: int(model.SubmitTypeAgree),
			"tf_opinion":         "同意",
			"f_title":            "修改标题",
			"f_amount":           999.0,
		}); err != nil {
		t.Fatalf("task1 failed: %v", err)
	}
	var title, opinion string
	var amount float64
	var task1, finish int
	if err := db.QueryRow("SELECT title, amount, opinion, task1, finish FROM biz_sync").
		Scan(&title, &amount, &opinion, &task1, &finish); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if title != "年假申请" {
		t.Fatalf("readonly field updated: title=%s", title)
	}
	if amount != 999.0 {
		t.Fatalf("editable field not updated: amount=%v", amount)
	}
	if opinion != "同意" {
		t.Fatalf("tf_ redundant not persisted: opinion=%s", opinion)
	}
	if task1 != 10 {
		t.Fatalf("task1 state mismatch: %d", task1)
	}
	if finish != 20 {
		t.Fatalf("finish state mismatch: %d", finish)
	}
}

// ⑦ SYNC 驳回：结束定稿最终状态 REJECT=45，数据不丢
func TestSyncModeReject(t *testing.T) {
	repo := memory.New()
	db, w := newSyncDB(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})
	registerSyncFlow(repo, "biz_sync")

	inst, err := eng.StartProcessInstanceByID(context.Background(), 2, "user1",
		map[string]interface{}{"f_title": "驳回单", "u_deptId": "D01"})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"user1"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "user1")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeApply)}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeReject)}); err != nil {
		t.Fatalf("task1 failed: %v", err)
	}

	var title string
	var finish int
	var createUser string
	if err := db.QueryRow("SELECT title, finish, create_user FROM biz_sync").
		Scan(&title, &finish, &createUser); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if title != "驳回单" {
		t.Fatalf("reject row lost: title=%s", title)
	}
	if finish != 45 {
		t.Fatalf("reject final state mismatch: %d", finish)
	}
	if createUser != "user1" {
		t.Fatalf("create_user mismatch: %s", createUser)
	}
}

// ─── 测试辅助（与 engine_test 同构） ──────────────────────────────────────────

type testUserProv struct{}

func (p *testUserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID, DeptID: "D01", DeptName: "测试部门", PostID: "P01", PostName: "测试岗位"}, nil
}

type testIDGen struct{ n int64 }

func (g *testIDGen) NextID() int64 { g.n++; return g.n }

type testExprEval struct{}

func (e *testExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	return false, nil
}

// ⑧ issues/26：办理提交被拒字段（只读/隐藏）不入变量——下游无权限节点无法绕过上游只读
func TestSyncPermBypass(t *testing.T) {
	repo := memory.New()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE biz_perm3 (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT, amount REAL,
		apply INTEGER, approve1 INTEGER, approve2 INTEGER, finish INTEGER,
		process_instance_id INTEGER,
		create_user TEXT, is_deleted INTEGER
	)`)
	w := persist.NewJdbcDynamicTableWriter(db)
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	content := `{"name": "perm3", "displayName": "权限绕过验证", "type": "approval",
		"relTableName": "biz_perm3", "persistMode": "SYNC",
		"nodes": [
			{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
			{"id": "apply", "type": "snaker:task", "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}, "text": {"value": "发起申请"}},
			{"id": "approve1", "type": "snaker:task", "properties": {"assignee": "leader1", "taskType": 0, "performType": 0, "field": {"PERMISSION_f_title": 1, "PERMISSION_amount": 2}}, "text": {"value": "审批一"}},
			{"id": "approve2", "type": "snaker:task", "properties": {"assignee": "leader2", "taskType": 0, "performType": 0}, "text": {"value": "审批二"}},
			{"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}
		],
		"edges": [
			{"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
			{"id": "e1", "sourceNodeId": "apply", "targetNodeId": "approve1", "properties": {}},
			{"id": "e2", "sourceNodeId": "approve1", "targetNodeId": "approve2", "properties": {}},
			{"id": "e3", "sourceNodeId": "approve2", "targetNodeId": "finish", "properties": {}}
		]}`
	def := &model.ProcessDefine{ID: 2, Name: "perm3", Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)

	inst, err := eng.StartProcessInstanceByID(context.Background(), 2, "user1",
		map[string]interface{}{"f_title": "原始标题", "f_amount": 800.0, "u_deptId": "D01"})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	completeNamed := func(name, actor string, args map[string]interface{}) {
		doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		for _, d := range doing {
			if d.TaskName == name {
				repo.AddTaskActor(context.Background(), d.ID, []string{actor})
				d.ActorIDs = append(d.ActorIDs, actor)
				if _, err := eng.ExecuteProcessTask(context.Background(), d.ID, actor, args); err != nil {
					t.Fatalf("%s failed: %v", name, err)
				}
				return
			}
		}
		t.Fatalf("task %s not found", name)
	}
	completeNamed("apply", "user1", map[string]interface{}{engine.KeySubmitType: 0})
	// approve1 只读 title，提交 TRY_HACK → 引擎入口过滤 → 不入变量 → 不落库
	completeNamed("approve1", "leader1",
		map[string]interface{}{engine.KeySubmitType: 1, "f_title": "TRY_HACK"})
	// approve2 无权限声明——变量无 TRY_HACK，title 保持原值
	completeNamed("approve2", "leader2",
		map[string]interface{}{engine.KeySubmitType: 1, "f_amount": 999.0})

	var title string
	var amount float64
	var approve1, approve2, finish int
	if err := db.QueryRow("SELECT title, amount, approve1, approve2, finish FROM biz_perm3").
		Scan(&title, &amount, &approve1, &approve2, &finish); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if title != "原始标题" {
		t.Fatalf("readonly bypass: title=%s", title)
	}
	if amount != 999.0 {
		t.Fatalf("editable not updated: amount=%v", amount)
	}
	if approve1 != 10 || approve2 != 10 || finish != 20 {
		t.Fatalf("state mismatch: %d/%d/%d", approve1, approve2, finish)
	}
}

// ⑨ issues/34：定义级拦截器（postInterceptors 声明 + 注册表按名解析，未声明不触发）
func TestDefineLevelInterceptor(t *testing.T) {
	repo := memory.New()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE biz_decl (
		id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT,
		process_instance_id INTEGER, is_deleted INTEGER
	)`)
	w := persist.NewJdbcDynamicTableWriter(db)
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	// 注册表挂载（定义级）
	eng.SetExtensions(&engine.Extensions{Interceptors: nil,
		InterceptorRegistry: map[string]engine.FlowInterceptor{"persist": ic}})

	loadFlow := func(name, table, declared string) *model.ProcessDefine {
		content := fmt.Sprintf(`{"name": %q, "displayName": %q, "type": "approval",
			"relTableName": %q, "persistMode": "SYNC", "postInterceptors": %q,
			"nodes": [{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
			          {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
			"edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "finish", "properties": {}}]}`, name, name, table, declared)
		return &model.ProcessDefine{ID: 0, Name: name, Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	}
	d1 := loadFlow("decl1", "biz_decl", "persist")
	repo.AddDefine(d1)
	if _, err := eng.StartProcessInstanceByID(context.Background(), d1.ID, "user1",
		map[string]interface{}{"f_title": "声明流程"}); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	var n int
	db.QueryRow("SELECT COUNT(1) FROM biz_decl").Scan(&n)
	if n != 1 {
		t.Fatalf("声明了拦截器的流程应落库: %d", n)
	}
	d2 := loadFlow("decl2", "biz_decl", "")
	repo.AddDefine(d2)
	if _, err := eng.StartProcessInstanceByID(context.Background(), d2.ID, "user2",
		map[string]interface{}{"f_title": "未声明流程"}); err != nil {
		t.Fatalf("start2 failed: %v", err)
	}
	db.QueryRow("SELECT COUNT(1) FROM biz_decl").Scan(&n)
	if n != 1 {
		t.Fatalf("未声明拦截器的流程不应落库: %d", n)
	}
}

// ⑩ issues/30/31：facade 顶层 JSON + listByType + bizData
func TestFacadeListByTypeAndTopLevelJSON(t *testing.T) {
	repo := memory.New()
	ext := memory.NewExt()
	_ = ext.SaveDesign(context.Background(), &model.ProcessDesign{ID: 1, Name: "old", DisplayName: "旧名", Type: "approval"})
	_ = ext.SaveDesign(context.Background(), &model.ProcessDesign{ID: 2, Name: "old2", DisplayName: "旧名2", Type: "approval"})
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	fac := facade.New(eng, repo, ext)

	// 顶层 JSON 保存（无 content）——issue 31
	r := fac.Flow("processDesign/updateDefine", map[string]interface{}{
		"processDesignId": int64(1), "operator": "user1",
		"name": "topjson", "displayName": "顶层JSON", "type": "approval",
		"relTableName": "biz_top", "nodes": []interface{}{}, "edges": []interface{}{}})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	d1, _ := ext.FindDesignByID(context.Background(), 1)
	if d1.Name != "topjson" {
		t.Fatalf("设计 name 未同步: %v", d1.Name)
	}
	his1, _ := ext.ListDesignHis(context.Background(), 1)
	if len(his1) == 0 || !strings.Contains(string(his1[0].Content), `"nodes"`) {
		t.Fatalf("顶层 JSON 应序列化为内容快照")
	}
	// listByType——issue 30
	r = fac.Flow("processDesign/listByType", map[string]interface{}{})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	groups, _ := r["data"].(map[string][]map[string]interface{})
	approval := groups["approval"]
	if len(approval) == 0 {
		t.Fatalf("应含 approval 分组: %v", groups)
	}
	found := false
	for _, item := range approval {
		if item["name"] == "topjson" {
			found = true
		}
	}
	if !found {
		t.Fatalf("分组应含 topjson: %v", approval)
	}
	// bizData：未注册 → 报错；注册后回显
	// 先部署真实流程（01-simple + relTableName）
	simple, _ := os.ReadFile("../../jeeflow-java/jeeflow-core/src/test/resources/flows/01-simple.json")
	content := strings.ReplaceAll(string(simple), `"type": "approval"`, `"type": "approval", "relTableName": "biz_top"`)
	r = fac.Flow("processDesign/updateDefine", map[string]interface{}{
		"processDesignId": int64(2), "operator": "user1", "content": content})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	r = fac.Flow("processDesign/deploy", map[string]interface{}{"id": int64(2), "operator": "user1"})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	defineID := r["data"].(map[string]interface{})["processDefineId"]
	r = fac.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "user1", "f_title": "x"})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	instID := r["data"].(map[string]interface{})["processInstanceId"]
	r = fac.Flow("processInstance/bizData", map[string]interface{}{"processInstanceId": instID})
	if codeOf(r) == 0 {
		t.Fatalf("未注册 reader 应报错: %v", r)
	}
	fac.SetMetaReader(&mockMetaReader{})
	r = fac.Flow("processInstance/bizData", map[string]interface{}{"processInstanceId": instID})
	if codeOf(r) != 0 {
		t.Fatalf("flow failed: %v", r)
	}
	if r["data"].(map[string]interface{})["tableName"] != "biz_top" {
		t.Fatalf("bizData 表名错误: %v", r)
	}
}

func codeOf(r map[string]interface{}) int {
	if c, ok := r["code"].(int); ok {
		return c
	}
	if f, ok := r["code"].(float64); ok {
		return int(f)
	}
	return -1
}

type mockMetaReader struct{}

func (m *mockMetaReader) ReadByProcessInstance(tableName string, processInstanceID interface{}) (interface{}, error) {
	return map[string]interface{}{"tableName": tableName, "title": "业务数据"}, nil
}
