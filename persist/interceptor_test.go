package persist_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/engine"
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
		applyDept != "D01" || createUser != "system" || deleted != 0 {
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

	// 模拟重复触发：直接对结束节点再调一次拦截器
	ic.PostHandle(&model.FlowNode{Type: model.TypeEnd}, inst)
	if n := countRows(t, db); n != 1 {
		t.Fatalf("idempotent failed, rows=%d", n)
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
