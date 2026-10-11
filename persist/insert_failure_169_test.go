package persist_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/persist"
)

// issues/169 负向夹具：只让 Insert 失败，其余腿透传给真 writer。
// 必须是"Exists 成功、Insert 失败"这一形状——那正是旧代码 `_, _ = p.Writer.Insert(...)` 吞掉的那一腿；
// 若直接给一张不存在的表，命中的是 Exists 那条已经会 panic 的腿，等于没测本案。
type failInsertWriter struct {
	*persist.JdbcDynamicTableWriter
	insertCalls int
	lastTable   string
}

func (f *failInsertWriter) Insert(ctx context.Context, tableName string, data map[string]interface{}) (interface{}, error) {
	f.insertCalls++
	f.lastTable = tableName
	return nil, fmt.Errorf("Column 'apply_user_id' cannot be null")
}

// 取 panic 值并核文案（三枚断言：炸了／带表名／错误链没被截断）
func mustPersistPanic(t *testing.T, recovered interface{}, wantTable string) {
	t.Helper()
	if recovered == nil {
		t.Fatal("issues/169：Insert 写失败必须显性失败——旧形状 `_, _ =` 吞成「流程照常办结＋业务表零行零日志」")
	}
	msg := fmt.Sprint(recovered)
	t.Logf("panic 原文：%s", msg)
	for _, want := range []string{"业务表写入失败", wantTable, "cannot be null"} {
		if !contains(msg, want) {
			t.Fatalf("panic 文案应含 %q，实得 %q", want, msg)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ① ARCHIVE 归档腿（interceptor.go 首建 INSERT）：表在、写失败 ⇒ panic，且业务表仍零行
func TestIssue169ArchiveInsertFailurePanics(t *testing.T) {
	repo := memory.New()
	db, base := newWriter(t)
	defer db.Close()
	w := &failInsertWriter{JdbcDynamicTableWriter: base}

	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	registerPersistFlow(repo, true) // relTableName=biz_leave，ARCHIVE 缺省

	inst := &model.ProcessInstance{
		ID: 900169001, DefineID: 1, State: model.InstanceStateDone,
		Variables: map[string]interface{}{
			engine.KeySubmitType: int(model.SubmitTypeAgree),
			"f_title":            "年假申请",
		},
	}

	func() {
		defer func() { mustPersistPanic(t, recover(), "biz_leave") }()
		ic.PostHandle(context.Background(), &model.FlowNode{ID: "end", Type: model.TypeEnd}, inst)
	}()

	if w.insertCalls != 1 {
		t.Fatalf("应真的走到 Insert 那一腿（否则本用例是空转），calls=%d", w.insertCalls)
	}
	if w.lastTable != "biz_leave" {
		t.Fatalf("表名应透传给 writer，实得 %q", w.lastTable)
	}
	if n := countRows(t, db); n != 0 {
		t.Fatalf("写失败不该留下半行，rows=%d", n)
	}
}

// ② SYNC 首建腿（handleSync 的 !exists 分支）：与上面的 Update 腿同档，不能一个炸一个咽
func TestIssue169SyncFirstInsertFailurePanics(t *testing.T) {
	repo := memory.New()
	db, base := newSyncDB(t)
	defer db.Close()
	w := &failInsertWriter{JdbcDynamicTableWriter: base}

	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	registerSyncFlow(repo, "biz_sync") // define ID=2，persistMode=SYNC

	inst := &model.ProcessInstance{
		ID: 900169002, DefineID: 2, State: model.InstanceStateDoing,
		Variables: map[string]interface{}{"f_title": "年假申请", "f_amount": 800.0},
	}

	func() {
		defer func() { mustPersistPanic(t, recover(), "biz_sync") }()
		ic.PostHandle(context.Background(), &model.FlowNode{ID: "start", Type: model.TypeStart}, inst)
	}()

	if w.insertCalls != 1 {
		t.Fatalf("SYNC 首建腿未被走到（用例空转），calls=%d", w.insertCalls)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(1) FROM biz_sync").Scan(&n); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("写失败不该留下半行，rows=%d", n)
	}
}

// ③ 回归：写成功那一腿不受本次改动影响（同夹具、真 writer、ARCHIVE 正常落库一行）
func TestIssue169ArchiveInsertSuccessStillPersists(t *testing.T) {
	repo := memory.New()
	db, w := newWriter(t)
	defer db.Close()
	ic := persist.NewPersistPostInterceptor(w, repo.FindDefineByID)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})

	def := registerPersistFlow(repo, true)
	runFlow(t, eng, repo, def.ID, true)

	if n := countRows(t, db); n != 1 {
		t.Fatalf("写成功应落库一行（回归位被本次改动打红过＝修法歪了），rows=%d", n)
	}
}
