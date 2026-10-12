// issues/178 R12（audit8 队列 R12；spec 06:722-730）· go 切片判据：
// candidatePage 模型候选映射链（findById→getUser→兜底）＋行键归一（id 三级回落）。
// 形状照 java JeeflowFacade.java:945-1012；三档对齐 issues/178 §4.1：
//
//	档① 仅 findById（命中）：行 id/realName/userId/deptName 取 findById 值，且压过 getUser；
//	    档①b findById 无 id 键（有 userId）：id 走 src.userId 回落；
//	档② 无 findById 有 getUser：realName/deptName 取 getUser，id 走 src.userId 回落；
//	档③ 双无：兜底 id==actor、realName==id（回归档，改前行为）。
//
// 修前红样：档①/①b/③ 全走恒兜底 {c,c,c}（realName 丢真名）；变异对照见 commit 记录。
package facade_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// i178Flow 单候选流程：apply 为待办面，review 的 candidateUsers=u100。
const i178Flow = `{
  "name": "i178-cand",
  "displayName": "178候选",
  "type": "approval",
  "nodes": [
    {"id": "start", "type": "snaker:start", "properties": {}},
    {"id": "apply", "type": "snaker:task",
     "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}},
    {"id": "review", "type": "snaker:task",
     "properties": {"candidateUsers": "u100", "taskType": 0, "performType": 0}},
    {"id": "end", "type": "snaker:end", "properties": {}}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "review", "properties": {}},
    {"id": "e2", "sourceNodeId": "review", "targetNodeId": "end", "properties": {}}
  ]
}`

// i178MissUp 双无档：GetUser 恒未命中。
type i178MissUp struct{}

func (p *i178MissUp) GetUser(userID string) (*model.UserInfo, error) { return nil, nil }

// i178Setup 装配：内存仓 + 指定 userProv；发起后取 apply 待办任务 id。
func i178Setup(t *testing.T, up spi.UserProvider) (*facade.Facade, int64) {
	t.Helper()
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, up, &testIDGen{}, &testExprEval{})
	f := facade.New(eng, repo, extRepo)
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": i178Flow})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r0)
	}
	def, err := repo.FindDefineByName(context.Background(), "i178-cand")
	if err != nil || def == nil {
		t.Fatalf("define not found: %v", err)
	}
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 {
		t.Fatalf("want 1 doing task, got %+v", doing)
	}
	return f, doing[0].ID
}

// i178FirstRow 打 candidatePage 并取唯一候选行。
func i178FirstRow(t *testing.T, f *facade.Facade, tid int64) map[string]interface{} {
	t.Helper()
	r := f.Flow("processTask/candidatePage", map[string]interface{}{"processTaskId": tid})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("candidatePage failed: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	rows, _ := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("候选行数 = %d, want 1: %v", len(rows), rows)
	}
	m, _ := rows[0].(map[string]interface{})
	return m
}

func TestI178FindByIDFirstWins(t *testing.T) {
	// testUserProv 作反证：若第二级被消费，realName 会是「用户u100」而非「张三」。
	f, tid := i178Setup(t, &testUserProv{})
	f.SetUserSearchByID(func(uid string) (map[string]interface{}, error) {
		if uid != "u100" {
			t.Fatalf("findById 收到 %q, want u100", uid)
		}
		return map[string]interface{}{
			"id": "U100", "userId": "U100", "realName": "张三", "deptName": "研发部",
		}, nil
	})
	m := i178FirstRow(t, f, tid)
	if m["id"] != "U100" || m["realName"] != "张三" || m["userId"] != "U100" || m["deptName"] != "研发部" {
		t.Fatalf("档①行应取 findById 真值: %v", m)
	}
}

func TestI178IDFallbackToUserID(t *testing.T) {
	f, tid := i178Setup(t, &testUserProv{})
	f.SetUserSearchByID(func(uid string) (map[string]interface{}, error) {
		return map[string]interface{}{"userId": "U200", "realName": "李四"}, nil
	})
	m := i178FirstRow(t, f, tid)
	if m["id"] != "U200" || m["realName"] != "李四" || m["userId"] != "U200" {
		t.Fatalf("档①b 行 id 应回落 src.userId: %v", m)
	}
}

func TestI178GetUserSecondLevel(t *testing.T) {
	f, tid := i178Setup(t, &testUserProv{})
	m := i178FirstRow(t, f, tid)
	if m["id"] != "u100" || m["realName"] != "用户u100" || m["userId"] != "u100" || m["deptName"] != "测试部门" {
		t.Fatalf("档②行应取 getUser: %v", m)
	}
}

func TestI178FallbackBothMiss(t *testing.T) {
	f, tid := i178Setup(t, &i178MissUp{})
	m := i178FirstRow(t, f, tid)
	if m["id"] != "u100" || m["realName"] != "u100" || m["userId"] != "u100" {
		t.Fatalf("档③行应兜底 actor: %v", m)
	}
}
