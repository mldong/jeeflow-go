package facade_test

// issues/177 R11 · 门面 candidatePage「field 子对象」候选兜底——旧设计器把 candidateUsers /
// candidateGroups 写进 properties.field（12 夹具字段的同款抽屉），门面 nextTaskCandidates 只认
// 顶层 ⇒ 候选恒空、回落 userSearch 钩子（未配置直接报错）。修后顶层优先、field 兜底
// （形状对齐 issues/163 会签条件附带读形）。夹具＝内联四节点流（start→apply→review→end）。

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
)

func i177Flow(topLevelUsers string) string {
	field := `{"candidateUsers":"userA,userB","candidateGroups":"finance"}`
	top := ""
	if topLevelUsers != "" {
		top = `,"candidateUsers":"` + topLevelUsers + `"`
	}
	return `{"name":"i177-candidate-field","displayName":"i177候选字段","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"S"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","performType":0},"text":{"value":"A"}},` +
		`{"id":"review","type":"snaker:task","properties":{"assignee":"leader","field":` + field + top + `},"text":{"value":"R"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"E"}}],` +
		`"edges":[{"id":"e0","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e1","sourceNodeId":"apply","targetNodeId":"review","properties":{}},` +
		`{"id":"e2","sourceNodeId":"review","targetNodeId":"end","properties":{}}]}`
}

func i177Candidates(t *testing.T, content string) []string {
	t.Helper()
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	f := facade.New(eng, repo, extRepo)
	f.SetOrgUserProvider(&testOrgUserProv{})

	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r0)
	}
	def, err := repo.FindDefineByName(context.Background(), "i177-candidate-field")
	if err != nil || def == nil {
		t.Fatalf("define not found: %v", err)
	}
	// 直接启动（不自动完成 apply）→ apply 任务 → candidatePage 查 review 候选
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "apply" {
		t.Fatalf("want apply task, got %+v", doing)
	}
	r := f.Flow("processTask/candidatePage", map[string]interface{}{"processTaskId": doing[0].ID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("candidatePage failed: %v", r)
	}
	data := r["data"].(map[string]interface{})
	rows := data["rows"].([]interface{})
	out := make([]string, 0, len(rows))
	for _, rw := range rows {
		m, _ := rw.(map[string]interface{})
		out = append(out, m["userId"].(string))
	}
	return out
}

func TestI177CandidateFieldFallback(t *testing.T) {
	got := i177Candidates(t, i177Flow(""))
	if len(got) != 4 {
		t.Fatalf("field 内两键应读出 4 候选（userA/userB+finA/finB），实得 %v（改前恒 0 ⇒ 回落钩子报错）", got)
	}
	for _, want := range []string{"userA", "userB", "finA", "finB"} {
		if !containsStr2(got, want) {
			t.Fatalf("缺少候选 %s: %v", want, got)
		}
	}
}

func TestI177CandidateTopLevelWins(t *testing.T) {
	got := i177Candidates(t, i177Flow("userX"))
	if !containsStr2(got, "userX") || containsStr2(got, "userA") {
		t.Fatalf("顶层 candidateUsers 应压过 field 内版本: %v", got)
	}
	if !containsStr2(got, "finA") {
		t.Fatalf("candidateGroups 腿应独立从 field 兜底（键级双源）: %v", got)
	}
}
