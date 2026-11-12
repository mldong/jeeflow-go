// 节点级 taskType=1（协办）解析期禁配（issues/252，规范 02 §4.4）——go 腿。
//
// 正向：折成 SECONDARY 的全部输入形（1／"1"／"协办"／"SECONDARY"，大小写不敏感——
// 本栈无既有 taskType 容错表，判定照 IsCountersign 同尺）解析期报错、逐字文案、零写入；
// 回归：0／"Major"／缺省／未知值 "Aidant" 仍发起成功且建单回落主办 TaskType=0
// （issues/245 口径不变）；custom 记录节点历史行形状不变（CreateHistoryTask 恒 0）。
//
// 夹具内联 JSON，不碰共享 flows/ 目录；SPI 与片段生成器复用同包 add_sign_test.go。
package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

const tt252Msg = "节点[review]不允许配置协办任务类型（taskType=1）"

var tt252Seq int

// ttFlowJSON start → apply → review → final → end；reviewTaskTypeRaw＝review 节点
// taskType 的原始 JSON 片段（空串＝缺省键）。
func ttFlowJSON(name, reviewTaskTypeRaw string) string {
	rprops := `"form":"form-r","assignee":"userA","performType":0`
	if reviewTaskTypeRaw != "" {
		rprops += `,"taskType":` + reviewTaskTypeRaw
	}
	return `{"name":"` + name + `","displayName":"协办禁配夹具","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"`, "发起申请"),
			asNode("review", model.TypeTask, rprops, "审批"),
			asNode("final", model.TypeTask, `"form":"form-f","assignee":"userF","taskType":0,"performType":0`, "终审"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "review"),
			asEdge("e3", "review", "final"), asEdge("e4", "final", "end")}, ",") + `]}`
}

// ttCustomFlowJSON start → apply → c1(custom，带 taskType:2 属性) → end。
func ttCustomFlowJSON(name string) string {
	return `{"name":"` + name + `","displayName":"custom 回归夹具","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"`, "发起申请"),
			asNode("c1", model.TypeCustom, `"taskType":2`, "记录"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "c1"),
			asEdge("e3", "c1", "end")}, ",") + `]}`
}

func ttHarness(t *testing.T, content string) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	tt252Seq++
	name := fmt.Sprintf("tt252_%d", tt252Seq)
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "协办禁配夹具", Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return engine.New(repo, asUserProv{}, &asIDGen{}, nil), repo, def.ID
}

// ttStartToReview 发起并办结申请节点，返回停在 review 的实例。
func ttStartToReview(t *testing.T, content string) (*engine.EngineImpl, *memory.Repository, *model.ProcessInstance) {
	t.Helper()
	eng, repo, defID := ttHarness(t, content)
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起流程失败: %v", err)
	}
	doing, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil {
		t.Fatalf("读待办失败: %v", err)
	}
	for _, tk := range doing {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant",
			map[string]interface{}{"submitType": int(model.SubmitTypeApply)}); err != nil {
			t.Fatalf("办结申请节点失败: %v", err)
		}
	}
	back, err := repo.FindInstanceByID(context.Background(), inst.ID)
	if err != nil {
		t.Fatalf("读回实例失败: %v", err)
	}
	return eng, repo, back
}

// ═══ 正向 ═══

// 格 1：折成协办的全部输入形 ⇒ 解析期（发起腿）报错、逐字文案、零写入。
func TestTaskType252SecondaryFormsRejectedAtParse(t *testing.T) {
	for _, raw := range []string{`1`, `"1"`, `"协办"`, `"SECONDARY"`, `"secondary"`} {
		name := fmt.Sprintf("tt252_bad_%d", tt252Seq+1)
		eng, repo, defID := ttHarness(t, ttFlowJSON(name, raw))
		_, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", map[string]interface{}{})
		if err == nil {
			t.Fatalf("应拦：taskType=%s，却发起了", raw)
		}
		if err.Error() != tt252Msg {
			t.Fatalf("逐字文案（taskType=%s）：want %q, got %q", raw, tt252Msg, err.Error())
		}
		has, herr := repo.HasInstancesByDefineID(context.Background(), defID)
		if herr != nil {
			t.Fatalf("查实例失败: %v", herr)
		}
		if has {
			t.Fatalf("解析报错必须零写入（taskType=%s）", raw)
		}
	}
}

// ═══ 回归 ═══

// 格 2：0／"Major"／缺省／未知值 "Aidant" 仍发起成功，待办行回落主办 TaskType=0。
func TestTaskType252MajorAndUnknownFormsFallbackUnchanged(t *testing.T) {
	for _, raw := range []string{`0`, `"0"`, `"Major"`, `"MAJOR"`, `"主办"`, `"Aidant"`, ``} {
		name := fmt.Sprintf("tt252_ok_%d", tt252Seq+1)
		_, repo, inst := ttStartToReview(t, ttFlowJSON(name, raw))
		doing, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		if err != nil {
			t.Fatalf("读待办失败: %v", err)
		}
		var review *model.ProcessTask
		for _, tk := range doing {
			if tk.TaskName == "review" {
				review = tk
			}
		}
		if review == nil {
			t.Fatalf("taskType=%q 应停在 review", raw)
		}
		if review.TaskType != 0 {
			t.Fatalf("taskType=%q 待办行应回落主办 0，实得 %d", raw, review.TaskType)
		}
	}
}

// 格 3：custom 记录节点历史行不变——taskType:2 属性不触发禁配，DONE 行照落（本栈形状恒 0）。
func TestTaskType252CustomHistoryRowUnchanged(t *testing.T) {
	name := fmt.Sprintf("tt252_custom_%d", tt252Seq+1)
	eng, repo, inst := ttStartToReview(t, ttCustomFlowJSON(name))
	rows, err := repo.FindHistoryTasks(context.Background(), inst.ID)
	if err != nil {
		t.Fatalf("读历史行失败: %v", err)
	}
	var history *model.ProcessTask
	for _, tk := range rows {
		if tk.TaskName == "c1" {
			history = tk
		}
	}
	if history == nil {
		t.Fatal("custom 记录行必须照落")
	}
	if history.TaskState != model.TaskStateDone {
		t.Fatalf("custom 行应为 DONE，实得 %d", history.TaskState)
	}
	if history.TaskType != 0 {
		t.Fatalf("本栈历史行 taskType 形状不变（恒 0），实得 %d", history.TaskType)
	}
	doing, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil {
		t.Fatalf("读待办失败: %v", err)
	}
	if len(doing) != 0 {
		t.Fatalf("令牌应续流到 end，仍有 %d 条待办", len(doing))
	}
	_ = eng
}