// issues/163 · go 并行会签「放行表达式真求值」——对齐 java（CountersignHandler）＋ rust gate_vars。
//
// 改前形状：countersignCompletionCondition 只被拿去比 ONE_VOTE_VETO 一档，非否决值从不交
// 求值器 ⇒ `#nrOfCompletedInstances>=2` 被完全忽略，放行只由"是否还有 DOING"决定（三人会签
// 要三人全办完才推进，与配置阈值语义相反）。本案（owner 2026-10-10 拍"修"）＝cond 交给同一条
// 求值通道（engine 的 exprEval），门控计数（total/activate/completed，**裸名**）并进求值原料，
// 照 rust `gate_vars` 那一步；`ONE_VOTE_VETO` 判定顺序保持原样。
//
// 判据档：
// ① 三人并行 + `#nrOfCompletedInstances>=2`：第 1 票后**仍停留**；第 2 票后**立即放行**
//    （state=20、第三位成员行被废弃 ABANDONED=99）——改前红在"第 2 票后仍停留"。
// ② 对照档（守过度合并）：三人并行**不带 cond**：两票后仍停留，第三票才放行。
// ③ `ONE_VOTE_VETO` 原样：submitType=20 ⇒ 立即合并（B/C 待废）。
// ④ 嵌套读形（附带对齐）：cond 写在设计器实际落笔处 `properties.field` 子对象里（共享夹具
//    07 就是这个形状），引擎"顶层优先、嵌套兜底"读取 ⇒ 与 ① 同判（第 2 票放行）。
//
// 夹具＝**内联**三节点流程（start → apply → 三人并行会签 task1 → end，与 python 侧
// `tests/test_countersign_cond_i163.py`、活栈探针 probe_live2.py 的 CS_FLOW 同形）；
// 不新增共享 flows/ 夹具（编辑源在 jeeflow-java，本轮不动跨仓夹具面）。
package jeeflow_test

import (
	"context"
	"regexp"
	"strconv"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// i163ExprEval 壳侧同款简易求值器：`#` 门控变量剥前缀裸名查（查不到判 false，
// 与 java SpEL catch ⇒ false 同档）；同时保留 amount 比较（本夹具不用，留着对照日常语义）。
type i163ExprEval struct{}

var i163Re = regexp.MustCompile(`^\s*#?(\w+)\s*(>=|<=|!=|==|>|<)\s*(\d+(?:\.\d+)?)\s*$`)

func (e *i163ExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	m := i163Re.FindStringSubmatch(expr)
	if m == nil {
		return false, nil
	}
	val, _ := strconv.ParseFloat(m[3], 64)
	raw, ok := vars[m[1]]
	if !ok {
		return false, nil
	}
	av := toFloat(raw)
	switch m[2] {
	case ">":
		return av > val, nil
	case ">=":
		return av >= val, nil
	case "<":
		return av < val, nil
	case "<=":
		return av <= val, nil
	case "==":
		return av == val, nil
	case "!=":
		return av != val, nil
	}
	return false, nil
}

func i163Flow(name, cond string) string {
	condProp := ""
	if cond != "" {
		condProp = `,"countersignCompletionCondition":` + strconv.Quote(cond)
	}
	return `{"name":"` + name + `","displayName":"i163会签条件","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","performType":0},"text":{"value":"发起申请"}},` +
		`{"id":"task1","type":"snaker:task","properties":{"assignee":"userA,userB,userC","performType":"1","countersignType":"PARALLEL"` + condProp + `},"text":{"value":"会签审批"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],` +
		`"edges":[{"id":"e0","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e1","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},` +
		`{"id":"e2","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`
}

// i163FlowNested ④：cond 藏在 properties.field 子对象里（设计器实际落笔处）。
func i163FlowNested(name, cond string) string {
	return `{"name":"` + name + `","displayName":"i163会签条件(嵌套形)","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","performType":0},"text":{"value":"发起申请"}},` +
		`{"id":"task1","type":"snaker:task","properties":{"assignee":"userA,userB,userC","performType":"1","countersignType":"PARALLEL","field":{"countersignCompletionCondition":` + strconv.Quote(cond) + `}},"text":{"value":"会签审批"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],` +
		`"edges":[{"id":"e0","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e1","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},` +
		`{"id":"e2","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`
}

func i163StartFrom(t *testing.T, name, content string) (*engine.EngineImpl, *memory.Repository, *model.ProcessInstance) {
	t.Helper()
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &i163ExprEval{})
	def := &model.ProcessDefine{Name: name, DisplayName: name, Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	inst := startAndExecute(eng, repo, def.ID, "user1", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 3 {
		t.Fatalf("三人并行会签应有 3 条成员待办，实得 %d", len(doing))
	}
	return eng, repo, inst
}

func i163Start(t *testing.T, name, cond string) (*engine.EngineImpl, *memory.Repository, *model.ProcessInstance) {
	t.Helper()
	return i163StartFrom(t, name, i163Flow(name, cond))
}

func i163Vote(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, instID int64, who string, args map[string]interface{}) {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instID, nil)
	if len(doing) == 0 {
		t.Fatalf("%s 应有待办可办", who)
	}
	task := doing[0]
	repo.AddTaskActor(context.Background(), task.ID, []string{who})
	task.ActorIDs = append(task.ActorIDs, who)
	if _, err := eng.ExecuteProcessTask(context.Background(), task.ID, who, args); err != nil {
		t.Fatalf("execute %s: %v", who, err)
	}
}

func i163State(t *testing.T, repo *memory.Repository, instID int64) model.InstanceState {
	t.Helper()
	inst, _ := repo.FindInstanceByID(context.Background(), instID)
	if inst == nil {
		t.Fatal("实例读回为空")
	}
	return inst.State
}

// TestI163ThresholdMergesAtTwoOfThree ① 核心格：`#nrOfCompletedInstances>=2` ⇒ 第 2 票立即
// 放行、第三位成员行被废弃（改前此格红：仍停留）。
func TestI163ThresholdMergesAtTwoOfThree(t *testing.T) {
	eng, repo, inst := i163Start(t, "i163-cond", "#nrOfCompletedInstances>=2")

	i163Vote(t, eng, repo, inst.ID, "userA", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDoing {
		t.Fatalf("第 1 票阈值未达，仍应停留: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 2 {
		t.Fatalf("第 1 票后应还剩 2 条待办，实得 %d", len(d))
	}

	i163Vote(t, eng, repo, inst.ID, "userB", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDone {
		t.Fatalf("第 2 票达到 #nrOfCompletedInstances>=2 应放行办结（改前此格红：仍停留）: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 0 {
		t.Fatalf("放行后剩余成员待办应废弃，实得 %d 条", len(d))
	}
	rows, _ := repo.FindHistoryTasks(context.Background(), inst.ID)
	abandoned := 0
	for _, r := range rows {
		if r.TaskName == "task1" && r.TaskState == model.TaskStateAbandoned {
			abandoned++
		}
	}
	if abandoned != 1 {
		t.Fatalf("应恰有 1 条成员行被废弃（99），实得 %d", abandoned)
	}
}

// TestI163NoCondKeepsAllFinishedRule ② 对照档：不带 cond ⇒ 两票仍停留、第三票才放行。
func TestI163NoCondKeepsAllFinishedRule(t *testing.T) {
	eng, repo, inst := i163Start(t, "i163-nocond", "")

	i163Vote(t, eng, repo, inst.ID, "userA", nil)
	i163Vote(t, eng, repo, inst.ID, "userB", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDoing {
		t.Fatalf("无 cond 时两票后必须仍停留: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 1 {
		t.Fatalf("无 cond 时两票后应还剩 1 条，实得 %d", len(d))
	}

	i163Vote(t, eng, repo, inst.ID, "userC", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDone {
		t.Fatalf("第三票（全部完成）才放行: state=%d", st)
	}
}

// TestI163OneVoteVetoOrderUnchanged ③ ONE_VOTE_VETO 原样：submitType=20 ⇒ 立即合并。
func TestI163OneVoteVetoOrderUnchanged(t *testing.T) {
	eng, repo, inst := i163Start(t, "i163-veto", "ONE_VOTE_VETO")

	i163Vote(t, eng, repo, inst.ID, "userA", map[string]interface{}{engine.KeySubmitType: 20})
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDone {
		t.Fatalf("一票否决应立即合并: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 0 {
		t.Fatalf("否决后 B/C 待办应废弃，实得 %d 条", len(d))
	}
}

// TestI163NestedFieldReadForm ④ 附带读形对齐：cond 藏在 properties.field 子对象
// （设计器实际落笔处）⇒ 同 ① 判（改前整条 cond 被忽略，两票后仍停留）。
func TestI163NestedFieldReadForm(t *testing.T) {
	eng, repo, inst := i163StartFrom(t, "i163-nested", i163FlowNested("i163-nested", "#nrOfCompletedInstances>=2"))

	i163Vote(t, eng, repo, inst.ID, "userA", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDoing {
		t.Fatalf("第 1 票后应停留: state=%d", st)
	}

	i163Vote(t, eng, repo, inst.ID, "userB", nil)
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDone {
		t.Fatalf("嵌套形 cond 也应被读到并在第 2 票放行: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 0 {
		t.Fatalf("放行后剩余成员待办应废弃，实得 %d 条", len(d))
	}
}
