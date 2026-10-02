// 参与者解析优先级（issues/100 · Go 栈）。
//
// 基准＝java `CreateTaskHandler.resolveActors`（:84-141）三档顺序：
//   1. tf_nextNodeOperator（非空即 return）
//   2. assignee（token 命中变量则换值、applicant 换发起人）
//   3. assignmentHandler —— **仅当上面没解析出人时**才生效（java 原话 `if (actors.isEmpty())`）
//
// 改前三档顺序是 handler → handler(Extensions) → 动态指派 → assignee，且 handler 命中即无条件
// `return` ⇒ 节点同时配 `assignmentHandler` 与 `assignee`／`tf_nextNodeOperator` 时，后两档被整档吞掉。
// php/python/rust/csharp/moon 五栈都是 java 序，只有 go 与 node 反着 ⇒ 本栈按 java 重排。
//
// ⚠️ 为什么专门补这一份：`RegisterAssignment` 在本栈测试里改前**从未被调用过**（全仓 grep 零命中），
// 也就是这条优先级路径历史上零覆盖 ⇒ 顺序被改错不会有任何一格变红，这正是它能一直错着的原因。
// 断言一律落在**读回的持久参与者行**上（repo.FindTaskActors），不是引擎返回的聚合对象。
package engine_test

import (
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
)

// h100Zhang 一条恒定返回 ["zhang"] 的指派处理器；被调用过与否由调用方靠结果判断。
type h100Zhang struct{}

func (h100Zhang) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return []string{"zhang"}
}

// h100Flow task1 的 properties 由调用方给（同一份夹具覆盖 handler／assignee 的各种组合）。
func h100Flow(name, props string) string {
	return c142Flow(name, c142Node{"task1", model.TypeTask, props})
}

// h100StartWithHandler 起流程并返回 task1 那条行的持久参与者。
func h100StartWithHandler(t *testing.T, key, props string, vars map[string]interface{}) []string {
	t.Helper()
	h := c142Setup(t, key, h100Flow(key, props), true)
	h.reg.RegisterAssignment("h100", h100Zhang{})
	return a142Actors(t, h, vars)
}

const h100Both = `{"performType":0,"assignee":"carol","assignmentHandler":"h100"}`
const h100HandlerOnly = `{"performType":0,"assignmentHandler":"h100"}`
const h100BlankAssignee = `{"performType":0,"assignee":" , , ","assignmentHandler":"h100"}`

// 档 2 压档 3：节点同时配 assignee 与 assignmentHandler ⇒ 参与者必须是 assignee 那个人。
//
// 改前红：handler 排在最前且命中即 return ⇒ 实得 ["zhang"]，carol 整档被吞。
func TestIssue100AssigneeBeatsAssignmentHandler(t *testing.T) {
	got := h100StartWithHandler(t, "i100-asn", h100Both, nil)
	if want := []string{"carol"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("java 序下 assignee 应压过 assignmentHandler，实得 %q want %q", got, want)
	}
}

// 档 1 压档 3：带 tf_nextNodeOperator 时，即使节点挂了 handler 也必须用动态指派的人。
//
// 改前红：同上一格，实得 ["zhang"]。
func TestIssue100NextNodeOperatorBeatsAssignmentHandler(t *testing.T) {
	got := h100StartWithHandler(t, "i100-next", h100HandlerOnly,
		map[string]interface{}{engine.KeyNextNodeOperator: "dave"})
	if want := []string{"dave"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("java 序下 tf_nextNodeOperator 应压过 assignmentHandler，实得 %q want %q", got, want)
	}
}

// 回归档：节点**只**挂 handler（没 assignee、没动态指派）⇒ handler 仍是扩展点，必须照常生效。
// 这一格是给"重排"本身把守的：把 handler 挪到最后不等于取消它。
func TestIssue100HandlerStillUsedWhenNothingElse(t *testing.T) {
	got := h100StartWithHandler(t, "i100-only", h100HandlerOnly, nil)
	if want := []string{"zhang"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("只挂 handler 时它必须生效（重排不得把扩展点改没），实得 %q want %q", got, want)
	}
}

// 与 java `if (actors.isEmpty())` 逐字对齐的一档：assignee 存在但**解析后为空**（" , , "）
// ⇒ 不算命中，继续往下落到 handler。改前这格是"handler 先赢"，重排后若把 assignee
// 分支写成"只要有 assignee 键就 return"，这格会红成 [] ⇒ 本格专门拦这种"改一半"。
func TestIssue100BlankAssigneeFallsBackToHandler(t *testing.T) {
	got := h100StartWithHandler(t, "i100-blank", h100BlankAssignee, nil)
	if want := []string{"zhang"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("全空白 assignee 不得算命中，应回落 handler，实得 %q want %q", got, want)
	}
}
