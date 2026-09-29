// 归属值写侧归一（issues/142 B 批 · Go 栈，**漏斗层** · 引擎参与者解析一路）。
//
// spec 06-facade.md §2.11 写点表第 3 行：`f_nextNodeOperator`／`tf_nextNodeOperator`
// **逗号串与数组两形同判据**，「数组元素不得被静默丢弃或串化成类型名」。普查实读本栈
// `engine/engine_impl.go` 的 valueToActors 改前是"数组里的 null 变 \"<nil>\"、两支都不 trim"
// （issues/142 §2 B 表 go 那一行），而逗号串那一支才 trim＋丢空——**同一支函数两把尺子**。
//
// 判据单点＝[spi.NormalizeActors]（与门面 toStringSlice2、两仓 AddTaskActor 同一枚）；
// 本文件只经**公开 API**（StartProcessInstanceByID 的 vars）驱动，不碰未导出的 valueToActors。
// 断言落在**读回的持久行**（repo.FindTaskActors）上，不是引擎返回的聚合对象。
package engine_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
)

// a142PlainTaskFlow start→task1（无 assignee／无 assignmentHandler ⇒ 参与者**只可能**
// 来自 tf_nextNodeOperator 这一支）→end。
func a142PlainTaskFlow(name string) string {
	return c142Flow(name, c142Node{"task1", model.TypeTask, `{"performType":0}`})
}

// a142AssigneeVarFlow task1 的 assignee 是**变量名** dyn ⇒ resolveActors 第 3 步命中变量后
// 走 valueToActors(val, split=false)（assignee 命中语义，与 nextNodeOperator 同一支函数）。
func a142AssigneeVarFlow(name string) string {
	return c142Flow(name, c142Node{"task1", model.TypeTask, `{"performType":0,"assignee":"dyn"}`})
}

// a142Actors 发起后 task1 那条行的参与者（从仓储读回）。
func a142Actors(t *testing.T, h *c142Harness, vars map[string]interface{}) []string {
	t.Helper()
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", vars)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing := c142Doing(t, h.repo, inst.ID)
	if len(doing) != 1 {
		t.Fatalf("夹具：应恰好一条 DOING 行，实得 %v", c142Names(doing))
	}
	actors, err := h.repo.FindTaskActors(context.Background(), doing[0].ID)
	if err != nil {
		t.Fatalf("读参与者: %v", err)
	}
	return actors
}

// TestIssue142EngineNextNodeOperatorArrayFormNormalized 数组形：逐元素 trim、空串/纯空白/nil
// 丢弃、数字元素字符串化后**仍走同一枚归一**、同次调用折叠。
//
// 改前红：[]interface{} 那一支不 trim 也不丢空串，nil 被 fmt.Sprintf 成字面量 "<nil>" ⇒
// 实得 [" x " "" "<nil>" "123" "x"]（" x " 与 "x" 还判成两个人）。
func TestIssue142EngineNextNodeOperatorArrayFormNormalized(t *testing.T) {
	h := c142Setup(t, "a142-arr", a142PlainTaskFlow("a142-arr"), false)
	got := a142Actors(t, h, map[string]interface{}{
		engine.KeyNextNodeOperator: []interface{}{" x ", "", "   ", nil, 123, "x"},
	})
	want := []string{"x", "123"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11 数组形应与串形同判据，实得 %q want %q", got, want)
	}
}

// TestIssue142EngineNextNodeOperatorTwoFormsSameAnswer 两形同判据的**正面对拍**：
// 同一批人分别用逗号串与数组给，落到参与者表上必须是同一份值。
//
// 改前红：数组形那侧红（同上一格）；串形那侧本就正确 ⇒ 本格把"别只修一条腿"钉成一次比对。
func TestIssue142EngineNextNodeOperatorTwoFormsSameAnswer(t *testing.T) {
	fromString := a142Actors(t, c142Setup(t, "a142-t2s", a142PlainTaskFlow("a142-t2s"), false),
		map[string]interface{}{engine.KeyNextNodeOperator: " x , , 123, x"})
	fromArray := a142Actors(t, c142Setup(t, "a142-t2a", a142PlainTaskFlow("a142-t2a"), false),
		map[string]interface{}{engine.KeyNextNodeOperator: []interface{}{" x ", "", nil, 123, "x"}})
	if !reflect.DeepEqual(fromString, fromArray) {
		t.Fatalf("§2.11「两形同判据」分叉：串形 %q vs 数组形 %q", fromString, fromArray)
	}
	if want := []string{"x", "123"}; !reflect.DeepEqual(fromString, want) {
		t.Fatalf("两形都该是 %q，实得串形 %q", want, fromString)
	}
}

// TestIssue142EngineNilNextNodeOperatorCreatesNoActorRow 整个入参是 nil（键在、值为 null，
// JSON body 的真实形状）⇒ 零参与者，**不得**串化成 "<nil>" 落进归属列。
// 行照建（issues/142 A 批「任务类零参与者必须建单」），参与者表零行。
//
// 改前红：default 分支 fmt.Sprintf("%v", nil) ⇒ 参与者 ["<nil>"]。
func TestIssue142EngineNilNextNodeOperatorCreatesNoActorRow(t *testing.T) {
	h := c142Setup(t, "a142-nil", a142PlainTaskFlow("a142-nil"), false)
	if got := a142Actors(t, h, map[string]interface{}{engine.KeyNextNodeOperator: nil}); len(got) != 0 {
		t.Fatalf("§2.11：nil 入参应与空集同档，不得变 \"<nil>\" 归属值，实得 %q", got)
	}
}

// TestIssue142EngineNextNodeOperatorKeepsSentinelIds 反向哨兵（引擎漏斗）："0"/"00"/"a"
// 是三个不同的人，只有纯空白与 nil 被丢。
//
// 改前红：" " 照落一行（不 trim ⇒ 判不出它是空值），"<nil>" 又多一行。
func TestIssue142EngineNextNodeOperatorKeepsSentinelIds(t *testing.T) {
	h := c142Setup(t, "a142-sent", a142PlainTaskFlow("a142-sent"), false)
	got := a142Actors(t, h, map[string]interface{}{
		engine.KeyNextNodeOperator: []interface{}{"0", "00", " ", nil, "a"},
	})
	want := []string{"0", "00", "a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11 哨兵：\"0\"/\"00\"/\"a\" 都得留下且互不折叠，实得 %q want %q", got, want)
	}
}

// TestIssue142EngineAssigneeVarArrayNormalized assignee 命中变量那一支（split=false）同样过
// 归一：数组元素 trim、空串/nil 丢弃。
//
// 改前红：[]string 一支整个原样照收 ⇒ 实得 [" y1 " "" "y2"]。
func TestIssue142EngineAssigneeVarArrayNormalized(t *testing.T) {
	h := c142Setup(t, "a142-dyn", a142AssigneeVarFlow("a142-dyn"), false)
	got := a142Actors(t, h, map[string]interface{}{"dyn": []string{" y1 ", "", "   ", "y2", "y1"}})
	want := []string{"y1", "y2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11：assignee 命中变量与 nextNodeOperator 同一枚判据，实得 %q want %q", got, want)
	}
}
