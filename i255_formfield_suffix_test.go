package jeeflow_test

// issues/255 · 旧 FormFieldAssigneeHandler 的「编号后缀」档（findFieldValue 第三档）
//
// issues/48 定这一档时，Go 侧判据写成 len(m) == 2，而 numberSuffixPattern 有两个捕获组
// ⇒ 命中时 FindStringSubmatch 返回 3 段，这一档从写下起一次都没执行过。本轮改正判据，
// 并把 Rust／MoonBit 补齐成同一形状（八语言三档）。
//
// 第三档**只查裸名 base**；f_<base> 是 issues/254 两枚新处理器（findFormUserFieldValue）的
// 扩面，老类不跟——所以「只有 f_task 时 task_01 必须仍取不到」也是本轮钉住的负面。

import (
	"testing"

	"github.com/mldong/jeeflow-go/engine"
)

func Test255FormFieldNumberSuffixTier(t *testing.T) {
	cases := []struct {
		name   string
		nodeID string
		vars   map[string]interface{}
		want   []string
	}{
		// 正向：task_01 → base task
		{"去后缀命中裸名", "task_01", map[string]interface{}{"task": "userA,userB"}, []string{"userA", "userB"}},
		// 惰性量词的最早可切点：a_b_01 的 base 是 a_b，不是 a
		{"多下划线取最早可切点", "a_b_01", map[string]interface{}{"a_b": "userC"}, []string{"userC"}},
		// 优先级：f_<name> → 裸名 → base 裸名
		{"裸名优先于去后缀", "task_01", map[string]interface{}{"task_01": "userD", "task": "userA"}, []string{"userD"}},
		{"f_前缀优先于两档回落", "task_01", map[string]interface{}{"f_task_01": "userE", "task_01": "userD", "task": "userA"}, []string{"userE"}},
		// 反向：钉住正则的尾锚与「base 至少一字符」
		{"尾巴非纯数字不命中", "task_01x", map[string]interface{}{"task": "userA"}, nil},
		{"base 为空不命中", "_01", map[string]interface{}{"": "userA"}, nil},
		{"base 只有 f_ 形不命中", "task_02", map[string]interface{}{"f_task": "userA"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eng, repo := fuEngine()
			flowName := "i255-" + c.nodeID
			def := registerFlowContent(repo, flowName, fuFlowContent(flowName,
				fuNode{c.nodeID, engine.HandlerFormFieldAssignee}))
			got := fuStartActors(t, eng, repo, def.ID, c.nodeID, c.vars)
			if !fuEq(got, c.want) {
				t.Fatalf("参与者 = %q, want %q", got, c.want)
			}
		})
	}
}
