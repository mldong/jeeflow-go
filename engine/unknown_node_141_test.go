// issues/141 G4 · spec/02-flow-definition.md「类型键的三条义务」第 2 条（go 腿的日志欠账，批二补）。
//
// 判据原文（jeeflow-doc/docs/spec/02-flow-definition.md:113-124）：
//
//	「未知档不得静默丢节点：类型不在表里时，必须**记一条可诊断日志（带节点 id 与实得类型串）**
//	 再决定跳过，不允许『静默丢节点＋连带丢它的出边』」
//	……「唯一欠账＝go/python/node 三栈未知档至今零条日志」。
//
// 本文件只管**记日志**那一半，不引入新的落穿语义：义务 2 的后半（指向被丢弃节点的那条边
// 「落穿停住、严禁打崩办理」）由 issues/143 在 java/php/c# 落地，go 属"按 id 现查目标、
// 查不到就停"那一派（engine_impl.go 的 followEdges），行为本来就对 ⇒ 第 ① 格顺带钉住
// 「行为不因日志而变」（无 err、实例停在 DOING、被跳过的节点不产生任何行）。
//
// 夹具复用 issues/142 那一套（同包 custom_node_142_test.go 的 c142Flow / c142Setup /
// c142CaptureLog / c142Doing / c142Rows / c142Names）。日志捕获方式＝本仓既有先例
// c142CaptureLog（custom_node_142_test.go:147-154，log.Writer() + log.SetOutput(&bytes.Buffer{})
// + t.Cleanup 还原），引擎侧那条 WARNING 走的就是标准库 log。
package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/model"
)

// unknownWarnMarker 未知档那句 WARNING 的指纹。反向哨兵也用它 ⇒ "无条件打印"那种改动
// 一定把已知类型的流转也打上这句，从而红在哨兵格。
const unknownWarnMarker = "不在类型表里"

// ─── ① 未知档：日志真发出来，且带 nodeId ＋ 实得类型串原文 ────────────────────

// TestIssue141G4UnknownNodeTypeWarns 四种"不在表里"的实得类型串（子流程驼峰串／裸名／
// 大小写拼错／缺 type 键），各自必须记**恰好一条**可诊断 WARNING，内容同时含
// 节点 id 与**逐字原文**；且流转行为不因这条日志而变（不打断、不建行、不越过）。
//
// 变异对照第 1 组（把日志行注释掉）当场红在这一格——那正是"零条日志"的病灶本身。
func TestIssue141G4UnknownNodeTypeWarns(t *testing.T) {
	cases := []struct {
		desc   string
		nodeID string
		rawTyp string
	}{
		// 设计器实际输出的子流程串：spec/02:107-111 owner 2026-10-01 二拍「子流程暂不进契约面」
		// ⇒ go 六栈按未知档暴露，这条日志就是它现状唯一的可诊断面。
		{"subProcess驼峰串（go 无此档）", "sub1", "snaker:subProcess"},
		// 裸名：本栈类型表只有 model/types.go:40-46 那七档 snaker:*，没有裸名档。
		{"裸名 task（go 不剥前缀查表）", "bare", "task"},
		// 大小写拼错：义务 1 的归一化未落地 ⇒ 仍是未知档，原文要打得出来才可诊断。
		{"大小写拼错 snaker:Custom", "typo1", "snaker:Custom"},
		{"缺 type 键（实得空串）", "notype", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			logs := c142CaptureLog(t)
			// start → <未知档节点> → end：令牌撞上它就停住，出边（含通往 end 那条）都不走。
			h := c142Setup(t, "g4-unknown", c142Flow("g4-unknown",
				c142Node{c.nodeID, c.rawTyp, `{}`}), true)

			inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
			if err != nil {
				t.Fatalf("记日志不是报错：未知档不该打断建单，实得 err=%v", err)
			}
			got := logs.String()

			// 1) 日志真发出来了（不是"跑通就算"）。
			if !strings.Contains(got, unknownWarnMarker) {
				t.Fatalf("类型不在表里应记一条可诊断 WARNING，实得日志：%q", got)
			}
			// 2) 两个必需要素：节点 id ＋ 实得类型串**原文**（逐字相邻 ⇒ 顺带钉住 kv 形态）。
			if want := "nodeId=" + c.nodeID + " type=" + c.rawTyp; !strings.Contains(got, want) {
				t.Fatalf("日志要带 nodeId＋实得类型串原文（期望含 %q），实得日志：%q", want, got)
			}
			// 3) 后果说明＋前缀/级别词按本栈惯例（engine_impl.go:875-883 那三条样板同形）。
			for _, need := range []string{"[jeeflow] WARNING", "该节点及其出边将被跳过"} {
				if !strings.Contains(got, need) {
					t.Fatalf("日志缺 %q，实得日志：%q", need, got)
				}
			}
			// 4) 不刷屏：一次触达只记一条。
			if n := strings.Count(got, unknownWarnMarker); n != 1 {
				t.Fatalf("同一节点应恰好记一条，实得 %d 条：%q", n, got)
			}

			// 5) 行为零变化（本条义务只补日志，落穿那一半 issues/143 已定 go 本来就对）：
			//    实例停在 DOING，被跳过的节点及其下游都不产生任何任务行。
			if inst.State != model.InstanceStateDoing {
				t.Fatalf("未知档不越过：实例应停在 DOING(10)，实得 state=%d", inst.State)
			}
			if rows := c142Rows(t, h.repo, inst.ID); len(rows) != 0 {
				t.Fatalf("被跳过的未知档节点不该产生任何行，实得 %v", c142Names(rows))
			}
			if doing := c142Doing(t, h.repo, inst.ID); len(doing) != 0 {
				t.Fatalf("未知档不建待办，实得 %v", c142Names(doing))
			}
		})
	}
}

// ─── ② 反向哨兵：已知类型不该出现这条 WARNING ────────────────────────────────

// TestIssue141G4KnownNodeTypesDoNotWarn 钉住"这条日志是**有条件**发的"：
// 解析并跑一条只含已知类型（snaker:task / snaker:end / snaker:start）的正常流，
// 捕获缓冲区里**不该**出现未知档 WARNING。
//
// 变异对照第 2 组（把日志改成无条件打印、不分已知/未知）当场红在这一格。
func TestIssue141G4KnownNodeTypesDoNotWarn(t *testing.T) {
	logs := c142CaptureLog(t)
	h := c142Setup(t, "g4-known", c142Flow("g4-known",
		c142Node{"task1", model.TypeTask, `{"assignee":"leader","performType":0}`}), true)

	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing := c142Doing(t, h.repo, inst.ID)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("已知类型链的待办应只有 task1（夹具本身要跑起来，否则哨兵是空转），实得 %v", c142Names(doing))
	}
	if _, err := h.eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("execute task1: %v", err)
	}
	// 读回持久实例（不是引擎返回的聚合对象）：链路真的走过了 snaker:end ⇒ 哨兵不是空转。
	gotInst, err := h.repo.FindInstanceByID(context.Background(), inst.ID)
	if err != nil || gotInst == nil {
		t.Fatalf("读回实例 %d: %v", inst.ID, err)
	}
	if gotInst.State != model.InstanceStateDone {
		t.Fatalf("已知类型链应走到 end（夹具要跑完整条链，否则哨兵空转），实得 state=%d", gotInst.State)
	}

	got := logs.String()
	if strings.Contains(got, unknownWarnMarker) || strings.Contains(got, "该节点及其出边将被跳过") {
		t.Fatalf("已知类型（snaker:task / snaker:end）不该触发未知档 WARNING，实得日志：%q", got)
	}
}
