// issues/142 A 批（go 腿）· 任务类节点**零参与者必须建单**。
//
// 判据＝spec 02-flow-definition.md §6.1 表第一行 ＋ §6.2 第 3 条（owner 2026-09-30 拍）：
//
//	任务类（task/approval，含会签）参与者为空时的正确形状＝**建待办行（task_state=10），
//	参与者可以为零**（java `CreateTaskHandler.java:38-63` 无条件建单）；
//	禁止的形状＝"因为没人可办就跳过建行" ⇒ 实例停在 state=10 却零可办行（**死锁黑洞**）。
//	普查实读本栈正是那一侧：`engine/engine_impl.go:716-719` `if len(actors) == 0 { return nil }`。
//
// 本文件除了钉新形状，还专门钉"实例状态机不被推歪"那半件事（owner 在派工里点名要确认）：
//   - 零参与者行**不自增出新行** ⇒ 门面 startAndExecute 那圈自动办结不会死循环
//     （facade/zero_actor_142_test.go 那格直接跑门面）；
//   - 它是正常 DOING 行 ⇒ join／并行会签的"还有 doing 就没到齐"判据照旧成立；
//   - 它可被"加派参与者"或 flow.auto/flow.admin 办动 ⇒ 黑洞真的被关掉了；
//   - 落库形状里 `wf_process_task_actor` **一行都不插**（不是插一条 actor_id='' 的行——
//     那是 issues/129／141 B 那族空归属值，§6.2 之外的事，但这一改顺手就会踩，故钉死）。
package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
)

// z142ZeroActorFlow start→task1（**没有任何 assignee／assignmentHandler** ⇒ 参与者必为空）→end。
// 夹具要点：properties 只给 performType，不给 assignee／candidateUsers／tf_nextNodeOperator，
// 保证 resolveActors 走到底返回 nil（旧形状在这里 `return nil` 一行都不建）。
func z142Flow(name, taskProps string) string {
	return c142Flow(name, c142Node{"task1", model.TypeTask, taskProps})
}

// TestIssue142TaskNodeZeroActorCreatesDoingRow 主格：零参与者 ⇒ 恰好一条 task_state=10 的行、
// 参与者为空、实例仍 state=10、码 3 照发（java 的 notifyTaskStart 对每条 saveNewTask 都播）。
//
// 改前必红（还原成 `if len(actors) == 0 { return nil }`）：
// 「恰好一条 DOING 行」红成 0 行——这正是 §6.1 点名的死锁黑洞形状。
func TestIssue142TaskNodeZeroActorCreatesDoingRow(t *testing.T) {
	h := c142Setup(t, "z142-plain", z142Flow("z142-plain", `{"performType":0}`), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing := c142Doing(t, h.repo, inst.ID)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("零参与者也应建恰好一条 DOING 行（§6.2 第 3 条），实得 %v", c142Names(doing))
	}
	row := doing[0]
	if row.TaskState != model.TaskStateDoing {
		t.Fatalf("行状态应为 10（DOING），实得 %d", row.TaskState)
	}
	if len(row.ActorIDs) != 0 {
		t.Fatalf("行参与者应为空集，实得 %v", row.ActorIDs)
	}
	actors, _ := h.repo.FindTaskActors(context.Background(), row.ID)
	if len(actors) != 0 {
		t.Fatalf("wf_process_task_actor 一行都不该插（空归属值是 issues/129 病根），实得 %v", actors)
	}
	for _, a := range actors {
		if strings.TrimSpace(a) == "" {
			t.Fatalf("参与者集合里不许出现空串：%q", actors)
		}
	}
	if inst.State != model.InstanceStateDoing {
		t.Fatalf("实例应停在 state=10（这一格确实还没人办），实得 %d", inst.State)
	}
	// 建单不变量照建（与参与者多少无关）
	if row.ParentTaskID == nil || *row.ParentTaskID != 0 {
		t.Fatalf("发起路径 parent 应落 0，实得 %v", row.ParentTaskID)
	}
	if row.Variables[model.IsFirstTaskNodeKey] != true {
		t.Fatalf("task1 是 start 直接后继 ⇒ 标记应为 true，实得 %v", row.Variables)
	}
	// 码 3 照发（事实是"这一格产生了待办行"），载荷 actors 为空集
	want := []string{"PROCESS_INSTANCE_START:", "PROCESS_TASK_START:task1"}
	if got := *h.events; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("事件序列不符，\n want %v\n  got %v", want, got)
	}
}

// TestIssue142ZeroActorRowRescuedByAddingActor 这一行**办得动**：加派参与者后由那人办结、
// 令牌继续走到 end ⇒ 黑洞真的被关掉（不是"建了一条永远办不动的死行"）。
func TestIssue142ZeroActorRowRescuedByAddingActor(t *testing.T) {
	h := c142Setup(t, "z142-add", z142Flow("z142-add", `{"performType":0}`), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	row := c142Doing(t, h.repo, inst.ID)[0]
	// 加派之前：非参与的人办不动（这条行不是"谁都能办"的开放行）
	if _, err := h.eng.ExecuteProcessTask(context.Background(), row.ID, "someoneElse", nil); err == nil {
		t.Fatal("未加派参与者时局外人应被拒（零参与者行 ≠ 人人可办）")
	}
	if err := h.repo.AddTaskActor(context.Background(), row.ID, []string{"leader01"}); err != nil {
		t.Fatalf("加派参与者: %v", err)
	}
	if _, err := h.eng.ExecuteProcessTask(context.Background(), row.ID, "leader01",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("加派后应办得动: %v", err)
	}
	after, err := h.repo.FindInstanceByID(context.Background(), inst.ID)
	if err != nil || after == nil {
		t.Fatalf("读回实例: %v", err)
	}
	if after.State != model.InstanceStateDone {
		t.Fatalf("办结后实例应到 end（state=20），实得 %d", after.State)
	}
	if n := len(c142Doing(t, h.repo, inst.ID)); n != 0 {
		t.Fatalf("办结后不该留待办，实得 %d", n)
	}
}

// TestIssue142ZeroActorRowExecutableBySystemOperator flow.auto／flow.admin 系统代执行放行
// （v1.0.1 既有契约）——零参与者行同样能被系统办动。
func TestIssue142ZeroActorRowExecutableBySystemOperator(t *testing.T) {
	for _, sys := range []string{engine.KeyAutoExecute, engine.KeyAdminID} {
		h := c142Setup(t, "z142-sys-"+strings.ReplaceAll(sys, ".", "-"),
			z142Flow("z142-sys-"+sys, `{"performType":0}`), true)
		inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
		if err != nil {
			t.Fatalf("%s start: %v", sys, err)
		}
		row := c142Doing(t, h.repo, inst.ID)[0]
		if _, err := h.eng.ExecuteProcessTask(context.Background(), row.ID, sys,
			map[string]interface{}{"submitType": 1}); err != nil {
			t.Fatalf("%s 应能办零参与者行: %v", sys, err)
		}
		after, _ := h.repo.FindInstanceByID(context.Background(), inst.ID)
		if after == nil || after.State != model.InstanceStateDone {
			t.Fatalf("%s 办结后实例应到 end，实得 %+v", sys, after)
		}
	}
}

// TestIssue142ZeroActorCountersignNoPanic 会签节点（performType=1 ＋ SEQUENTIAL）解析到零参与者：
// 旧代码若只删守卫、不理会签分支，`actors[0]` 会 panic；全员逐人建单那条路也建不出行（还是黑洞）。
// 新形状＝一条 perform_type=1 的零参与者 DOING 行。
//
// 注：java 侧 `createCountersignTasks` 的 SEQUENTIAL 分支在空集合上是 `actorIds.get(0)`
// 直接 IndexOutOfBounds、PARALLEL 分支是循环 0 次＝零行（两个都是黑洞/崩），
// 本栈按 owner 裁定的"必须建一行"收口，这一处**与基准分歧**已写进交付报告。
func TestIssue142ZeroActorCountersignNoPanic(t *testing.T) {
	for _, ct := range []string{"SEQUENTIAL", "PARALLEL", "UNKNOWN_TYPE"} {
		props := fmt.Sprintf(`{"performType":"1","countersignType":%q}`, ct)
		h := c142Setup(t, "z142-cs-"+ct, z142Flow("z142-cs-"+ct, props), true)
		inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
		if err != nil {
			t.Fatalf("countersignType=%s start: %v", ct, err)
		}
		doing := c142Doing(t, h.repo, inst.ID)
		if len(doing) != 1 {
			t.Fatalf("countersignType=%s 零参与者也应建恰好一行，实得 %v", ct, c142Names(doing))
		}
		if doing[0].PerformType != 1 {
			t.Fatalf("countersignType=%s 那行应落 perform_type=1（会签任务，issues/57 E29）", ct)
		}
		if actors, _ := h.repo.FindTaskActors(context.Background(), doing[0].ID); len(actors) != 0 {
			t.Fatalf("countersignType=%s 不该插 actor 行，实得 %v", ct, actors)
		}
	}
}

// TestIssue142ZeroActorRowHoldsJoinUntilExecuted 状态机不被推歪（owner 点名要确认的那半件事）：
// fork → [taskA 零参与者 | taskB=leader] → join → end。
// 新形状下 taskA 是一条**真实 DOING 行** ⇒ join 的"还有 doing 就没到齐"判据把它算进去，
// 令牌不会越过这一格提前办结；办掉它（flow.auto）之后才到 end。
//
// 改前形状：taskA 一行不建 ⇒ 办完 taskB 后 doing 为空 ⇒ join 直接放行 ⇒
// 实例被推成 state=20，而那一格其实谁也没办过（§6.1 说的"停在 state=10 却零可办行"
// 在这里的另一面是"根本停不住"）。
func TestIssue142ZeroActorRowHoldsJoinUntilExecuted(t *testing.T) {
	const content = `{"name":"z142-join","displayName":"零参与者与 join","type":"approval","nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"fork1","type":"snaker:fork","properties":{},"text":{"value":"并行开始"}},
	  {"id":"taskA","type":"snaker:task","properties":{"performType":0},"text":{"value":"零参与者那一格"}},
	  {"id":"taskB","type":"snaker:task","properties":{"assignee":"leader","performType":0},"text":{"value":"有人那一格"}},
	  {"id":"join1","type":"snaker:join","properties":{},"text":{"value":"并行结束"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e1","sourceNodeId":"start","targetNodeId":"fork1","properties":{}},
	  {"id":"e2","sourceNodeId":"fork1","targetNodeId":"taskA","properties":{}},
	  {"id":"e3","sourceNodeId":"fork1","targetNodeId":"taskB","properties":{}},
	  {"id":"e4","sourceNodeId":"taskA","targetNodeId":"join1","properties":{}},
	  {"id":"e5","sourceNodeId":"taskB","targetNodeId":"join1","properties":{}},
	  {"id":"e6","sourceNodeId":"join1","targetNodeId":"end","properties":{}}]}`
	h := c142Setup(t, "z142-join", content, true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing := c142Doing(t, h.repo, inst.ID)
	if len(doing) != 2 {
		t.Fatalf("fork 两支都该有行（含零参与者那一支），实得 %v", c142Names(doing))
	}
	var rowA, rowB *model.ProcessTask
	for _, d := range doing {
		switch d.TaskName {
		case "taskA":
			rowA = d
		case "taskB":
			rowB = d
		}
	}
	if rowA == nil || rowB == nil {
		t.Fatalf("夹具失效：taskA/taskB 应各一行，实得 %v", c142Names(doing))
	}
	if _, err := h.eng.ExecuteProcessTask(context.Background(), rowB.ID, "leader",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("办 taskB: %v", err)
	}
	// join 不放行：零参与者那一行还活着 ⇒ 实例不许被推成办结
	after, _ := h.repo.FindInstanceByID(context.Background(), inst.ID)
	if after.State != model.InstanceStateDoing {
		t.Fatalf("taskA（零参与者）未办结 ⇒ join 不该放行，实例 state=%d", after.State)
	}
	if n := len(c142Doing(t, h.repo, inst.ID)); n != 1 {
		t.Fatalf("应只剩 taskA 那一条 DOING，实得 %d", n)
	}
	// 系统代执行办掉它 ⇒ join 齐了才到 end
	if _, err := h.eng.ExecuteProcessTask(context.Background(), rowA.ID, engine.KeyAutoExecute,
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("flow.auto 办零参与者行: %v", err)
	}
	after, _ = h.repo.FindInstanceByID(context.Background(), inst.ID)
	if after.State != model.InstanceStateDone {
		t.Fatalf("两支都办结后应到 end，实得 state=%d", after.State)
	}
}
