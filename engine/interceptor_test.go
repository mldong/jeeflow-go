package engine

import (
	"strings"
	"testing"
)

// 兜底语义（issues/104 P2）：单监听器 panic 不传播、不中断后续监听器。
func TestFireEventListenerPanicIsolated(t *testing.T) {
	var called []string
	e := &EngineImpl{ext: &Extensions{Listeners: []ProcessEventListener{
		func(evt ProcessEvent) { panic("boom") },
		func(evt ProcessEvent) { called = append(called, "second") },
	}}}
	e.fireEvent(ProcessEvent{Type: EventProcessStart, InstanceID: 1})
	if len(called) != 1 {
		t.Fatalf("panic 后后续监听器应仍被调用，实际 %v", called)
	}
}

// ─── 规范 11-events §11.3 码表 / §11.5 订阅形状（issues/127＋132 事件代码腿）───

// TestEventTypeCodeTableA A 套整型码钉死：1..9 逐一对齐 spec §11.3，10+ 只占号不发。
// issues/132 §1 的"码值三套并存"就是这一格缺失导致的；写反（5/6 互换、CcCreate 留在 5）
// 必须由本格报红，不是靠集成层猜。
func TestEventTypeCodeTableA(t *testing.T) {
	cases := []struct {
		got  EventType
		code int
		name string
	}{
		{EventProcessInstanceStart, 1, "PROCESS_INSTANCE_START"},
		{EventProcessInstanceEnd, 2, "PROCESS_INSTANCE_END"},
		{EventProcessTaskStart, 3, "PROCESS_TASK_START"},
		{EventCCCreate, 4, "CC_CREATE"},
		{EventTaskComplete, 5, "TASK_COMPLETE"},
		{EventTaskReject, 6, "TASK_REJECT"},
		{EventTaskTransfer, 7, "TASK_TRANSFER"},
		{EventTaskWithdraw, 8, "TASK_WITHDRAW"},
		{EventInstanceTerminated, 9, "INSTANCE_TERMINATED"},
	}
	for _, c := range cases {
		if int(c.got) != c.code {
			t.Errorf("码值错位: %s 应为 %d，实得 %d", c.name, c.code, int(c.got))
		}
		if got := c.got.SpecName(); got != c.name {
			t.Errorf("规范名错位: 码 %d 应为 %s，实得 %s", c.code, c.name, got)
		}
		back, ok := EventTypeBySpecName(c.name)
		if !ok || back != c.got {
			t.Errorf("规范名反查失败: %s → %d/%v", c.name, int(back), ok)
		}
	}
	// 兼容别名必须与 A 套同值（§11.6：CcCreate 5→4、TaskComplete 4→5；
	// Finish/Reject 两条旧实例级位收敛为"实例终态 2"，任务退回另立 6）
	if EventProcessStart != 1 || EventTaskCreate != 3 || EventTaskComplete != 5 || EventCCCreate != 4 ||
		EventProcessFinish != 2 || EventProcessReject != 2 {
		t.Errorf("兼容别名未随 A 套重排: start=%d create=%d complete=%d cc=%d finish=%d reject=%d",
			EventProcessStart, EventTaskCreate, EventTaskComplete, EventCCCreate, EventProcessFinish, EventProcessReject)
	}
	// 号段不外溢：登记表里只允许 1..9（10+ 是"占号不发"，私自发号＝养出第四套码值）
	for code := range specNameByEventType {
		if code < 1 || code > 9 {
			t.Errorf("契约外码值 %d 进了规范名表（本轮只发 1..9）", code)
		}
	}
	// 未登记码不许猜名
	if n := EventType(11).SpecName(); !strings.HasPrefix(n, "UNKNOWN") {
		t.Errorf("未登记码应返回 UNKNOWN，实得 %s", n)
	}
}

// TestProcessEventPayloadSpecKeys §11.3「直传载荷键」的 map 形态：必备键要能在载荷里拿到，
// 未设的键不许凭空出现（监听器按"键在不在"判载荷完整性）。
func TestProcessEventPayloadSpecKeys(t *testing.T) {
	st := 1
	evt := ProcessEvent{Type: EventTaskComplete, InstanceID: 11, TaskID: 22,
		NodeID: "task1", Operator: "u1", SubmitType: &st}
	p := evt.Payload()
	for _, k := range []string{"instanceId", "taskId", "nodeId", "operator", "submitType"} {
		if _, ok := p[k]; !ok {
			t.Errorf("TASK_COMPLETE 载荷缺必备键 %s: %v", k, p)
		}
	}
	// submitType=0（APPLY）是合法值，指针必须把"设为 0"与"未设"分开
	zero := 0
	if v, ok := (ProcessEvent{SubmitType: &zero}).Payload()["submitType"]; !ok || v != 0 {
		t.Errorf("submitType=0 必须出现在载荷里，实得 %v/%v", v, ok)
	}
	// 未设的键不得凭空出现：START 只带 instanceId/operator/nodeId 之外的键都不许有
	pl := (ProcessEvent{Type: EventProcessInstanceStart, InstanceID: 7, Operator: "u"}).Payload()
	for _, k := range []string{"state", "submitType", "actors", "ccActorId", "taskId", "fromActor", "toActor", "reason"} {
		if _, ok := pl[k]; ok {
			t.Errorf("未设的键 %s 不该出现在载荷里: %v", k, pl)
		}
	}
	if pl := (ProcessEvent{Type: EventCCCreate, InstanceID: 7, CcActorID: "cc_a"}).Payload(); pl["ccActorId"] != "cc_a" || pl["instanceId"] != int64(7) {
		t.Errorf("CC_CREATE 载荷必备键缺失/变形: %v", pl)
	}
	if pl := (ProcessEvent{Type: EventTaskTransfer, InstanceID: 7, TaskID: 8, FromActor: "a", ToActor: "b", Operator: "a"}).Payload(); pl["fromActor"] != "a" || pl["toActor"] != "b" {
		t.Errorf("TASK_TRANSFER 载荷必备键缺失: %v", pl)
	}
	// actors 是副本：监听器改它不该污染事件体
	evt2 := ProcessEvent{Type: EventProcessTaskStart, InstanceID: 1, TaskID: 2, Actors: []string{"a"}}
	evt2.Payload()["actors"].([]string)[0] = "hacked"
	if evt2.Actors[0] != "a" {
		t.Errorf("Payload 的 actors 必须是副本，实得 %v", evt2.Actors)
	}
}

// TestFireEventBroadcastsToListenersInRegistrationOrder §11.2 原则 4 / §11.5：
// 一次 fire 送给**全部**监听器，注册顺序＝回调顺序，"后注册覆盖前注册"是缺陷。
func TestFireEventBroadcastsToListenersInRegistrationOrder(t *testing.T) {
	var order []string
	e := &EngineImpl{ext: &Extensions{Listeners: []ProcessEventListener{
		func(evt ProcessEvent) { order = append(order, "first") },
		func(evt ProcessEvent) { order = append(order, "second") },
		func(evt ProcessEvent) { order = append(order, "third") },
	}}}
	e.fireEvent(ProcessEvent{Type: EventCCCreate, InstanceID: 1, CcActorID: "cc"})
	want := []string{"first", "second", "third"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("监听器应按注册顺序全部回调，实得 %v want %v", order, want)
	}
}

// TestFireEventWithoutListenersIsSafe §11.5「零注册时 fire 必须安全返回（不得空指针/panic）」。
func TestFireEventWithoutListenersIsSafe(t *testing.T) {
	(&EngineImpl{}).fireEvent(ProcessEvent{Type: EventTaskTransfer})                // Extensions 未装配
	(&EngineImpl{ext: &Extensions{}}).fireEvent(ProcessEvent{Type: EventCCCreate}) // 装配了但零监听器
}
