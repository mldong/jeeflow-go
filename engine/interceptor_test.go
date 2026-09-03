package engine

import "testing"

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
