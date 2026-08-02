package metadata

import (
	"testing"
)

// ─── 枚举字典 ────────────────────────────────────────────────────────────────

func TestEnumDictKeys(t *testing.T) {
	keys := EnumDictKeys()
	want := []string{"wf_countersign_type", "wf_process_define_state", "wf_process_instance_state",
		"wf_process_submit_type", "wf_process_task_perform_type", "wf_process_task_state", "wf_process_task_type"}
	if len(keys) != 7 {
		t.Fatalf("keys = %v, want 7 keys", keys)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Fatalf("keys[%d] = %s, want %s", i, keys[i], k)
		}
	}
}

func TestInstanceStateDict(t *testing.T) {
	items := EnumDict("wf_process_instance_state")
	if len(items) != 7 {
		t.Fatalf("items = %d, want 7", len(items))
	}
	if items[0].Value != "10" || items[0].Label != "进行中" {
		t.Fatalf("items[0] = %v", items[0])
	}
	if items[4].Value != "45" || items[4].Label != "已拒绝" {
		t.Fatalf("items[4] = %v", items[4])
	}
	if items[6].Value != "99" || items[6].Label != "已废弃" {
		t.Fatalf("items[6] = %v", items[6])
	}
}

func TestSubmitTypeDict(t *testing.T) {
	items := EnumDict("wf_process_submit_type")
	if len(items) != 8 {
		t.Fatalf("items = %d, want 8", len(items))
	}
	if items[0].Value != "0" || items[0].Label != "发起申请" {
		t.Fatalf("items[0] = %v", items[0])
	}
	if items[7].Value != "20" || items[7].Label != "拒绝申请" {
		t.Fatalf("items[7] = %v", items[7])
	}
}

func TestUnknownKeyReturnsEmpty(t *testing.T) {
	if items := EnumDict("wf_no_such_dict"); len(items) != 0 {
		t.Fatalf("unknown key should return empty, got %v", items)
	}
}

// ─── SPI 实现清单 ────────────────────────────────────────────────────────────

func TestHandlerRegistry(t *testing.T) {
	r := NewHandlerRegistry()
	r.Register(HandlerMeta{Type: "AssignmentHandler", ClassName: "com.example.DeptLeaderHandler", DisplayName: "部门领导审批", Order: 2})
	r.Register(HandlerMeta{Type: "AssignmentHandler", ClassName: "com.example.BossHandler", DisplayName: "老板审批", Order: 1})
	r.Register(HandlerMeta{Type: "FlowInterceptor", ClassName: "com.example.TimeInterceptor", DisplayName: "耗时记录", Order: 0, Group: "post"})
	r.Register(HandlerMeta{Type: "FlowInterceptor", ClassName: "com.example.LogInterceptor", DisplayName: "日志记录", Order: 1, Group: "pre"})

	assignments := r.ListHandlers("AssignmentHandler")
	if len(assignments) != 2 {
		t.Fatalf("assignments = %d, want 2", len(assignments))
	}
	if assignments[0].ClassName != "com.example.BossHandler" || assignments[0].DisplayName != "老板审批" {
		t.Fatalf("assignments[0] = %v, want order 1 的 BossHandler", assignments[0])
	}

	pre := r.ListHandlersGroup("FlowInterceptor", "pre")
	if len(pre) != 1 || pre[0].ClassName != "com.example.LogInterceptor" {
		t.Fatalf("pre = %v", pre)
	}
	post := r.ListHandlersGroup("FlowInterceptor", "post")
	if len(post) != 1 || post[0].ClassName != "com.example.TimeInterceptor" {
		t.Fatalf("post = %v", post)
	}
	if group := r.ListHandlersGroup("FlowInterceptor", "unknown"); len(group) != 0 {
		t.Fatalf("unknown group should be empty, got %v", group)
	}
}

func TestEmptyRegistry(t *testing.T) {
	r := NewHandlerRegistry()
	if list := r.ListHandlers("AssignmentHandler"); len(list) != 0 {
		t.Fatalf("empty registry should return empty, got %v", list)
	}
	if types := r.ListHandlerTypes(); len(types) != 0 {
		t.Fatalf("empty types, got %v", types)
	}
}
