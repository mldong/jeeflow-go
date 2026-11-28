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
	// issues/115：新增 7=转办（processTask/transfer 留痕），共 9 档
	if len(items) != 9 {
		t.Fatalf("items = %d, want 9", len(items))
	}
	if items[0].Value != "0" || items[0].Label != "发起申请" {
		t.Fatalf("items[0] = %v", items[0])
	}
	if items[7].Value != "7" || items[7].Label != "转办" {
		t.Fatalf("items[7] = %v, want 7 转办", items[7])
	}
	// spec 07：2=拒绝申请、20=会签拒绝——两项同名会让前端下拉分不开（Go 曾两档都写"拒绝申请"）
	if items[2].Value != "2" || items[2].Label != "拒绝申请" {
		t.Fatalf("items[2] = %v, want 2 拒绝申请", items[2])
	}
	if items[8].Value != "20" || items[8].Label != "会签拒绝" {
		t.Fatalf("items[8] = %v, want 20 会签拒绝", items[8])
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
	// 内置 13 个通用 handler（issues/16 的 7 个 ＋ 表单指定用户两个 ＋ 表单指定部门两个 ＋ 岗位两个）+ 2 个自定义
	if len(assignments) != 15 {
		t.Fatalf("assignments = %d, want 15", len(assignments))
	}
	if assignments[0].ClassName != "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler" ||
		assignments[0].DisplayName != "流程发起人" {
		t.Fatalf("assignments[0] = %v, want 内置 OperatorAssignmentHandler(order=-9999)", assignments[0])
	}
	if assignments[1].ClassName != "com.example.BossHandler" || assignments[1].DisplayName != "老板审批" {
		t.Fatalf("assignments[1] = %v, want order 1 的 BossHandler", assignments[1])
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
	// 构造即内置通用 handler 元数据（issues/16 七个 ＋ 表单指定用户两个 ＋ 表单指定部门两个 ＋ 岗位两个）。
	list := r.ListHandlers("AssignmentHandler")
	want := []struct {
		class, display string
		order          int
	}{
		{"com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler", "流程发起人", -9999},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler", "发起人所属部门经理", 10},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler", "发起人所属部门分管领导", 20},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler", "当前用户所属部门经理", 30},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler", "当前用户所属部门分管领导", 40},
		{"com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler", "根据表单字段值分配参与者", 50},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler", "根据任务节点唯一编码关联角色分配参与者", 60},
		{"com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskPostAssigneeHandler", "根据任务节点唯一编码关联岗位分配参与者", 65},
		{"com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler", "表单指定用户所属部门经理", 70},
		{"com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler", "表单指定用户所属部门分管领导", 80},
		{"com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler", "表单指定部门负责人", 90},
		{"com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler", "表单指定部门分管领导", 100},
		{"com.mldong.jeeflow.interceptor.impl.FormPostAssignmentHandler", "表单指定岗位分配参与者", 110},
	}
	if len(list) != len(want) {
		t.Fatalf("want %d builtin handlers, got %d: %v", len(want), len(list), list)
	}
	for i, w := range want {
		m := list[i]
		if m.ClassName != w.class || m.DisplayName != w.display || m.Order != w.order {
			t.Fatalf("list[%d] = %+v, want {%s %s %d}", i, m, w.class, w.display, w.order)
		}
	}
}

func TestPostAssignmentMetadata259(t *testing.T) {
	list := NewHandlerRegistry().ListHandlers("AssignmentHandler")
	if len(list) != 13 {
		t.Fatalf("issues/259 后内置 AssignmentHandler 应为 13 个，实得 %d", len(list))
	}
	checks := []struct {
		index          int
		class, display string
		order          int
	}{
		{7, "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskPostAssigneeHandler", "根据任务节点唯一编码关联岗位分配参与者", 65},
		{12, "com.mldong.jeeflow.interceptor.impl.FormPostAssignmentHandler", "表单指定岗位分配参与者", 110},
	}
	for _, c := range checks {
		m := list[c.index]
		if m.ClassName != c.class || m.DisplayName != c.display || m.Order != c.order {
			t.Fatalf("list[%d] = %+v, want {%s %s %d}", c.index, m, c.class, c.display, c.order)
		}
	}
}
