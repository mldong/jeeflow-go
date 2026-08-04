// Package metadata 引擎元数据能力（v1.4.0，issues/04）
//
// 两类元数据：
//  1. EnumDictKeys/EnumDict —— 内置状态枚举字典（key 对齐 boot3：wf_process_define_state 等），
//     枚举 code/label 直接来自 model 包常量，杜绝集成方重复定义导致的值漂移；
//  2. HandlerRegistry —— SPI 实现清单（AssignmentHandler/CandidateHandler/FlowInterceptor），
//     集成方显式注册可用实现（handlerName + 显示名/排序/分组），作为前端设计器字典源，
//     与运行时引擎加载的 handlerName 天然一致。
package metadata

import "sort"

// ─── 枚举字典 ────────────────────────────────────────────────────────────────

// DictItem 单个字典项
type DictItem struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

// dictEntry 枚举字典条目（key + 生成函数）
type dictEntry struct {
	key    string
	values []DictItem
}

// EnumDictKeys 内置枚举字典 key 清单（对齐 boot3 字典 key，存量前端零改动）
func EnumDictKeys() []string {
	keys := make([]string, 0, len(dicts))
	for _, e := range dicts {
		keys = append(keys, e.key)
	}
	sort.Strings(keys)
	return keys
}

// EnumDict 按 key 取字典（[{value, label}]），未知 key 返回空列表
func EnumDict(key string) []DictItem {
	for _, e := range dicts {
		if e.key == key {
			return e.values
		}
	}
	return []DictItem{}
}

// dictOf 由「值→标签」生成字典项
func dictOf(pairs map[string]string, order []string) []DictItem {
	items := make([]DictItem, 0, len(order))
	for _, v := range order {
		items = append(items, DictItem{Value: v, Label: pairs[v]})
	}
	return items
}

// 内置字典表（值顺序与 Java enums 声明顺序一致）
var dicts = []dictEntry{
	{
		key: "wf_process_define_state",
		values: dictOf(map[string]string{"0": "禁用", "1": "启用"}, []string{"0", "1"}),
	},
	{
		key: "wf_process_instance_state",
		values: dictOf(map[string]string{
			"10": "进行中", "20": "已完成", "30": "已撤回", "40": "强行终止",
			"45": "已拒绝", "50": "挂起", "99": "已废弃",
		}, []string{"10", "20", "30", "40", "45", "50", "99"}),
	},
	{
		key: "wf_process_submit_type",
		values: dictOf(map[string]string{
			"0": "发起申请", "1": "同意申请", "2": "拒绝申请", "3": "退回上一步",
			"4": "跳转", "5": "重新提交", "6": "退回发起人", "20": "拒绝申请",
		}, []string{"0", "1", "2", "3", "4", "5", "6", "20"}),
	},
	{
		key: "wf_process_task_state",
		values: dictOf(map[string]string{
			"10": "进行中", "20": "已完成", "30": "已撤回", "40": "强行终止",
			"50": "挂起", "99": "已废弃",
		}, []string{"10", "20", "30", "40", "50", "99"}),
	},
	{
		key: "wf_process_task_type",
		values: dictOf(map[string]string{"0": "主办", "1": "协办", "2": "记录"}, []string{"0", "1", "2"}),
	},
	{
		key: "wf_process_task_perform_type",
		values: dictOf(map[string]string{"0": "普通参与", "1": "会签参与"}, []string{"0", "1"}),
	},
	{
		key: "wf_countersign_type",
		values: dictOf(map[string]string{"0": "并行会签", "1": "串行会签"}, []string{"0", "1"}),
	},
}

// ─── SPI 实现清单 ────────────────────────────────────────────────────────────

// HandlerMeta 处理器元数据
type HandlerMeta struct {
	Type        string `json:"type"` // 处理器类型名（AssignmentHandler / CandidateHandler / FlowInterceptor）
	ClassName   string `json:"className"`
	DisplayName string `json:"displayName"`
	Order       int    `json:"order"`
	Group       string `json:"group"` // 拦截器 pre/post 分组，可为空
}

// HandlerRegistry 处理器注册中心（可选能力：不注册不影响引擎加载行为）
type HandlerRegistry struct {
	handlers map[string][]HandlerMeta
}

// NewHandlerRegistry 构造注册中心（构造即内置 7 个通用 AssignmentHandler 元数据，v1.6.0 issues/16）
func NewHandlerRegistry() *HandlerRegistry {
	r := &HandlerRegistry{handlers: map[string][]HandlerMeta{}}
	r.RegisterAll(builtinAssignmentMetas())
	return r
}

// builtinAssignmentMetas 内置通用参与者 handler 元数据（注册名与 Java 类全限定名一致，四语言通用）
func builtinAssignmentMetas() []HandlerMeta {
	return []HandlerMeta{
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler", DisplayName: "流程发起人", Order: -9999},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler", DisplayName: "发起人所属部门经理", Order: 10},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler", DisplayName: "发起人所属部门分管领导", Order: 20},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler", DisplayName: "当前用户所属部门经理", Order: 30},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler", DisplayName: "当前用户所属部门分管领导", Order: 40},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler", DisplayName: "根据表单字段值分配参与者", Order: 50},
		{Type: "AssignmentHandler", ClassName: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler", DisplayName: "根据任务节点唯一编码关联角色分配参与者", Order: 60},
	}
}

// Register 注册单个处理器元数据
func (r *HandlerRegistry) Register(meta HandlerMeta) {
	r.handlers[meta.Type] = append(r.handlers[meta.Type], meta)
}

// RegisterAll 批量注册
func (r *HandlerRegistry) RegisterAll(metas []HandlerMeta) {
	for _, m := range metas {
		r.Register(m)
	}
}

// ListHandlers 按处理器类型列出（按 order 升序）
func (r *HandlerRegistry) ListHandlers(typeName string) []HandlerMeta {
	list := append([]HandlerMeta{}, r.handlers[typeName]...)
	sort.SliceStable(list, func(i, j int) bool { return list[i].Order < list[j].Order })
	return list
}

// ListHandlersGroup 按处理器类型 + 分组列出（拦截器 pre/post）
func (r *HandlerRegistry) ListHandlersGroup(typeName, group string) []HandlerMeta {
	var result []HandlerMeta
	for _, m := range r.handlers[typeName] {
		if m.Group == group {
			result = append(result, m)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Order < result[j].Order })
	return result
}

// ListHandlerTypes 已注册的处理器类型名清单
func (r *HandlerRegistry) ListHandlerTypes() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}
