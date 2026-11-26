package engine

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 内置通用参与者处理器（issues/16）──────────────────────────────────────────
//
// 注册名与 Java 类全限定名一致，跨语言流程 JSON 通用（前端设计器配置天然兼容）。
// OperatorAssignmentHandler / FormFieldAssigneeHandler 为纯引擎语义，零外部依赖；
// 组织维度 handler 通过 OrgUserProvider SPI 取数据，业务方只实现数据接口。

const (
	// OperatorAssignmentHandler 流程发起人
	HandlerOperatorAssignment = "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler"
	// FormFieldAssigneeHandler 按表单字段值分配参与者
	HandlerFormFieldAssignee = "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler"
	// OrgUserAssignmentHandlers 内部类前缀
	orgHandlersPrefix = "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$"
	// DeptLeaderAssignmentHandler 当前用户部门领导
	HandlerDeptLeader = orgHandlersPrefix + "DeptLeaderAssignmentHandler"
	// DeptMainLeaderAssignmentHandler 当前用户部门分管领导
	HandlerDeptMainLeader = orgHandlersPrefix + "DeptMainLeaderAssignmentHandler"
	// ApplicantDeptLeaderAssignmentHandler 发起人部门领导
	HandlerApplicantDeptLeader = orgHandlersPrefix + "ApplicantDeptLeaderAssignmentHandler"
	// ApplicantDeptMainLeaderAssignmentHandler 发起人部门分管领导
	HandlerApplicantDeptMainLeader = orgHandlersPrefix + "ApplicantDeptMainLeaderAssignmentHandler"
	// TaskRoleAssigneeHandler 任务节点唯一编码关联角色
	HandlerTaskRoleAssignee = orgHandlersPrefix + "TaskRoleAssigneeHandler"
	// FormUserDeptLeaderAssignmentHandler 表单指定用户所属部门经理（顶级类，注册名逐字等于 Java FQN）
	HandlerFormUserDeptLeader = "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler"
	// FormUserDeptMainLeaderAssignmentHandler 表单指定用户所属部门分管领导（顶级类）
	HandlerFormUserDeptMainLeader = "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler"
	// FormDeptLeaderAssignmentHandler 表单指定部门负责人（顶级类，注册名逐字等于 Java FQN）
	HandlerFormDeptLeader = "com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler"
	// FormDeptMainLeaderAssignmentHandler 表单指定部门分管领导（顶级类）
	HandlerFormDeptMainLeader = "com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler"
)

// 表单字段编号后缀正则（task_01 → task）
var numberSuffixPattern = regexp.MustCompile(`^(.+?)_(\d+)$`)

// ─── 纯引擎语义 ─────────────────────────────────────────────────────────────────

// OperatorAssignmentHandler 流程发起人（兜底 "apply.operator"）
type OperatorAssignmentHandler struct{}

func (h *OperatorAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	if inst != nil && inst.Operator != "" {
		return []string{inst.Operator}
	}
	return []string{"apply.operator"}
}

// FormFieldAssigneeHandler 按表单字段值分配参与者：
// issues/48：f_ 前缀优先 → 裸名回落 → _数字 后缀去后缀再匹配。
// 字段值支持逗号分隔字符串 / []string / []interface{}。
type FormFieldAssigneeHandler struct{}

func (h *FormFieldAssigneeHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	if inst == nil || node == nil {
		return nil
	}
	v := findFieldValue(inst.Variables, node.ID)
	if v == nil {
		return nil
	}
	var ids []string
	addTokens(&ids, v)
	return ids
}

// ─── 组织维度（OrgUserProvider SPI）────────────────────────────────────────────

// orgBase 组织维度 handler 公共依赖
type orgBase struct {
	userProv spi.UserProvider
	orgProv  spi.OrgUserProvider
}

// deptOf 取 userId 的部门领导/分管领导（main=true 分管领导）
func (b *orgBase) byDept(deptID string, main bool) []string {
	if deptID == "" || b.orgProv == nil {
		return nil
	}
	if main {
		if ids, err := b.orgProv.FindDeptMainLeaders(deptID); err == nil {
			return ids
		}
		return nil
	}
	if ids, err := b.orgProv.FindDeptLeaders(deptID); err == nil {
		return ids
	}
	return nil
}

// deptIdOf 取 userId 的 deptId
func (b *orgBase) deptIdOf(userID string) string {
	if userID == "" || b.userProv == nil {
		return ""
	}
	u, err := b.userProv.GetUser(userID)
	if err != nil || u == nil {
		return ""
	}
	return u.DeptID
}

// DeptLeaderAssignmentHandler 当前用户（任务操作人）部门领导
type DeptLeaderAssignmentHandler struct{ orgBase }

func (h *DeptLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byDept(h.deptIdOf(operator), false)
}

// DeptMainLeaderAssignmentHandler 当前用户（任务操作人）部门分管领导
type DeptMainLeaderAssignmentHandler struct{ orgBase }

func (h *DeptMainLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byDept(h.deptIdOf(operator), true)
}

// ApplicantDeptLeaderAssignmentHandler 发起人部门领导
type ApplicantDeptLeaderAssignmentHandler struct{ orgBase }

func (h *ApplicantDeptLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	if inst == nil {
		return nil
	}
	return h.byDept(h.deptIdOf(inst.Operator), false)
}

// ApplicantDeptMainLeaderAssignmentHandler 发起人部门分管领导
type ApplicantDeptMainLeaderAssignmentHandler struct{ orgBase }

func (h *ApplicantDeptMainLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	if inst == nil {
		return nil
	}
	return h.byDept(h.deptIdOf(inst.Operator), true)
}

// TaskRoleAssigneeHandler 任务节点唯一编码关联角色（roleCode = 节点 ID）
type TaskRoleAssigneeHandler struct {
	orgProv spi.OrgUserProvider
}

func (h *TaskRoleAssigneeHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	if node == nil || h.orgProv == nil {
		return nil
	}
	if ids, err := h.orgProv.FindByRole(node.ID); err == nil {
		return ids
	}
	return nil
}

// ─── 表单指定用户 → 该用户所属部门领导 ──────────────────────────────────────────
//
// 场景（参照 mldong-fastapi-wf 提交 fe9233d）：HR 代员工提交、代理申请——表单里给出被代理人的
// 用户 id，取**该员工**所属部门的经理／分管领导，与当前操作人无关。
// 字段查找走 [findFormUserFieldValue] 的四档并集，取人走 §3.3（user → dept → leader）。

// FormUserDeptLeaderAssignmentHandler 表单指定用户所属部门的经理
type FormUserDeptLeaderAssignmentHandler struct{ orgBase }

func (h *FormUserDeptLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byFormUser(node, inst, false)
}

// FormUserDeptMainLeaderAssignmentHandler 表单指定用户所属部门的分管领导
type FormUserDeptMainLeaderAssignmentHandler struct{ orgBase }

func (h *FormUserDeptMainLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byFormUser(node, inst, true)
}

// byFormUser 表单字段 → 用户 id → 该用户部门 → 领导（main=true 分管领导）。
// 字段缺失／值为空／用户不存在／无部门／部门无领导 ⇒ 空结果，不报错（与现存 handler 同形）。
func (b *orgBase) byFormUser(node *model.FlowNode, inst *model.ProcessInstance, main bool) []string {
	if node == nil || inst == nil {
		return nil
	}
	userID := firstFormUserId(findFormUserFieldValue(inst.Variables, node.ID))
	if userID == "" {
		return nil
	}
	return b.byDept(b.deptIdOf(userID), main)
}

// ─── 表单指定部门 → 各部门负责人／分管领导（issues/258）────────────────────────

// FormDeptLeaderAssignmentHandler 表单指定 1–N 个部门，取这些部门的负责人。
type FormDeptLeaderAssignmentHandler struct{ orgBase }

func (h *FormDeptLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byFormDept(node, inst, false)
}

// FormDeptMainLeaderAssignmentHandler 表单指定 1–N 个部门，取这些部门的分管领导。
type FormDeptMainLeaderAssignmentHandler struct{ orgBase }

func (h *FormDeptMainLeaderAssignmentHandler) Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string {
	return h.byFormDept(node, inst, true)
}

// byFormDept 表单字段 → 部门 id 列表 → 各部门领导（main=true 分管领导）。
// 部门按输入顺序逐个取数，SPI 返回顺序即追加顺序；部门与最终 userId 均保序去重。
// 字段缺失／值为空／部门无领导／SPI 未注册或返回错误／无节点 ⇒ 空结果，不报错。
func (b *orgBase) byFormDept(node *model.FlowNode, inst *model.ProcessInstance, main bool) []string {
	if node == nil || inst == nil || node.ID == "" || b.orgProv == nil {
		return nil
	}
	deptIDs := formDeptIDs(findFormUserFieldValue(inst.Variables, node.ID))
	if len(deptIDs) == 0 {
		return nil
	}
	var userIDs []string
	for _, deptID := range deptIDs {
		userIDs = append(userIDs, b.byDept(deptID, main)...)
	}
	return spi.NormalizeActors(userIDs...)
}

// formDeptIDs 解析部门列表：字符串按英文逗号拆；数组逐元素读取，元素不二次拆逗号。
// trim 后丢弃空值，"0" 合法；部门 id 去重并保留首次出现顺序。
func formDeptIDs(v interface{}) []string {
	var raw []string
	switch t := v.(type) {
	case nil:
		return nil
	case []string:
		raw = append(raw, t...)
	case []interface{}:
		for _, item := range t {
			if item != nil {
				raw = append(raw, toString(item))
			}
		}
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				raw = append(raw, toString(rv.Index(i).Interface()))
			}
		} else {
			raw = strings.Split(toString(v), ",")
		}
	}
	return spi.NormalizeActors(raw...)
}

// ─── 注册 ───────────────────────────────────────────────────────────────────────

// RegisterBuiltinAssignments 注册内置通用参与者处理器到注册表。
// 组织维度 handler 依赖 userProv/orgProv；纯引擎语义 handler 无需数据源。
func RegisterBuiltinAssignments(reg *HandlerRegistry, userProv spi.UserProvider, orgProv spi.OrgUserProvider) {
	reg.RegisterAssignment(HandlerOperatorAssignment, &OperatorAssignmentHandler{})
	reg.RegisterAssignment(HandlerFormFieldAssignee, &FormFieldAssigneeHandler{})
	base := orgBase{userProv: userProv, orgProv: orgProv}
	reg.RegisterAssignment(HandlerDeptLeader, &DeptLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerDeptMainLeader, &DeptMainLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerApplicantDeptLeader, &ApplicantDeptLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerApplicantDeptMainLeader, &ApplicantDeptMainLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerTaskRoleAssignee, &TaskRoleAssigneeHandler{orgProv: orgProv})
	reg.RegisterAssignment(HandlerFormUserDeptLeader, &FormUserDeptLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerFormUserDeptMainLeader, &FormUserDeptMainLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerFormDeptLeader, &FormDeptLeaderAssignmentHandler{orgBase: base})
	reg.RegisterAssignment(HandlerFormDeptMainLeader, &FormDeptMainLeaderAssignmentHandler{orgBase: base})
}

// ─── 工具 ───────────────────────────────────────────────────────────────────────

// findFieldValue issues/48：f_ 前缀优先 → 裸名回落 → 编号后缀去后缀匹配裸名
func findFieldValue(vars map[string]interface{}, fieldName string) interface{} {
	// 1) f_ 前缀优先（表单字段变量为 f_approver）
	if v, ok := vars["f_"+fieldName]; ok {
		return v
	}
	// 2) 裸名回落（兼容存量）
	if v, ok := vars[fieldName]; ok {
		return v
	}
	// 3) _NN 后缀匹配（task_01 → task，仅查裸名）
	// ⚠️ 判据是 len(m) >= 2 而不是 == 2：numberSuffixPattern 有两个捕获组，命中时
	// FindStringSubmatch 返回「整串 + 两组」＝3 段。issues/255 之前这里写 == 2，这一档从写下起就没生效过。
	m := numberSuffixPattern.FindStringSubmatch(fieldName)
	if len(m) >= 2 {
		if v, ok := vars[m[1]]; ok {
			return v
		}
	}
	return nil
}

// findFormUserFieldValue 「表单指定用户」两个 handler 专用查找（四档并集，顺序即优先级）：
//  1. f_<nodeKey>
//  2. <nodeKey>
//  3. nodeKey 含 # 时取最后一个 # 之后的 field：先 <field>、再 f_<field>
//  4. nodeKey 匹配 ^(.+?)_(\d+)$ 时取 base：先 f_<base>、再 <base>
//
// 与 [findFieldValue] 的差别只在 3、4 两档补了 f_ 变体（参照实现 mldong-fastapi-wf 没有 f_ 分支，
// jeeflow 家族变量存 f_ 必须有）。**现存 7 个 handler 仍走 findFieldValue，本函数不改它们的行为。**
func findFormUserFieldValue(vars map[string]interface{}, nodeKey string) interface{} {
	if vars == nil || nodeKey == "" {
		return nil
	}
	if v, ok := vars["f_"+nodeKey]; ok {
		return v
	}
	if v, ok := vars[nodeKey]; ok {
		return v
	}
	if i := strings.LastIndex(nodeKey, "#"); i >= 0 {
		field := nodeKey[i+1:]
		if v, ok := vars[field]; ok {
			return v
		}
		if v, ok := vars["f_"+field]; ok {
			return v
		}
	}
	// ⚠️ 判据是 len(m) >= 2：numberSuffixPattern 有两个捕获组，命中时 FindStringSubmatch
	// 返回「整串 + 两组」＝3 段，写成 == 2 会让这一档永远不生效（老 findFieldValue 在 issues/255 之前就是这么死的）。
	if m := numberSuffixPattern.FindStringSubmatch(nodeKey); len(m) >= 2 {
		if v, ok := vars["f_"+m[1]]; ok {
			return v
		}
		if v, ok := vars[m[1]]; ok {
			return v
		}
	}
	return nil
}

// firstFormUserId 从字段值里取**第一个**非空用户 id（逗号串／数组同样只取第一个，多值语义不在本轮承诺）。
// ⚠️ 判空一律 trim 后比空串，"0" 是合法 id（与 [spi.NormalizeActors] 同一反向哨兵）。
func firstFormUserId(v interface{}) string {
	switch t := v.(type) {
	case []string:
		for _, s := range t {
			if id := strings.TrimSpace(s); id != "" {
				return id
			}
		}
		return ""
	case []interface{}:
		for _, s := range t {
			if id := strings.TrimSpace(toString(s)); id != "" {
				return id
			}
		}
		return ""
	}
	for _, s := range strings.Split(toString(v), ",") {
		if id := strings.TrimSpace(s); id != "" {
			return id
		}
	}
	return ""
}

// addTokens 收集字段值到参与者列表（字符串按逗号分隔、集合逐个展开）
func addTokens(ids *[]string, v interface{}) {
	switch t := v.(type) {
	case []string:
		for _, s := range t {
			addToken(ids, s)
		}
	case []interface{}:
		for _, s := range t {
			addToken(ids, toString(s))
		}
	default:
		addToken(ids, toString(v))
	}
}

func addToken(ids *[]string, token string) {
	for _, s := range strings.Split(token, ",") {
		s = strings.TrimSpace(s)
		if s != "" && !contains(*ids, s) {
			*ids = append(*ids, s)
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// toString 值转字符串（保留原生 string，其余 fmt.Sprint）
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
