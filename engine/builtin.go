package engine

import (
	"fmt"
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
	m := numberSuffixPattern.FindStringSubmatch(fieldName)
	if len(m) == 2 {
		if v, ok := vars[m[1]]; ok {
			return v
		}
	}
	return nil
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
