package persist

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
)

// ─── DefineLoader ──────────────────────────────────────────────────────────────

// DefineLoader 按定义 ID 加载流程定义（用于解析 relTableName / persistMode / 流程 name）。
// Go 拦截器签名（PostHandle）不含流程模型，表名信息需由集成方注入加载函数，
// 通常直接透传仓库的 FindDefineByID。
type DefineLoader func(ctx context.Context, defineID int64) (*model.ProcessDefine, error)

// 持久化模式（流程定义顶层 persistMode，缺省 ARCHIVE）
const (
	PersistModeArchive = "ARCHIVE" // 结束归档（现状）：流程结束同意后落库
	PersistModeSync    = "SYNC"    // 同步演进：发起 INSERT → 任务节点 UPDATE → 结束定稿
)

// 字段权限值（任务节点 properties.field 的 PERMISSION_{字段名}，vben5-wf 机制）
const (
	PermReadOnly = 1 // 只读：不更新
	PermEdit     = 2 // 可编辑：更新
	PermHidden   = 3 // 隐藏：不更新
)

// 状态字段值：任务节点统一写 DOING（任务推进状态），结束节点写实例最终状态
var stateDoing = int(model.InstanceStateDoing)

// ─── PersistPostInterceptor ────────────────────────────────────────────────────

// PersistPostInterceptor 工作流业务数据入库适配拦截器（issues/18）——
// 按流程定义顶层 persistMode 分派：
//
//   - ARCHIVE（缺省）：流程结束同意后，f_ 表单数据写入业务表（一次落库）
//   - SYNC（1.8.0，issues/24 同步演进）：提交申请即入库（start 节点 INSERT 全量），
//     任务节点推进 UPDATE（f_ 按节点字段权限过滤 + tf_ 冗余 + 状态字段=DOING），
//     结束节点定稿 UPDATE（最终状态 FINISHED/REJECT）——不管成功失败都入库
//
// 对标 Java PersistPostInterceptor（1.8.0）。
type PersistPostInterceptor struct {
	// Writer 动态表写入器（必须注入，否则静默跳过）
	Writer DynamicTableWriter
	// LoadDefine 流程定义加载器（必须注入；解析 relTableName / persistMode / 流程 name）
	LoadDefine DefineLoader

	// 字段前缀（实例表单字段），默认 f_
	FieldPrefix string
	// 任务冗余字段前缀（审批意见等，SYNC 下冗余到业务表对应列），默认 tf_
	TaskFieldPrefix string
}

// NewPersistPostInterceptor 创建拦截器
func NewPersistPostInterceptor(writer DynamicTableWriter, loader DefineLoader) *PersistPostInterceptor {
	return &PersistPostInterceptor{Writer: writer, LoadDefine: loader, FieldPrefix: "f_", TaskFieldPrefix: "tf_"}
}

// PreHandle 不拦截任何节点
func (p *PersistPostInterceptor) PreHandle(node *model.FlowNode, inst *model.ProcessInstance) (proceed bool) {
	return true
}

// Order 拦截器排序（默认 0）
func (p *PersistPostInterceptor) Order() int { return 0 }

// PostHandle 节点执行后调用——按 persistMode 分派 ARCHIVE / SYNC
func (p *PersistPostInterceptor) PostHandle(node *model.FlowNode, inst *model.ProcessInstance) {
	if p.Writer == nil || p.LoadDefine == nil {
		return // 未注入：静默跳过
	}
	if node == nil || inst == nil {
		return
	}
	tableName, persistMode, err := p.resolveDefine(inst)
	if err != nil || tableName == "" {
		return // 解析失败/未配置：静默跳过
	}
	if strings.EqualFold(persistMode, PersistModeSync) {
		p.handleSync(node, inst, tableName)
		return
	}
	p.handleArchive(node, inst, tableName)
}

// ─── ARCHIVE（现状：结束同意归档） ─────────────────────────────────────────────

func (p *PersistPostInterceptor) handleArchive(node *model.FlowNode, inst *model.ProcessInstance, tableName string) {
	// 时机：仅结束节点 + 流程正常完成（Go Finish() 置 InstanceStateDone）+ 同意
	if node.Type != model.TypeEnd {
		return
	}
	if inst.State != model.InstanceStateDone {
		return
	}
	submitType, ok := inst.Variables[engine.KeySubmitType]
	if !ok || toInt(submitType) != int(model.SubmitTypeAgree) {
		return
	}
	if !p.markChain(node, inst) {
		return
	}
	// 幂等：以 process_instance_id 为键，先查后插。
	// 表不存在等探测失败是配置错误，必须显性暴露（与 Java/Python 抛异常一致）
	exists, err := p.Writer.Exists(tableName, "process_instance_id", inst.ID)
	if err != nil {
		panic(fmt.Errorf("persist: %w", err))
	}
	if exists {
		return
	}

	data := p.extractFields(inst, nil, false, true) // 只 f_ 全量
	p.fillContext(data, inst)
	p.Writer.FillSystemFields(data, true)
	_, _ = p.Writer.Insert(tableName, data)
}

// ─── SYNC（1.8.0 同步演进：发起入库 → 节点推进 → 结束定稿） ────────────────────

func (p *PersistPostInterceptor) handleSync(node *model.FlowNode, inst *model.ProcessInstance, tableName string) {
	if !p.markChain(node, inst) {
		return // 同链同节点不重复（节点级，issues/19 演进）
	}
	exists, err := p.Writer.Exists(tableName, "process_instance_id", inst.ID)
	if err != nil {
		panic(fmt.Errorf("persist: %w", err))
	}

	// 任务节点（TypeTask/TypeCustom）才更新业务字段：f_ 按节点字段权限过滤；
	// 结束/网关等非任务节点只定稿状态，避免全量覆盖任务节点的只读/隐藏限制
	isTask := node.Type == model.TypeTask || node.Type == model.TypeCustom
	var fieldPerm map[string]interface{}
	if isTask {
		fieldPerm = p.resolveFieldPermission(node)
	}
	data := p.extractFields(inst, fieldPerm, !exists || isTask, !exists || isTask)

	// 状态字段：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列。
	// 任务节点写 DOING(10)——任务推进状态；结束节点写实例最终状态（FINISHED/REJECT）
	stateCode := int(inst.State)
	if isTask {
		stateCode = stateDoing
	}
	p.putStateField(tableName, data, node.ID, stateCode)

	p.fillContext(data, inst)
	if !exists {
		p.Writer.FillSystemFields(data, true)
		_, _ = p.Writer.Insert(tableName, data)
		return
	}
	p.Writer.FillSystemFields(data, false) // 只填 update 组
	if _, err := p.Writer.Update(tableName, data, "process_instance_id", inst.ID); err != nil {
		panic(fmt.Errorf("persist: %w", err))
	}
}

// ─── 公共 ──────────────────────────────────────────────────────────────────────

// resolveDefine 从流程定义 content 顶层解析 relTableName / persistMode（缺省回落流程 name）
func (p *PersistPostInterceptor) resolveDefine(inst *model.ProcessInstance) (string, string, error) {
	define, err := p.LoadDefine(context.Background(), inst.DefineID)
	if err != nil || define == nil {
		return "", "", err
	}
	var meta struct {
		RelTableName string `json:"relTableName"`
		Name         string `json:"name"`
		PersistMode  string `json:"persistMode"`
	}
	if err := json.Unmarshal(define.Content, &meta); err != nil {
		return "", "", err
	}
	tableName := strings.TrimSpace(meta.RelTableName)
	if tableName == "" {
		tableName = strings.TrimSpace(meta.Name) // 缺省回落流程 name
	}
	return tableName, strings.TrimSpace(meta.PersistMode), nil
}

// markChain 同链重复触发防护（issues/19，1.8.0 节点级）：同一执行链中**每个节点**
// 触发一次（任务推进更新 + 结束定稿是不同节点，都要生效），同节点不重复；
// exists 兜底跨请求。
func (p *PersistPostInterceptor) markChain(node *model.FlowNode, inst *model.ProcessInstance) bool {
	chainKey := "__persist_executed_" + strconv.FormatInt(inst.ID, 10) + "_" + node.ID
	if v, ok := inst.Variables[chainKey]; ok && v == true {
		return false
	}
	inst.Variables[chainKey] = true
	return true
}

// extractFields 提取字段：f_ 去前缀（SYNC 下按字段权限过滤——只读/隐藏不更新；
// includeFormFields=false 时不带出，用于非任务节点定稿避免覆盖只读限制）；
// tf_ 去前缀冗余（有列则写，列过滤由 writer 做）
func (p *PersistPostInterceptor) extractFields(inst *model.ProcessInstance, fieldPerm map[string]interface{},
	includeTaskFields, includeFormFields bool) map[string]interface{} {
	prefix := p.FieldPrefix
	if prefix == "" {
		prefix = "f_"
	}
	taskPrefix := p.TaskFieldPrefix
	if taskPrefix == "" {
		taskPrefix = "tf_"
	}
	data := make(map[string]interface{})
	for k, v := range inst.Variables {
		if includeFormFields && strings.HasPrefix(k, prefix) && len(k) > len(prefix) {
			name := k[len(prefix):]
			if !p.isEditable(fieldPerm, name) {
				continue
			}
			data[name] = v
		} else if includeTaskFields && strings.HasPrefix(k, taskPrefix) && len(k) > len(taskPrefix) {
			data[k[len(taskPrefix):]] = v
		}
	}
	return data
}

// resolveFieldPermission 任务节点字段权限（node.properties.field 的 PERMISSION_x；缺省 null=全部可编辑）
func (p *PersistPostInterceptor) resolveFieldPermission(node *model.FlowNode) map[string]interface{} {
	if node == nil || node.Properties == nil {
		return nil
	}
	if field, ok := node.Properties["field"]; ok {
		if m, ok := field.(map[string]interface{}); ok && len(m) > 0 {
			return m
		}
	}
	return nil
}

// isEditable 字段可编辑判定：无声明或 EDIT(2) 可更新；READ_ONLY(1)/HIDDEN(3) 不更新
func (p *PersistPostInterceptor) isEditable(fieldPerm map[string]interface{}, fieldName string) bool {
	if len(fieldPerm) == 0 {
		return true
	}
	perm, ok := fieldPerm["PERMISSION_"+fieldName]
	if !ok {
		return true
	}
	return toInt(perm) == PermEdit
}

// putStateField 状态字段写入：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列（列探测过滤）
func (p *PersistPostInterceptor) putStateField(tableName string, data map[string]interface{}, nodeID string, stateCode int) {
	if nodeID == "" {
		return
	}
	kept, err := p.Writer.FilterColumns(tableName,
		[]string{nodeID + "_" + strconv.Itoa(stateCode), nodeID})
	if err != nil || len(kept) == 0 {
		return
	}
	data[kept[0]] = stateCode
}

// fillContext 流程上下文字段（蛇形列名约定，与 writer 系统字段一致）
func (p *PersistPostInterceptor) fillContext(data map[string]interface{}, inst *model.ProcessInstance) {
	if _, ok := data["process_instance_id"]; !ok {
		data["process_instance_id"] = inst.ID
	}
	if _, ok := data["apply_user_id"]; !ok {
		data["apply_user_id"] = inst.Operator
	}
	if _, ok := data["apply_dept_id"]; !ok {
		data["apply_dept_id"] = inst.Variables[engine.KeyDeptID]
	}
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, err := strconv.Atoi(n)
		if err == nil {
			return i
		}
	}
	return -1
}
