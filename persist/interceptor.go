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

// DefineLoader 按定义 ID 加载流程定义（用于解析 relTableName / 流程 name）。
// Go 拦截器签名（PostHandle）不含流程模型，表名信息需由集成方注入加载函数，
// 通常直接透传仓库的 FindDefineByID。
type DefineLoader func(ctx context.Context, defineID int64) (*model.ProcessDefine, error)
// ─── PersistPostInterceptor ────────────────────────────────────────────────────

// PersistPostInterceptor 工作流业务数据入库适配拦截器（issues/18）——
// 流程结束同意后，f_ 表单数据写入业务表。对标 Java PersistPostInterceptor。
//
// 语义（spec 契约，与 Java 版一致）：
//   - 时机：EndModel 执行后（Go 引擎每个节点 PostHandle 都会触发，此处按
//     节点类型 + 实例状态 + submitType 三重过滤）+ submitType=AGREE（不同意/退回不入库）
//   - 字段：实例 Variables 中 f_ 前缀字段，去前缀
//   - 表名：流程定义顶层 relTableName，缺省回落流程 name
//   - 系统字段：writer 通用字段 + 流程上下文（process_instance_id / apply_user_id / apply_dept_id，蛇形列名约定）
//   - 幂等：bizKey = process_instance_id（先查后插，跨请求有效）
//   - 静默跳过：非结束节点 / 非同意 / 未配置表名 / writer 未注入
type PersistPostInterceptor struct {
	// Writer 动态表写入器（必须注入，否则静默跳过）
	Writer DynamicTableWriter
	// LoadDefine 流程定义加载器（必须注入；解析 relTableName / 流程 name）
	LoadDefine DefineLoader

	// 字段前缀（实例表单字段），默认 f_
	FieldPrefix string
}

// NewPersistPostInterceptor 创建拦截器
func NewPersistPostInterceptor(writer DynamicTableWriter, loader DefineLoader) *PersistPostInterceptor {
	return &PersistPostInterceptor{Writer: writer, LoadDefine: loader, FieldPrefix: "f_"}
}

// PreHandle 不拦截任何节点
func (p *PersistPostInterceptor) PreHandle(node *model.FlowNode, inst *model.ProcessInstance) (proceed bool) {
	return true
}

// Order 拦截器排序（默认 0）
func (p *PersistPostInterceptor) Order() int { return 0 }

// PostHandle 节点执行后调用——仅流程正常结束（Done）且同意时入库
func (p *PersistPostInterceptor) PostHandle(node *model.FlowNode, inst *model.ProcessInstance) {
	if p.Writer == nil || p.LoadDefine == nil {
		return // 未注入：静默跳过
	}
	// 时机：仅结束节点 + 流程正常完成（Go Finish() 置 InstanceStateDone）+ 同意
	if node == nil || node.Type != model.TypeEnd {
		return
	}
	if inst == nil || inst.State != model.InstanceStateDone {
		return
	}
	submitType, ok := inst.Variables[engine.KeySubmitType]
	if !ok || toInt(submitType) != int(model.SubmitTypeAgree) {
		return
	}

	// 同链重复触发防护（issues/19）：最后任务节点与结束节点都会触发后置拦截器，
	// 同一执行链（共享 inst.Variables）只插一次。标记写入时实例已完成持久化
	// （引擎 TypeEnd 分支先 UpdateInstance 后触发拦截器），不会落库；
	// exists 保留作为跨请求/重启的幂等兜底（先查后插语义不变）。
	chainKey := "__persist_executed_" + strconv.FormatInt(inst.ID, 10)
	if v, ok := inst.Variables[chainKey]; ok && v == true {
		return
	}
	inst.Variables[chainKey] = true

	// 表名：流程定义顶层 relTableName，缺省回落流程 name
	tableName, err := p.resolveTableName(inst)
	if err != nil || tableName == "" {
		return // 解析失败/未配置：静默跳过
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

	// 提取 f_ 前缀字段（去前缀）
	prefix := p.FieldPrefix
	if prefix == "" {
		prefix = "f_"
	}
	data := make(map[string]interface{})
	for k, v := range inst.Variables {
		if strings.HasPrefix(k, prefix) && len(k) > len(prefix) {
			data[k[len(prefix):]] = v
		}
	}

	// 流程上下文字段（蛇形列名约定，与 writer 系统字段一致）
	if _, ok := data["process_instance_id"]; !ok {
		data["process_instance_id"] = inst.ID
	}
	if _, ok := data["apply_user_id"]; !ok {
		data["apply_user_id"] = inst.Operator
	}
	if _, ok := data["apply_dept_id"]; !ok {
		data["apply_dept_id"] = inst.Variables[engine.KeyDeptID]
	}

	// 通用系统字段（writer 按配置列填充）
	p.Writer.FillSystemFields(data, true)

	_, _ = p.Writer.Insert(tableName, data)
}

// resolveTableName 从流程定义 content 顶层解析 relTableName（缺省回落 name）
func (p *PersistPostInterceptor) resolveTableName(inst *model.ProcessInstance) (string, error) {
	define, err := p.LoadDefine(context.Background(), inst.DefineID)
	if err != nil || define == nil {
		return "", err
	}
	var meta struct {
		RelTableName string `json:"relTableName"`
		Name         string `json:"name"`
	}
	if err := json.Unmarshal(define.Content, &meta); err != nil {
		return "", err
	}
	tableName := strings.TrimSpace(meta.RelTableName)
	if tableName == "" {
		tableName = strings.TrimSpace(meta.Name) // 缺省回落流程 name
	}
	return tableName, nil
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
