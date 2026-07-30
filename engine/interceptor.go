package engine

import "github.com/mldong/jeeflow-go/model"

// ─── Interceptor ───────────────────────────────────────────────────────────────

// FlowInterceptor 流程拦截器——对标 Java FlowInterceptor
type FlowInterceptor interface {
	// PreHandle 节点执行前调用，返回 false 则跳过该节点
	PreHandle(node *model.FlowNode, inst *model.ProcessInstance) (proceed bool)
	// PostHandle 节点执行后调用
	PostHandle(node *model.FlowNode, inst *model.ProcessInstance)
	// Order 拦截器排序，越小越先执行
	Order() int
}

// ─── Assignment ────────────────────────────────────────────────────────────────

// AssignmentHandler 动态参与者指派——对标 Java AssignmentHandler.assign
// 入参: 当前节点 + 流程实例，返回参与者 ID 列表（nil 表示不处理）
type AssignmentHandler func(node *model.FlowNode, inst *model.ProcessInstance) []string

// ─── Decision ──────────────────────────────────────────────────────────────────

// DecisionHandler 自定义决策处理器——对标 Java DecisionHandler
// 入参: 当前节点 + 实例 + 当前变量，返回选中的分支边 ID（空字符串表示不处理）
type DecisionHandler func(node *model.FlowNode, inst *model.ProcessInstance, vars map[string]interface{}) string

// ─── Event ─────────────────────────────────────────────────────────────────────

type EventType int

const (
	EventProcessStart    EventType = iota // 流程启动
	EventProcessFinish                    // 流程结束
	EventProcessReject                    // 流程拒绝
	EventTaskCreate                       // 任务创建
	EventTaskComplete                     // 任务完成
)

// ProcessEvent 流程事件
type ProcessEvent struct {
	Type       EventType
	InstanceID int64
	TaskID     int64
	NodeID     string
	Operator   string
}

// ProcessEventListener 事件监听器
type ProcessEventListener func(event ProcessEvent)

// ─── Engine Extensions ─────────────────────────────────────────────────────────

// Extensions 引擎扩展配置
type Extensions struct {
	Interceptors     []FlowInterceptor
	AssignmentHandler AssignmentHandler
	DecisionHandler   DecisionHandler
	Listeners        []ProcessEventListener
}

func (e *EngineImpl) SetExtensions(ext *Extensions) {
	e.ext = ext
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

// firePreInterceptors 执行前置拦截器
func (e *EngineImpl) firePreInterceptors(node *model.FlowNode, inst *model.ProcessInstance) bool {
	if e.ext == nil { return true }
	for _, ic := range e.ext.Interceptors {
		if !ic.PreHandle(node, inst) { return false }
	}
	return true
}

// firePostInterceptors 执行后置拦截器
func (e *EngineImpl) firePostInterceptors(node *model.FlowNode, inst *model.ProcessInstance) {
	if e.ext == nil { return }
	for _, ic := range e.ext.Interceptors {
		ic.PostHandle(node, inst)
	}
}

// fireEvent 发布事件
func (e *EngineImpl) fireEvent(evt ProcessEvent) {
	if e.ext == nil { return }
	for _, l := range e.ext.Listeners {
		l(evt)
	}
}
