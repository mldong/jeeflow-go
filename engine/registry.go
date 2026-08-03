package engine

import "github.com/mldong/jeeflow-go/model"

// ─── Handler Registry（对标 Spring IoC 容器）─────────────────────────────────

// IAssignmentHandler 参与者指派处理器接口
type IAssignmentHandler interface {
	// Assign 返回参与者列表（operator: 当前任务操作人，issues/16 对齐 Java Execution.getOperator）
	Assign(node *model.FlowNode, inst *model.ProcessInstance, operator string) []string
}

// IDecisionHandler 决策处理器接口
type IDecisionHandler interface {
	// Decide 返回选中的分支边 ID
	Decide(node *model.FlowNode, inst *model.ProcessInstance, vars map[string]interface{}) string
}

// HandlerRegistry 处理器注册表——按名称注册/解析
type HandlerRegistry struct {
	assignments map[string]IAssignmentHandler
	decisions   map[string]IDecisionHandler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		assignments: make(map[string]IAssignmentHandler),
		decisions:   make(map[string]IDecisionHandler),
	}
}

// RegisterAssignment 注册参与者指派处理器
func (r *HandlerRegistry) RegisterAssignment(name string, h IAssignmentHandler) {
	r.assignments[name] = h
}

// RegisterDecision 注册决策处理器
func (r *HandlerRegistry) RegisterDecision(name string, h IDecisionHandler) {
	r.decisions[name] = h
}

// ResolveAssignment 按名称解析指派处理器
func (r *HandlerRegistry) ResolveAssignment(name string) IAssignmentHandler {
	return r.assignments[name]
}

// ResolveDecision 按名称解析决策处理器
func (r *HandlerRegistry) ResolveDecision(name string) IDecisionHandler {
	return r.decisions[name]
}

// ─── 集成到 Extensions ────────────────────────────────────────────────────────

func (e *EngineImpl) SetRegistry(reg *HandlerRegistry) {
	e.registry = reg
}
