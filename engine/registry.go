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

// ICustomHandler 记录类节点（`snaker:custom`／带 `properties.clazz` 的自定义节点）处理器接口。
//
// issues/142 A 批 · spec 02-flow-definition.md §6.2 第 2 条（owner 2026-09-30 拍）：
// go 没有 java 那套反射语义可依托（`Class.forName(clazz.trim()).newInstance()`），
// 故与 **C#／python 同策——按名注册表**：集成方 `RegisterCustom("<clazz 原样字符串>", handler)`，
// 引擎按 `clazz` 的**原名**查表。键不做任何归一（不剥前缀、不转大小写），
// 因为 `clazz` 是流程定义里的业务标识串，共享夹具 `flows/08-custom-node.json` 用的是
// JVM 类名 `com.mldong.jeeflow.test.TestCustomHandler`——在 go 上它就是一个名字。
//
// 返回值非 nil 时由引擎写进流程变量（键＝节点 `properties.val`，缺省 `custom_return_val`，
// 对齐 java `CustomModel.java:46-48` ＋ `FlowConst.CUSTOM_RETURN_VAL`）。
//
// ⚠️ error 的语义分两档，别混：
//   - **"未注册处理器"／"clazz 为空串"不是 error**——引擎记 WARNING 后照常落历史行＋续流，
//     严禁报错打断建单（§6.2 第 2 条，与 spec/04"节点属性配错不该把流程炸掉"同一条哲学）；
//   - **处理器自身执行失败**（查到了、跑炸了）返回的 error 一律外抛，§6.2 明写它不在豁免内——
//     那是业务错误，不是配错形状。
type ICustomHandler interface {
	// Handle 执行处理器；返回 (写进流程变量的值, error)。
	// value 为 nil ⇒ 引擎不写变量键；error 非 nil ⇒ 引擎原样外抛，中断本次流转。
	Handle(node *model.FlowNode, inst *model.ProcessInstance, operator string,
		vars map[string]interface{}) (interface{}, error)
}

// HandlerRegistry 处理器注册表——按名称注册/解析
type HandlerRegistry struct {
	assignments map[string]IAssignmentHandler
	decisions   map[string]IDecisionHandler
	customs     map[string]ICustomHandler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		assignments: make(map[string]IAssignmentHandler),
		decisions:   make(map[string]IDecisionHandler),
		customs:     make(map[string]ICustomHandler),
	}
}

// RegisterAssignment 注册参与者指派处理器
func (r *HandlerRegistry) RegisterAssignment(name string, h IAssignmentHandler) {
	if r.assignments == nil {
		r.assignments = make(map[string]IAssignmentHandler)
	}
	r.assignments[name] = h
}

// RegisterDecision 注册决策处理器
func (r *HandlerRegistry) RegisterDecision(name string, h IDecisionHandler) {
	if r.decisions == nil {
		r.decisions = make(map[string]IDecisionHandler)
	}
	r.decisions[name] = h
}

// RegisterCustom 注册记录类节点处理器。
// 键＝流程定义 `properties.clazz` 的**原样字符串**（issues/142 A 批，与 python
// `HandlerRegistry.register_custom`／C# `ServiceContext.CustomHandlers` 同一约定）。
func (r *HandlerRegistry) RegisterCustom(name string, h ICustomHandler) {
	if r.customs == nil {
		r.customs = make(map[string]ICustomHandler)
	}
	if name == "" || h == nil {
		return
	}
	r.customs[name] = h
}

// ResolveAssignment 按名称解析指派处理器
func (r *HandlerRegistry) ResolveAssignment(name string) IAssignmentHandler {
	return r.assignments[name]
}

// ResolveDecision 按名称解析决策处理器
func (r *HandlerRegistry) ResolveDecision(name string) IDecisionHandler {
	return r.decisions[name]
}

// ResolveCustom 按 `clazz` 原名解析记录类节点处理器；未注册／名为空 ⇒ nil
// （nil 不是错误：引擎侧记 WARNING 后照常落历史行＋续流，见 ICustomHandler 注释）。
//
// 注意返回的是接口值，调用方要用 `h == nil` 判空前先接成变量（本方法返回 nil 时
// 表示"查无此人"，不是"处理器返回了 nil"）。
func (r *HandlerRegistry) ResolveCustom(name string) ICustomHandler {
	if name == "" || r.customs == nil {
		return nil
	}
	return r.customs[name]
}

// ─── 集成到 Extensions ────────────────────────────────────────────────────────

func (e *EngineImpl) SetRegistry(reg *HandlerRegistry) {
	e.registry = reg
}
