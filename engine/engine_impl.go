package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

type EngineImpl struct {
	repo             spi.ProcessRepository
	userProv         spi.UserProvider
	idGen            spi.IDGenerator
	exprEval         spi.ExpressionEvaluator
	ext              *Extensions
	registry         *HandlerRegistry
	interceptorCache map[int64][]FlowInterceptor
}

func New(repo spi.ProcessRepository, userProv spi.UserProvider, idGen spi.IDGenerator, exprEval spi.ExpressionEvaluator) *EngineImpl {
	return &EngineImpl{repo: repo, userProv: userProv, idGen: idGen, exprEval: exprEval}
}

// UserProvider 用户提供者访问（issue 41 补强：nodeProgress 姓名解析用）
func (e *EngineImpl) UserProvider() spi.UserProvider {
	return e.userProv
}

// EvalExpr 表达式求值（v1.5.0，门面 highLight 决策分支过滤用）
func (e *EngineImpl) EvalExpr(expr string, vars map[string]interface{}) (interface{}, error) {
	if e.exprEval == nil {
		return nil, fmt.Errorf("ExpressionEvaluator 未配置")
	}
	return e.exprEval.Eval(expr, vars)
}

// ─── Start ─────────────────────────────────────────────────────────────────────

func (e *EngineImpl) StartProcessInstanceByID(ctx context.Context, defineID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	def, err := e.repo.FindDefineByID(ctx, defineID)
	if err != nil || def == nil {
		return nil, fmt.Errorf("define not found: %d", defineID)
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return nil, fmt.Errorf("parse flow: %w", err)
	}
	vars := mergeVars(args, nil)
	e.addUserInfo(operator, vars)

	now := time.Now()
	// 聚合根工厂创建实例
	inst := model.NewProcessInstance(e.nextID(), defineID, operator, vars, now)
	e.repo.SaveInstance(ctx, inst)
	e.fireEvent(ProcessEvent{Type: EventProcessStart, InstanceID: inst.ID, Operator: operator})

	startNode := findNodeByType(&flow, model.TypeStart)
	if startNode == nil {
		return nil, fmt.Errorf("no start node")
	}
	for _, node := range followEdges(&flow, startNode.ID) {
		e.executeNode(ctx, &flow, inst, node, operator, vars)
	}
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Execute ───────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteProcessTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(ctx, taskID, operator)
	if err != nil {
		return nil, err
	}
	// issues/26：办理提交的 f_ 字段按任务节点字段权限过滤（只读/隐藏不入变量）——
	// 被拒值无法经流程变量落到下游节点写入，上游只读声明不可被绕过
	var flow model.FlowModel
	def, _ := e.repo.FindDefineByID(ctx, inst.DefineID)
	if def != nil {
		json.Unmarshal(def.Content, &flow)
	}
	args = filterFieldByPerm(args, findNode(&flow, task.TaskName))

	vars := mergeVars(args, inst.Variables)
	for k, v := range task.Variables {
		vars[k] = v
	}
	e.addUserInfo(operator, vars)

	now := time.Now()
	// 聚合根：完成任务（子实体状态转换 + 实例变量合并）
	inst.CompleteTask(task, operator, vars, now)
	e.repo.UpdateTask(ctx, task)
	// v1.0.1：updateInstance 级联持久化依赖聚合内任务副本为最新状态，
	// CompleteTask 改的是外部任务对象，需同步回聚合根
	syncTaskToAggregate(inst, task)
	e.fireEvent(ProcessEvent{Type: EventTaskComplete, InstanceID: inst.ID, TaskID: task.ID, NodeID: task.TaskName, Operator: operator})

	inst.Variables = vars
	e.repo.UpdateInstance(ctx, inst)

	curNode := findNode(&flow, task.TaskName)
	if curNode != nil {
		// 1.8.0：任务完成节点自身的后置拦截器（SYNC 同步演进——任务节点推进更新状态/字段）。
		// createTask 触发的同节点 PostHandle 幂等一致（同一节点同一次执行仅更新一次）
		e.firePostInterceptors(curNode, inst)
		ct, _ := stringFromProps(curNode.Properties, "countersignType")
		if ct == "SEQUENTIAL" {
			doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
			if len(doing) == 0 {
				actors, lc := getCsState(vars, curNode.ID)
				if actors != nil && lc+1 < len(actors) {
					// 聚合根：创建串行会签下一步任务
					nt := inst.CreateTask(e.nextID(), curNode.ID, curNode.Text.Value, actors[lc+1], operator, formKeyOf(curNode), now)
					nt.Variables = map[string]interface{}{
						prefixKey("nrOfInstances", curNode.ID): len(actors),
						prefixKey("loopCounter", curNode.ID):   lc + 1,
						prefixKey("operatorList", curNode.ID):  actors,
					}
					e.repo.SaveTask(ctx, nt)
					inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
					return inst, nil
				}
			} else {
				inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
				return inst, nil
			}
		}
		if ct == "PARALLEL" || strings.HasPrefix(ct, "RATIO") {
			doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
			if len(doing) > 0 {
				inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
				return inst, nil
			}
		}
		for _, node := range followEdges(&flow, curNode.ID) {
			// 统一走 executeNode：结束节点也经节点执行链（拦截器/事件完整触发），
			// executeNode 内部 TypeEnd 分支完成聚合根 Finish + 事件发布
			e.executeNode(ctx, &flow, inst, node, operator, vars)
		}
	}
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// syncTaskToAggregate 把外部任务对象的最新状态同步回聚合根任务副本
// （v1.0.1：updateInstance 级联持久化依赖聚合内任务副本为最新状态）
func syncTaskToAggregate(inst *model.ProcessInstance, task *model.ProcessTask) {
	for i, t := range inst.Tasks {
		if t.ID == task.ID {
			inst.Tasks[i] = task
			return
		}
	}
}

// ─── Reject ────────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteAndJumpToEnd(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(ctx, taskID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	// 聚合根：废弃所有进行中任务
	for _, t := range inst.AbandonAllDoing(now) {
		e.repo.UpdateTask(ctx, t)
	}
	// 子实体：完成任务
	task.Finish(operator, task.Variables, now)
	e.repo.UpdateTask(ctx, task)
	// v1.0.1：同步回聚合根，避免 updateInstance 级联把任务写回旧状态
	syncTaskToAggregate(inst, task)
	// 聚合根：驳回
	inst.Reject(now)
	e.repo.UpdateInstance(ctx, inst)
	e.fireEvent(ProcessEvent{Type: EventProcessReject, InstanceID: inst.ID, TaskID: taskID, Operator: operator})
	return inst, nil
}

// ─── Jump ─────────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteAndJumpTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(ctx, taskID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	// 聚合根：废弃所有进行中任务
	for _, t := range inst.AbandonAllDoing(now) {
		e.repo.UpdateTask(ctx, t)
	}
	// 子实体：完成任务
	task.Finish(operator, task.Variables, now)
	e.repo.UpdateTask(ctx, task)

	if targetTaskName != "" {
		var flow model.FlowModel
		def, _ := e.repo.FindDefineByID(ctx, inst.DefineID)
		if def != nil {
			json.Unmarshal(def.Content, &flow)
		}
		target := findNode(&flow, targetTaskName)
		if target != nil {
			e.executeNode(ctx, &flow, inst, target, operator, inst.Variables)
		}
	}
	return inst, nil
}

// ─── Jump To First Task（退回发起人，boot2 ROLLBACK_TO_OPERATOR=6）──────────────

func (e *EngineImpl) ExecuteAndJumpToFirstTaskNode(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(ctx, taskID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	// 聚合根：废弃所有进行中任务
	for _, t := range inst.AbandonAllDoing(now) {
		e.repo.UpdateTask(ctx, t)
	}
	// 子实体：完成任务
	task.Finish(operator, task.Variables, now)
	e.repo.UpdateTask(ctx, task)
	// 找到第一个任务节点，强制参与者为发起人，重新执行
	var flow model.FlowModel
	def, _ := e.repo.FindDefineByID(ctx, inst.DefineID)
	if def != nil {
		json.Unmarshal(def.Content, &flow)
	}
	if start := findNodeByType(&flow, model.TypeStart); start != nil {
		for _, node := range followEdges(&flow, start.ID) {
			if node.Type == model.TypeTask || node.Type == model.TypeCustom {
				node.Properties["assignee"] = inst.Operator
				e.executeNode(ctx, &flow, inst, node, operator, inst.Variables)
				break
			}
		}
	}
	return inst, nil
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

func (e *EngineImpl) loadAndCheck(ctx context.Context, taskID int64, operator string) (*model.ProcessTask, *model.ProcessInstance, error) {
	task, err := e.repo.FindTaskByID(ctx, taskID)
	if err != nil || task == nil {
		return nil, nil, fmt.Errorf("task not found: %d", taskID)
	}
	if task.TaskState != model.TaskStateDoing {
		return nil, nil, fmt.Errorf("task not doing: %d", task.TaskState)
	}
	if !e.isAllowed(task, operator) {
		return nil, nil, fmt.Errorf("operator %s not allowed", operator)
	}
	inst, err := e.repo.FindInstanceByID(ctx, task.ProcessInstanceID)
	if err != nil {
		return nil, nil, fmt.Errorf("instance not found: %w", err)
	}
	return task, inst, nil
}

func (e *EngineImpl) executeNode(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}) error {
	// 任务创建（对齐 Java CreateTaskHandler：不触发节点拦截器——创建任务 ≠ 节点执行完成；
	// 任务完成的拦截器由 ExecuteProcessTask 显式触发，1.8.0 SYNC 同步演进）
	if node.Type == model.TypeTask || node.Type == model.TypeCustom {
		return e.createTask(ctx, node, inst, operator, vars)
	}
	if !e.firePreInterceptors(node, inst) {
		return nil
	}
	defer e.firePostInterceptors(node, inst)

	switch node.Type {
	case model.TypeDecision:
		return e.evaluateDecision(ctx, flow, inst, node, operator, vars)
	case model.TypeFork:
		for _, n := range followEdges(flow, node.ID) {
			e.executeNode(ctx, flow, inst, n, operator, vars)
		}
		return nil
	case model.TypeJoin:
		doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
		if len(doing) == 0 {
			for _, n := range followEdges(flow, node.ID) {
				e.executeNode(ctx, flow, inst, n, operator, vars)
			}
		}
		return nil
	case model.TypeEnd:
		// 对齐 Java EndProcessHandler：submitType=REJECT → Reject，否则 Finish
		submitType, ok := inst.Variables[KeySubmitType]
		if ok && toIntOf(submitType) == int(model.SubmitTypeReject) {
			inst.Reject(time.Now())
		} else {
			inst.Finish(time.Now())
		}
		inst.Variables = vars
		e.repo.UpdateInstance(ctx, inst)
		e.fireEvent(ProcessEvent{Type: EventProcessFinish, InstanceID: inst.ID, Operator: operator})
		return nil
	}
	return nil
}

func (e *EngineImpl) evaluateDecision(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}) error {
	// 自定义决策处理器（Registry 优先）
	if e.registry != nil {
		handlerName, _ := node.Properties["decisionHandler"].(string)
		if handlerName == "" {
			handlerName, _ = node.Properties["assignmentHandler"].(string)
		}
		if handlerName != "" {
			if h := e.registry.ResolveDecision(handlerName); h != nil {
				branchID := h.Decide(node, inst, vars)
				if branchID != "" {
					for _, edge := range flow.Edges {
						if edge.ID == branchID {
							if target := findNode(flow, edge.TargetNodeID); target != nil {
								return e.executeNode(ctx, flow, inst, target, operator, vars)
							}
						}
					}
				}
			}
		}
	}
	// 自定义决策处理器（Extensions 兼容）
	if e.ext != nil && e.ext.DecisionHandler != nil {
		handlerName, _ := node.Properties["decisionHandler"].(string)
		branchID := e.ext.DecisionHandler(handlerName, node, inst, vars)
		if branchID != "" {
			for _, edge := range flow.Edges {
				if edge.ID == branchID {
					if target := findNode(flow, edge.TargetNodeID); target != nil {
						return e.executeNode(ctx, flow, inst, target, operator, vars)
					}
				}
			}
		}
	}
	// 表达式决策
	for _, edge := range flow.Edges {
		if edge.SourceNodeID != node.ID {
			continue
		}
		expr, _ := edge.Properties["expr"].(string)
		if expr == "" {
			if target := findNode(flow, edge.TargetNodeID); target != nil {
				return e.executeNode(ctx, flow, inst, target, operator, vars)
			}
			return nil
		}
		if e.exprEval != nil {
			result, err := e.exprEval.Eval(expr, vars)
			if err != nil {
				continue
			}
			if isTruthy(result) {
				if target := findNode(flow, edge.TargetNodeID); target != nil {
					return e.executeNode(ctx, flow, inst, target, operator, vars)
				}
				return nil
			}
		}
	}
	return nil
}

func (e *EngineImpl) createTask(ctx context.Context, node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}) error {
	actors := e.resolveActors(node, inst, operator, vars)
	if len(actors) == 0 {
		return nil
	}
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()
	form := formKeyOf(node)

	// issue 42：performType 字符串兼容（'1'/'ALL'/'COUNTERSIGN' → 会签，对齐 Java codeOf）
	if IsCountersign(node.Properties["performType"]) && ct != "" {
		switch ct {
		case "PARALLEL":
			for _, actor := range actors {
				e.repo.SaveTask(ctx, inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now))
			}
		case "SEQUENTIAL":
			nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now)
			nt.Variables = map[string]interface{}{
				prefixKey("nrOfInstances", node.ID): len(actors),
				prefixKey("loopCounter", node.ID):   0,
				prefixKey("operatorList", node.ID):  actors,
			}
			e.repo.SaveTask(ctx, nt)
		default:
			for _, actor := range actors {
				e.repo.SaveTask(ctx, inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now))
			}
		}
		return nil
	}
	// 普通任务：一个任务，全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	return e.repo.SaveTask(ctx, nt)
}

func formKeyOf(node *model.FlowNode) string {
	form, _ := node.Properties["form"].(string)
	return form
}

func (e *EngineImpl) resolveActors(node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}) []string {
	// 1a. Registry 按名称解析（推荐，对标 Spring IoC）
	if e.registry != nil {
		handlerName, _ := node.Properties["assignmentHandler"].(string)
		if handlerName != "" {
			if h := e.registry.ResolveAssignment(handlerName); h != nil {
				return h.Assign(node, inst, operator)
			}
		}
	}
	// 1b. Extensions 兼容模式（旧 API）
	if e.ext != nil && e.ext.AssignmentHandler != nil {
		handlerName, _ := node.Properties["assignmentHandler"].(string)
		if actors := e.ext.AssignmentHandler(handlerName, node, inst); len(actors) > 0 {
			return actors
		}
	}
	// 2. 动态指定下一节点处理人优先（v1.0.1：对齐 boot3 tf_nextNodeOperator）
	if v, ok := vars[KeyNextNodeOperator]; ok {
		return valueToActors(v, true)
	}
	// 3. 固定指派 assignee——token 即变量 key，能替换就换，换不了就是字面量（v1.0.1 对齐 boot3 args.get(token, token)）
	if v, ok := node.Properties["assignee"].(string); ok && v != "" {
		var actors []string
		for _, p := range strings.Split(v, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			// mldong 契约特殊值：applicant → 流程发起人
			if p == "applicant" {
				p = inst.Operator
			}
			if val, ok := vars[p]; ok {
				actors = append(actors, valueToActors(val, false)...)
			} else {
				actors = append(actors, p)
			}
		}
		return actors
	}
	return nil
}

// valueToActors 把变量值转参与者列表：
// split=true 时 String 按逗号分割（tf_nextNodeOperator 语义）；否则 String 原样单个（assignee 命中语义）
func valueToActors(v interface{}, split bool) []string {
	var out []string
	switch t := v.(type) {
	case string:
		if split {
			for _, s := range strings.Split(t, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
		} else if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	case []string:
		out = append(out, t...)
	case []interface{}:
		for _, s := range t {
			if str, ok := s.(string); ok {
				out = append(out, str)
			} else {
				out = append(out, fmt.Sprintf("%v", s))
			}
		}
	default:
		out = append(out, fmt.Sprintf("%v", v))
	}
	return out
}

func (e *EngineImpl) isAllowed(task *model.ProcessTask, operator string) bool {
	// v1.0.1：系统代执行（flow.auto）/超级管理员（flow.admin）放行（对齐 boot3 isAllowed）
	if strings.EqualFold(operator, KeyAutoExecute) || strings.EqualFold(operator, KeyAdminID) {
		return true
	}
	// 子实体：actorIds 权限判断
	if task.IsAllowed(operator) {
		return true
	}
	// 仓储兜底：任务参与人表
	actors, _ := e.repo.FindTaskActors(context.Background(), task.ID)
	for _, a := range actors {
		if a == operator {
			return true
		}
	}
	return false
}

func (e *EngineImpl) addUserInfo(operator string, vars map[string]interface{}) {
	if e.userProv == nil {
		return
	}
	// v1.0.1：系统代执行（flow.auto）/超级管理员（flow.admin）非真实用户，跳过注入（对齐 boot3）
	if strings.EqualFold(operator, KeyAutoExecute) || strings.EqualFold(operator, KeyAdminID) {
		return
	}
	u, err := e.userProv.GetUser(operator)
	if err != nil || u == nil {
		return
	}
	vars[KeyUserID] = u.UserID
	if u.RealName != "" {
		vars[KeyRealName] = u.RealName
	}
	if u.DeptID != "" {
		vars[KeyDeptID] = u.DeptID
	}
	if u.DeptName != "" {
		vars[KeyDeptName] = u.DeptName
	}
	if u.PostID != "" {
		vars[KeyPostID] = u.PostID
	}
	if u.PostName != "" {
		vars[KeyPostName] = u.PostName
	}
}

func (e *EngineImpl) nextID() int64 {
	if e.idGen != nil {
		return e.idGen.NextID()
	}
	return time.Now().UnixNano()
}

// ─── Pure Functions ────────────────────────────────────────────────────────────

func findNode(flow *model.FlowModel, id string) *model.FlowNode {
	for i := range flow.Nodes {
		if flow.Nodes[i].ID == id {
			return &flow.Nodes[i]
		}
	}
	return nil
}

func findNodeByType(flow *model.FlowModel, typ string) *model.FlowNode {
	for i := range flow.Nodes {
		if flow.Nodes[i].Type == typ {
			return &flow.Nodes[i]
		}
	}
	return nil
}

func followEdges(flow *model.FlowModel, sourceID string) []*model.FlowNode {
	var result []*model.FlowNode
	for _, edge := range flow.Edges {
		if edge.SourceNodeID == sourceID {
			if n := findNode(flow, edge.TargetNodeID); n != nil {
				result = append(result, n)
			}
		}
	}
	return result
}

func mergeVars(args map[string]interface{}, base map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range base {
		out[k] = v
	}
	for k, v := range args {
		out[k] = v
	}
	return out
}

func getCsState(vars map[string]interface{}, nodeID string) ([]string, int) {
	var actors []string
	if v, ok := vars[prefixKey("operatorList", nodeID)]; ok {
		switch a := v.(type) {
		case []string:
			actors = a
		case []interface{}:
			for _, x := range a {
				actors = append(actors, fmt.Sprint(x))
			}
		}
	}
	lc := 0
	if v, ok := vars[prefixKey("loopCounter", nodeID)]; ok {
		switch x := v.(type) {
		case float64:
			lc = int(x)
		case int:
			lc = x
		}
	}
	return actors, lc
}

func isTruthy(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val != "" && val != "false"
	case float64:
		return val != 0
	case nil:
		return false
	default:
		return true
	}
}

func prefixKey(key, nodeID string) string { return key + "_" + nodeID }

func intFromProps(props map[string]interface{}, key string) (int, bool) {
	if v, ok := props[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val), true
		case int:
			return val, true
		case string:
			var n int
			if _, err := fmt.Sscanf(val, "%d", &n); err == nil {
				return n, true
			}
		case json.Number:
			n, _ := val.Int64()
			return int(n), true
		}
	}
	return 0, false
}

func stringFromProps(props map[string]interface{}, key string) (string, bool) {
	if v, ok := props[key]; ok {
		switch val := v.(type) {
		case string:
			return val, true
		case float64:
			return fmt.Sprint(val), true
		default:
			return fmt.Sprint(val), true
		}
	}
	return "", false
}

// toIntOf 宽松数字转换（submitType 变量可能为 int/int64/float64）
func toIntOf(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return -1
}

// filterFieldByPerm 办理提交的 f_ 字段按任务节点 field 权限过滤（issues/26）——
// 任务节点 properties.field 声明 PERMISSION_f_{全名}（前端约定，优先）或
// PERMISSION_{去前缀名}（兼容）的字段，值非 EDIT(2)（只读 1/隐藏 3 等）→ 剔除不入变量。
// 键格式双兼容（issues/25），与 persist 拦截器 isEditable 同契约。
func filterFieldByPerm(args map[string]interface{}, node *model.FlowNode) map[string]interface{} {
	if len(args) == 0 || node == nil || (node.Type != model.TypeTask && node.Type != model.TypeCustom) {
		return args
	}
	field, ok := node.Properties["field"]
	if !ok {
		return args
	}
	fieldPerm, ok := field.(map[string]interface{})
	if !ok || len(fieldPerm) == 0 {
		return args
	}
	out := make(map[string]interface{}, len(args))
	for k, v := range args {
		if strings.HasPrefix(k, "f_") && len(k) > 2 {
			name := k[2:]
			perm, ok := fieldPerm["PERMISSION_f_"+name]
			if !ok {
				perm, ok = fieldPerm["PERMISSION_"+name]
			}
			if ok && toIntOf(perm) != 2 {
				continue // 只读/隐藏：剔除（不入变量）
			}
		}
		out[k] = v
	}
	return out
}

// isCountersign 会签判定（issue 42，对齐 Java ProcessTaskPerformTypeEnum.codeOf）：
// '1'/'ALL'/'COUNTERSIGN'（大小写不敏感）→ 会签。设计器属性面板保存 'ALL' 字符串符合契约
// IsCountersign 会签判定（issue 42，对齐 Java ProcessTaskPerformTypeEnum.codeOf）：
// '1'/'ALL'/'COUNTERSIGN'（大小写不敏感）→ 会签。设计器属性面板保存 'ALL' 字符串符合契约
func IsCountersign(v interface{}) bool {
	s := strings.ToUpper(strings.TrimSpace(fmt.Sprintf("%v", v)))
	return s == "1" || s == "ALL" || s == "COUNTERSIGN"
}
