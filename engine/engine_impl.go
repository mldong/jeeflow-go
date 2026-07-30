package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

type EngineImpl struct {
	repo     spi.ProcessRepository
	userProv spi.UserProvider
	idGen    spi.IDGenerator
	exprEval spi.ExpressionEvaluator
	ext      *Extensions
}

func New(repo spi.ProcessRepository, userProv spi.UserProvider, idGen spi.IDGenerator, exprEval spi.ExpressionEvaluator) *EngineImpl {
	return &EngineImpl{repo: repo, userProv: userProv, idGen: idGen, exprEval: exprEval}
}

// ─── Start ─────────────────────────────────────────────────────────────────────

func (e *EngineImpl) StartProcessInstanceByID(defineID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	def, err := e.repo.FindDefineByID(defineID)
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
	inst := &model.ProcessInstance{
		ID: e.nextID(), DefineID: defineID, State: model.InstanceStateDoing,
		Operator: operator, Variables: vars,
		CreateTime: now, UpdateTime: now, CreateUser: operator, UpdateUser: operator,
	}
	if v, ok := vars[KeyBusinessNo]; ok {
		inst.BusinessNo = fmt.Sprint(v)
	}
	e.repo.SaveInstance(inst)
	e.fireEvent(ProcessEvent{Type: EventProcessStart, InstanceID: inst.ID, Operator: operator})

	startNode := findNodeByType(&flow, model.TypeStart)
	if startNode == nil {
		return nil, fmt.Errorf("no start node")
	}
	for _, node := range followEdges(&flow, startNode.ID) {
		e.executeNode(&flow, inst, node, operator, vars)
	}
	inst, _ = e.repo.FindInstanceByID(inst.ID)
	return inst, nil
}

// ─── Execute ───────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteProcessTask(taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(taskID, operator)
	if err != nil {
		return nil, err
	}
	vars := mergeVars(args, inst.Variables)
	for k, v := range task.Variables {
		vars[k] = v
	}
	e.addUserInfo(operator, vars)

	now := time.Now()
	task.TaskState = model.TaskStateDone
	task.ActorID = operator
	task.FinishTime = &now
	task.UpdateTime = now
	task.UpdateUser = operator
	task.Variables = vars
	e.repo.UpdateTask(task)
	e.fireEvent(ProcessEvent{Type: EventTaskComplete, InstanceID: inst.ID, TaskID: task.ID, NodeID: task.TaskName, Operator: operator})

	var flow model.FlowModel
	def, _ := e.repo.FindDefineByID(inst.DefineID)
	if def != nil {
		json.Unmarshal(def.Content, &flow)
	}
	inst.Variables = vars
	e.repo.UpdateInstance(inst)

	curNode := findNode(&flow, task.TaskName)
	if curNode != nil {
		ct, _ := stringFromProps(curNode.Properties, "countersignType")
		if ct == "SEQUENTIAL" {
			doing, _ := e.repo.FindDoingTasks(inst.ID, nil)
			if len(doing) == 0 {
				actors, lc := getCsState(vars, curNode.ID)
				if actors != nil && lc+1 < len(actors) {
					nt := e.newTask(curNode, inst, actors[lc+1], operator, now)
					nt.Variables = map[string]interface{}{
						prefixKey("nrOfInstances", curNode.ID): len(actors),
						prefixKey("loopCounter", curNode.ID):   lc + 1,
						prefixKey("operatorList", curNode.ID):  actors,
					}
					e.repo.SaveTask(nt)
					inst, _ = e.repo.FindInstanceByID(inst.ID)
					return inst, nil
				}
			} else {
				inst, _ = e.repo.FindInstanceByID(inst.ID)
				return inst, nil
			}
		}
		if ct == "PARALLEL" || strings.HasPrefix(ct, "RATIO") {
			doing, _ := e.repo.FindDoingTasks(inst.ID, nil)
			if len(doing) > 0 {
				inst, _ = e.repo.FindInstanceByID(inst.ID)
				return inst, nil
			}
		}
		for _, node := range followEdges(&flow, curNode.ID) {
				if node.Type == model.TypeEnd {
					inst.State = model.InstanceStateDone
					inst.UpdateTime = time.Now()
					inst.Variables = vars
					e.repo.UpdateInstance(inst)
					e.fireEvent(ProcessEvent{Type: EventProcessFinish, InstanceID: inst.ID, Operator: operator})
			} else {
				e.executeNode(&flow, inst, node, operator, vars)
			}
		}
	}
	inst, _ = e.repo.FindInstanceByID(inst.ID)
	return inst, nil
}

// ─── Reject ────────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteAndJumpToEnd(taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(taskID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	doing, _ := e.repo.FindDoingTasks(inst.ID, nil)
	for _, t := range doing {
		t.TaskState = model.TaskStateAbandoned
		t.UpdateTime = now
		e.repo.UpdateTask(t)
	}
	task.TaskState = model.TaskStateDone
	task.ActorID = operator
	task.FinishTime = &now
	task.UpdateTime = now
	e.repo.UpdateTask(task)
	inst.State = model.InstanceStateReject
	inst.UpdateTime = now
	e.repo.UpdateInstance(inst)
	e.fireEvent(ProcessEvent{Type: EventProcessReject, InstanceID: inst.ID, TaskID: taskID, Operator: operator})
	return inst, nil
}

// ─── Jump ─────────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteAndJumpTask(taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error) {
	task, inst, err := e.loadAndCheck(taskID, operator)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	doing, _ := e.repo.FindDoingTasks(inst.ID, nil)
	for _, t := range doing {
		t.TaskState = model.TaskStateAbandoned
		t.UpdateTime = now
		e.repo.UpdateTask(t)
	}
	task.TaskState = model.TaskStateDone
	task.ActorID = operator
	task.FinishTime = &now
	task.UpdateTime = now
	e.repo.UpdateTask(task)

	if targetTaskName != "" {
		var flow model.FlowModel
		def, _ := e.repo.FindDefineByID(inst.DefineID)
		if def != nil {
			json.Unmarshal(def.Content, &flow)
		}
		target := findNode(&flow, targetTaskName)
		if target != nil {
			e.executeNode(&flow, inst, target, operator, inst.Variables)
		}
	}
	return inst, nil
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

func (e *EngineImpl) loadAndCheck(taskID int64, operator string) (*model.ProcessTask, *model.ProcessInstance, error) {
	task, err := e.repo.FindTaskByID(taskID)
	if err != nil || task == nil {
		return nil, nil, fmt.Errorf("task not found: %d", taskID)
	}
	if task.TaskState != model.TaskStateDoing {
		return nil, nil, fmt.Errorf("task not doing: %d", task.TaskState)
	}
	if !e.isAllowed(task, operator) {
		return nil, nil, fmt.Errorf("operator %s not allowed", operator)
	}
	inst, err := e.repo.FindInstanceByID(task.ProcessInstanceID)
	if err != nil {
		return nil, nil, fmt.Errorf("instance not found: %w", err)
	}
	return task, inst, nil
}

func (e *EngineImpl) executeNode(flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}) error {
	if !e.firePreInterceptors(node, inst) { return nil }
	defer e.firePostInterceptors(node, inst)

	switch node.Type {
	case model.TypeTask, model.TypeCustom:
		return e.createTask(node, inst, operator, vars)
	case model.TypeDecision:
		return e.evaluateDecision(flow, inst, node, operator, vars)
	case model.TypeFork:
		for _, n := range followEdges(flow, node.ID) {
			e.executeNode(flow, inst, n, operator, vars)
		}
		return nil
	case model.TypeJoin:
		doing, _ := e.repo.FindDoingTasks(inst.ID, nil)
		if len(doing) == 0 {
			for _, n := range followEdges(flow, node.ID) {
				e.executeNode(flow, inst, n, operator, vars)
			}
		}
		return nil
	case model.TypeEnd:
		inst.State = model.InstanceStateDone
		inst.UpdateTime = time.Now()
		inst.Variables = vars
		e.repo.UpdateInstance(inst)
		e.fireEvent(ProcessEvent{Type: EventProcessFinish, InstanceID: inst.ID, Operator: operator})
		return nil
	}
	return nil
}

func (e *EngineImpl) evaluateDecision(flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}) error {
	// 自定义决策处理器（优先级最高）
	if e.ext != nil && e.ext.DecisionHandler != nil {
		handlerName, _ := node.Properties["decisionHandler"].(string)
		branchID := e.ext.DecisionHandler(handlerName, node, inst, vars)
		if branchID != "" {
			for _, edge := range flow.Edges {
				if edge.ID == branchID {
					if target := findNode(flow, edge.TargetNodeID); target != nil {
						return e.executeNode(flow, inst, target, operator, vars)
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
				return e.executeNode(flow, inst, target, operator, vars)
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
					return e.executeNode(flow, inst, target, operator, vars)
				}
				return nil
			}
		}
	}
	return nil
}

func (e *EngineImpl) createTask(node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}) error {
	actors := e.resolveActors(node)
	if len(actors) == 0 {
		return nil
	}
	performType, _ := intFromProps(node.Properties, "performType")
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()

	if performType == 1 && ct != "" {
		switch ct {
		case "PARALLEL":
			for _, actor := range actors {
				e.repo.SaveTask(e.newTask(node, inst, actor, operator, now))
			}
		case "SEQUENTIAL":
			nt := e.newTask(node, inst, actors[0], operator, now)
			nt.Variables = map[string]interface{}{
				prefixKey("nrOfInstances", node.ID): len(actors),
				prefixKey("loopCounter", node.ID):   0,
				prefixKey("operatorList", node.ID):  actors,
			}
			e.repo.SaveTask(nt)
		default:
			for _, actor := range actors {
				e.repo.SaveTask(e.newTask(node, inst, actor, operator, now))
			}
		}
		return nil
	}
	return e.repo.SaveTask(e.newTask(node, inst, actors[0], operator, now))
}

func (e *EngineImpl) newTask(node *model.FlowNode, inst *model.ProcessInstance, actor string, operator string, now time.Time) *model.ProcessTask {
	task := &model.ProcessTask{
		ID: e.nextID(), ProcessInstanceID: inst.ID,
		TaskName: node.ID, DisplayName: node.Text.Value, TaskState: model.TaskStateDoing,
		ActorIDs: []string{actor},
		CreateTime: now, UpdateTime: now, CreateUser: operator, UpdateUser: operator,
	}
	if form, ok := node.Properties["form"].(string); ok {
		task.FormKey = form
	}
	return task
}

func (e *EngineImpl) resolveActors(node *model.FlowNode) []string {
	// 1. 动态指派（优先级最高）
	if e.ext != nil && e.ext.AssignmentHandler != nil {
		handlerName, _ := node.Properties["assignmentHandler"].(string)
		if actors := e.ext.AssignmentHandler(handlerName, node, nil); len(actors) > 0 {
			return actors
		}
	}
	// 2. 固定指派 assignee
	if v, ok := node.Properties["assignee"].(string); ok && v != "" {
		var actors []string
		for _, p := range strings.Split(v, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				actors = append(actors, p)
			}
		}
		return actors
	}
	return nil
}

func (e *EngineImpl) isAllowed(task *model.ProcessTask, operator string) bool {
	for _, a := range task.ActorIDs {
		if a == operator {
			return true
		}
	}
	actors, _ := e.repo.FindTaskActors(task.ID)
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
	u, err := e.userProv.GetUser(operator)
	if err != nil || u == nil {
		return
	}
	vars[KeyUserID] = u.UserID
	if u.RealName != "" { vars[KeyRealName] = u.RealName }
	if u.DeptID != "" { vars[KeyDeptID] = u.DeptID }
	if u.DeptName != "" { vars[KeyDeptName] = u.DeptName }
	if u.PostID != "" { vars[KeyPostID] = u.PostID }
	if u.PostName != "" { vars[KeyPostName] = u.PostName }
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
	for k, v := range base { out[k] = v }
	for k, v := range args { out[k] = v }
	return out
}

func getCsState(vars map[string]interface{}, nodeID string) ([]string, int) {
	var actors []string
	if v, ok := vars[prefixKey("operatorList", nodeID)]; ok {
		switch a := v.(type) {
		case []string: actors = a
		case []interface{}:
			for _, x := range a { actors = append(actors, fmt.Sprint(x)) }
		}
	}
	lc := 0
	if v, ok := vars[prefixKey("loopCounter", nodeID)]; ok {
		switch x := v.(type) {
		case float64: lc = int(x)
		case int: lc = x
		}
	}
	return actors, lc
}

func isTruthy(v interface{}) bool {
	switch val := v.(type) {
	case bool: return val
	case string: return val != "" && val != "false"
	case float64: return val != 0
	case nil: return false
	default: return true
	}
}

func prefixKey(key, nodeID string) string { return key + "_" + nodeID }

func intFromProps(props map[string]interface{}, key string) (int, bool) {
	if v, ok := props[key]; ok {
		switch val := v.(type) {
		case float64: return int(val), true
		case int: return val, true
		case string:
			var n int
			if _, err := fmt.Sscanf(val, "%d", &n); err == nil { return n, true }
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
		case string: return val, true
		case float64: return fmt.Sprint(val), true
		default: return fmt.Sprint(val), true
		}
	}
	return "", false
}
