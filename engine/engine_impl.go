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
	// 委托代理自动生效（issues/116）：surrogateRepo 未注入＝静默跳过；
	// surrogateOff 为"关闭位"（零值＝开启，见 SurrogateAutoApply 注释）
	surrogateRepo   spi.ProcessExtRepository
	surrogateOff    bool
	defineNameCache map[int64]string
}

func New(repo spi.ProcessRepository, userProv spi.UserProvider, idGen spi.IDGenerator, exprEval spi.ExpressionEvaluator, opts ...Option) *EngineImpl {
	e := &EngineImpl{repo: repo, userProv: userProv, idGen: idGen, exprEval: exprEval}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	return e
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
	e.addAutoGenTitle(def.DisplayName, vars)

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
		// issues/60：executeNode 错误（拦截器解析等）必须传播，不静默吞掉
		// 建单不变量（issues/121 P1）：发起 execution 没有"刚办结的当前任务" ⇒ parent 传 0（落库 0）
		if err := e.executeNode(ctx, &flow, inst, node, operator, vars, 0); err != nil {
			return nil, err
		}
	}
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Execute ───────────────────────────────────────────────────────────────────

func (e *EngineImpl) ExecuteProcessTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args)
	if err != nil {
		return nil, err
	}

	curNode := findNode(flow, task.TaskName)
	if curNode != nil {
		// 1.8.0：任务完成节点自身的后置拦截器（SYNC 同步演进——任务节点推进更新状态/字段）。
		// createTask 触发的同节点 PostHandle 幂等一致（同一节点同一次执行仅更新一次）
		// issues/60：声明未解析 → 显式报错（不静默跳过）
		if err := e.firePostInterceptors(curNode, inst); err != nil {
			return nil, err
		}
		now := time.Now()
		ct, _ := stringFromProps(curNode.Properties, "countersignType")
		csCond, _ := stringFromProps(curNode.Properties, "countersignCompletionCondition")
		// issues/91：会签一票否决仅当节点配置 ONE_VOTE_VETO（忽略大小写）时生效，
		// submitType=20 才跳过会签"未完成即停留"门控提前流转；否则为软拒绝——
		// 否决者任务正常完成、countersignDisagreeFlag=1 已记录为变量（供下游参考），
		// 流程不阻断（对齐 mldong 内置引擎 / Java CountersignHandler）
		csVeto := ct != "" &&
			toIntOf(vars[KeySubmitType]) == int(model.SubmitTypeCountersignDisagree) &&
			strings.EqualFold(strings.TrimSpace(csCond), "ONE_VOTE_VETO")
		if ct == "SEQUENTIAL" && !csVeto {
			doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
			if len(doing) == 0 {
				actors, lc := getCsState(vars, curNode.ID)
				if actors != nil && lc+1 < len(actors) {
					// 聚合根：创建串行会签下一步任务
					// 建单不变量（issues/121 P1，对齐 Java CountersignHandler）：串行会签的下一位成员，
					// parent＝刚办结的那一位（execution 当前任务）；首节点标记沿用现成判据按**当前节点**算
					nt := inst.CreateTask(e.nextID(), curNode.ID, curNode.Text.Value, actors[lc+1], operator, formKeyOf(curNode), now,
						task.ID, e.isFirstTaskNode(flow, curNode), 1)
					// 会签簿记并入既有变量（CreateTask 已写入 isFirstTaskNode，整体覆写会把标记丢掉）
					putTaskVars(nt, map[string]interface{}{
						prefixKey("nrOfInstances", curNode.ID): len(actors),
						prefixKey("loopCounter", curNode.ID):   lc + 1,
						prefixKey("operatorList", curNode.ID):  actors,
					})
					// issues/116：顺序会签推进的新任务同样在建单期并入生效委托代理人
					e.saveNewTask(ctx, nt, e.surrogateProcessName(flow, inst))
					// TASK_CREATE：顺序会签推进新任务落库后 fire（对齐 Java CreateTaskHandler）
					e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: curNode.ID, Operator: operator})
					inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
					return inst, nil
				}
			} else {
				inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
				return inst, nil
			}
		}
		if (ct == "PARALLEL" || strings.HasPrefix(ct, "RATIO")) && !csVeto {
			doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
			if len(doing) > 0 {
				inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
				return inst, nil
			}
		}
		// issues/91：会签节点 merged 后（ONE_VOTE_VETO 否决 / 全部完成任一路径），
		// 废弃该节点剩余 DOING 任务（对齐内置引擎 abandonProcessTask）：
		// SEQUENTIAL 后续成员任务尚未创建天然 no-op；PARALLEL 全员预创建，否决时废弃其余
		// （刚完成者已 FINISHED 不会误伤）。逐条持久化并回写聚合副本（E25：防 UpdateInstance 级联回写旧状态）
		if ct != "" {
			remaining, _ := e.repo.FindDoingTasks(ctx, inst.ID, []string{curNode.ID})
			for _, t := range remaining {
				t.Abandon(now)
				e.repo.UpdateTask(ctx, t)
				syncTaskToAggregate(inst, t)
			}
		}
		for _, node := range followEdges(flow, curNode.ID) {
			// 统一走 executeNode：结束节点也经节点执行链（拦截器/事件完整触发），
			// executeNode 内部 TypeEnd 分支完成聚合根 Finish + 事件发布
			// 建单不变量（issues/121 P1）：新任务的 parent＝本次刚办结的当前任务
			e.executeNode(ctx, flow, inst, node, operator, vars, task.ID)
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
	_, inst, _, _, err := e.prepareExecuteTask(ctx, taskID, operator, args)
	if err != nil {
		return nil, err
	}
	// 门面 submitType=2 REJECT 唯一入口（对齐 Java executeAndJumpToEnd 语义）
	inst.Reject(time.Now())
	e.repo.UpdateInstance(ctx, inst)
	e.fireEvent(ProcessEvent{Type: EventProcessReject, InstanceID: inst.ID, TaskID: taskID, Operator: operator})
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Jump（ROLLBACK 空 target / JUMP 命名 target，boot2 executeAndJumpTask）─────

func (e *EngineImpl) ExecuteAndJumpTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error) {
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args)
	if err != nil {
		return nil, err
	}
	if targetTaskName == "" {
		// issues/79：ROLLBACK 对齐 Java rejectTask——退回上一任务节点（首条输入边 source），
		// 新任务 actor=当前任务完成人（退回操作人）；无上一任务节点则不产生新待办
		prevName := e.previousTaskName(flow, task.TaskName)
		if prevName != "" {
			if prev := findNode(flow, prevName); prev != nil {
				actors := e.resolveActorsForRollback(prev, inst, operator, task)
				// 建单不变量（issues/121 P1）：回退新建的任务同样必写 parent。
				// 本路径**仍是拓扑版落点**（P2 才换血缘版）：parent 先记"谁造了它"＝被回退的当前任务；
				// 届时按契约第 8 条改为随复活行拷贝（＝上一步的上一步）。
				e.createTaskWithActors(ctx, flow, prev, inst, operator, vars, actors, task.ID)
			}
		}
	} else {
		// issues/79：对齐 Java——目标节点不存在显式报错（前端 JUMP 无效 taskName 不再静默空操作）
		target := findNode(flow, targetTaskName)
		if target == nil {
			return nil, fmt.Errorf("根据节点名称[%s]无法找到节点模型", targetTaskName)
		}
		// 对齐 Java isFirstTaskName：跳首任务节点（start 直接后继）assignee 强制为发起人
		if target.Type == model.TypeTask && e.isFirstTaskNode(flow, target) {
			if target.Properties == nil {
				target.Properties = map[string]interface{}{}
			}
			target.Properties["assignee"] = inst.Operator
		}
		// 建单不变量（issues/121 P1）：JUMP 新建任务的 parent＝本次刚办结的当前任务
		if err := e.executeNode(ctx, flow, inst, target, operator, vars, task.ID); err != nil {
			return nil, err
		}
	}
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Jump To First Task（退回发起人，boot2 ROLLBACK_TO_OPERATOR=6）──────────────

func (e *EngineImpl) ExecuteAndJumpToFirstTaskNode(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args)
	if err != nil {
		return nil, err
	}
	// 找到第一个任务节点，强制参与者为发起人，重新执行
	if start := findNodeByType(flow, model.TypeStart); start != nil {
		for _, node := range followEdges(flow, start.ID) {
			if node.Type == model.TypeTask || node.Type == model.TypeCustom {
				if node.Properties == nil {
					node.Properties = map[string]interface{}{}
				}
				node.Properties["assignee"] = inst.Operator
				// 建单不变量（issues/121 P1）：退回发起人新建的任务 parent＝本次刚办结的当前任务
				if err := e.executeNode(ctx, flow, inst, node, operator, vars, task.ID); err != nil {
					return nil, err
				}
				break
			}
		}
	}
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Execute 公共序言（对齐 Java prepareExecution）──────────────────────────────

// prepareExecuteTask 执行公共序言（对齐 Java prepareExecution）：权限校验 → f_ 字段权限
// 过滤 → 完成任务（子实体状态转换 + 实例变量合并，经 UpdateInstance 级联落库）→ 返回
// 流程模型 + 合并后执行变量。Java jump 路径不废弃其余 DOING 任务（会签兄弟任务不受影响），
// 此处保持一致。
func (e *EngineImpl) prepareExecuteTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessTask, *model.ProcessInstance, *model.FlowModel, map[string]interface{}, error) {
	task, inst, err := e.loadAndCheck(ctx, taskID, operator)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// issues/26：办理提交的 f_ 字段按任务节点字段权限过滤（只读/隐藏不入变量）
	var flow model.FlowModel
	def, _ := e.repo.FindDefineByID(ctx, inst.DefineID)
	if def != nil {
		json.Unmarshal(def.Content, &flow)
	}
	args = filterFieldByPerm(args, findNode(&flow, task.TaskName))

	// issues/97：捕获原始实例变量（start 注入的发起人 u_*）——操作人 u_* 只进执行上下文
	// 与任务行，不得整体写回实例（对齐 Java completeTask=putAll(args)，args 不含 u_*）。
	baseVars := inst.Variables
	// 任务既有变量（会签簿记、转办留痕 submitType=7/tf_transferTo 等）以实例变量为底并入，
	// 但**本次提交参数覆盖同名键**——对齐 Java ProcessTask.finish 的 `variables.putAll(args)`：
	// 转办（issues/115）把留痕写在仍然进行中的同一任务行上，若不让我方 args 反压，
	// B 后续提交"同意/会签拒绝"会被上一手的 submitType=7 顶掉（记录失真 + 一票否决判据失效）。
	vars := mergeVars(args, mergeVars(task.Variables, baseVars))
	e.addUserInfo(operator, vars)

	now := time.Now()
	// 聚合根：完成任务（子实体状态转换 + 实例变量合并）
	inst.CompleteTask(task, operator, vars, now)
	e.repo.UpdateTask(ctx, task)
	// v1.0.1：updateInstance 级联持久化依赖聚合内任务副本为最新状态，
	// CompleteTask 改的是外部任务对象，需同步回聚合根
	syncTaskToAggregate(inst, task)
	e.fireEvent(ProcessEvent{Type: EventTaskComplete, InstanceID: inst.ID, TaskID: task.ID, NodeID: task.TaskName, Operator: operator})

	// issues/97：实例变量写回排除操作人 u_*，保留 start 注入的发起人 u_*（u_realName 恒为发起人）
	inst.Variables = mergeExecIntoInstance(baseVars, vars)
	e.repo.UpdateInstance(ctx, inst)
	return task, inst, &flow, vars, nil
}

// previousTaskName 当前任务节点的首条输入边 source（issues/79 对齐 Java getPreviousTaskName）
func (e *EngineImpl) previousTaskName(flow *model.FlowModel, taskName string) string {
	node := findNode(flow, taskName)
	if node == nil {
		return ""
	}
	for _, edge := range flow.Edges {
		if edge.TargetNodeID == node.ID {
			if src := findNode(flow, edge.SourceNodeID); src != nil &&
				(src.Type == model.TypeTask || src.Type == model.TypeCustom) {
				return src.ID
			}
		}
	}
	return ""
}

// isFirstTaskNode 是否 start 直接后继任务节点（issues/79 对齐 Java FlowUtil.isFirstTaskName）
func (e *EngineImpl) isFirstTaskNode(flow *model.FlowModel, node *model.FlowNode) bool {
	start := findNodeByType(flow, model.TypeStart)
	if start == nil {
		return false
	}
	for _, edge := range flow.Edges {
		if edge.SourceNodeID == start.ID && edge.TargetNodeID == node.ID {
			return true
		}
	}
	return false
}

// resolveActorsForRollback ROLLBACK 新任务参与者：优先当前任务完成人（退回操作人，
// 对齐 Java rejectTask Collections.singletonList(currentTask.getActorId())），
// 其次按目标节点 assignee 解析
func (e *EngineImpl) resolveActorsForRollback(node *model.FlowNode, inst *model.ProcessInstance, operator string, task *model.ProcessTask) []string {
	if task.ActorID != "" {
		return []string{task.ActorID}
	}
	actors := e.resolveActors(node, inst, operator, inst.Variables)
	if len(actors) == 0 {
		actors = []string{operator}
	}
	return actors
}

// createTaskWithActors 以显式参与者建任务（会签节点拆分为逐人任务，对齐 Java 会签创建语义）
//
// parentTaskID：建单不变量 ①——产生这些任务的那个"刚办结的任务"id（发起路径为 0），见 model.CreateTask
func (e *EngineImpl) createTaskWithActors(ctx context.Context, flow *model.FlowModel, node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}, actors []string, parentTaskID int64) error {
	if len(actors) == 0 {
		return nil
	}
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()
	form := formKeyOf(node)
	pn := e.surrogateProcessName(flow, inst)
	// 建单不变量 ②：判据沿用现成的 isFirstTaskNode（start 直接后继），同一次建单只算一次
	isFirst := e.isFirstTaskNode(flow, node)
	if IsCountersign(node.Properties["performType"]) && ct != "" {
		switch ct {
		case "PARALLEL", "":
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
			}
		case "SEQUENTIAL":
			nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst, 1)
			// 会签簿记并入既有变量（整体覆写会丢掉 CreateTask 写好的 isFirstTaskNode 标记）
			putTaskVars(nt, map[string]interface{}{
				prefixKey("nrOfInstances", node.ID): len(actors),
				prefixKey("loopCounter", node.ID):   0,
				prefixKey("operatorList", node.ID):  actors,
			})
			e.saveNewTask(ctx, nt, pn)
			e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
		default:
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
			}
		}
		return nil
	}
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	e.saveNewTask(ctx, nt, pn)
	e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
	return nil
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

// executeNode 节点执行链。parentTaskID ＝本次 execution 刚办结的当前任务 id
// （建单不变量 ①，逐层透传给 decision/fork/join 落到的任务节点；发起路径传 0）
func (e *EngineImpl) executeNode(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}, parentTaskID int64) error {
	// 任务创建（对齐 Java CreateTaskHandler：不触发节点拦截器——创建任务 ≠ 节点执行完成；
	// 任务完成的拦截器由 ExecuteProcessTask 显式触发，1.8.0 SYNC 同步演进）
	if node.Type == model.TypeTask || node.Type == model.TypeCustom {
		return e.createTask(ctx, flow, node, inst, operator, vars, parentTaskID)
	}
	// issues/60：声明未解析 → 显式报错（不静默跳过）
	proceed, err := e.firePreInterceptors(node, inst)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}
	defer func() {
		// 错误理论不可达：resolve 已由上方 firePre 校验并缓存（同 inst/DefineID）
		_ = e.firePostInterceptors(node, inst)
	}()

	switch node.Type {
	case model.TypeDecision:
		return e.evaluateDecision(ctx, flow, inst, node, operator, vars, parentTaskID)
	case model.TypeFork:
		for _, n := range followEdges(flow, node.ID) {
			// issues/60：错误传播（拦截器解析等）
			if err := e.executeNode(ctx, flow, inst, n, operator, vars, parentTaskID); err != nil {
				return err
			}
		}
		return nil
	case model.TypeJoin:
		doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
		if len(doing) == 0 {
			for _, n := range followEdges(flow, node.ID) {
				// issues/60：错误传播（拦截器解析等）
				if err := e.executeNode(ctx, flow, inst, n, operator, vars, parentTaskID); err != nil {
					return err
				}
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
		// issues/97：结束节点写回同样排除操作人 u_*（保留发起人 u_*，与 prepareExecuteTask 一致）
		inst.Variables = mergeExecIntoInstance(inst.Variables, vars)
		e.repo.UpdateInstance(ctx, inst)
		e.fireEvent(ProcessEvent{Type: EventProcessFinish, InstanceID: inst.ID, Operator: operator})
		return nil
	}
	return nil
}

func (e *EngineImpl) evaluateDecision(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}, parentTaskID int64) error {
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
								return e.executeNode(ctx, flow, inst, target, operator, vars, parentTaskID)
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
						return e.executeNode(ctx, flow, inst, target, operator, vars, parentTaskID)
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
				return e.executeNode(ctx, flow, inst, target, operator, vars, parentTaskID)
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
					return e.executeNode(ctx, flow, inst, target, operator, vars, parentTaskID)
				}
				return nil
			}
		}
	}
	return nil
}

func (e *EngineImpl) createTask(ctx context.Context, flow *model.FlowModel, node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}, parentTaskID int64) error {
	actors := e.resolveActors(node, inst, operator, vars)
	if len(actors) == 0 {
		return nil
	}
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()
	form := formKeyOf(node)
	pn := e.surrogateProcessName(flow, inst)
	// 建单不变量 ②（issues/121 P1）：判据沿用现成的 isFirstTaskNode（start 直接后继），一次算好复用
	isFirst := e.isFirstTaskNode(flow, node)

	// issue 42：performType 字符串兼容（'1'/'ALL'/'COUNTERSIGN' → 会签，对齐 Java codeOf）
	if IsCountersign(node.Properties["performType"]) && ct != "" {
		switch ct {
		case "PARALLEL":
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				e.saveNewTask(ctx, nt, pn)
				// TASK_CREATE：任务落库后逐个 fire（会签多任务逐个，对齐 Java CreateTaskHandler）
				e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
			}
		case "SEQUENTIAL":
			// 顺序会签任务也是会签任务（issues/57 E29 修正：仅普通分支默认 0）
			nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst, 1)
			// 会签簿记并入既有变量（整体覆写会丢掉 CreateTask 写好的 isFirstTaskNode 标记）
			putTaskVars(nt, map[string]interface{}{
				prefixKey("nrOfInstances", node.ID): len(actors),
				prefixKey("loopCounter", node.ID):   0,
				prefixKey("operatorList", node.ID):  actors,
			})
			e.saveNewTask(ctx, nt, pn)
			e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
		default:
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
			}
		}
		return nil
	}
	// 普通任务：一个任务，全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	// issues/116：委托代理在**参与者落库前**并入（见 engine/surrogate.go），随任务一起落库
	e.saveNewTask(ctx, nt, pn)
	e.fireEvent(ProcessEvent{Type: EventTaskCreate, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator})
	return nil
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

func (e *EngineImpl) addAutoGenTitle(displayName string, vars map[string]interface{}) {
	realName, _ := vars[KeyRealName].(string)
	title := fmt.Sprintf("%s的%s-%s", realName, displayName, time.Now().Format("2006-01-02 15:04"))
	vars[KeyAutoGenTitle] = title
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

// mergeExecIntoInstance 实例变量写回合并（issues/97 对齐 Java）：以 base（start 注入的
// 发起人 u_*）为底，并入执行上下文中**非 u_*** 键（f_ 表单字段 / submitType 等流转数据）。
// addUserInfo 生成的操作人 u_* 只属于当次执行上下文与任务行 ext，不整体写回实例——
// 实例 u_realName 语义是「发起人」（与 autoGenTitle 一致），不随审批节点漂移。
//
// isFirstTaskNode 同被挡在实例之外（issues/121 P1）：它是**行级**建单标记（规范「引擎操作 04」
// 建单不变量②），实例层没有"首任务节点行"这一说。Java 侧 task 变量根本不并入实例变量
// （prepareExecution 只 merge instance.variables + args），Go 侧因会签簿记共用 exec 变量池
// （prepareExecuteTask 把 task.Variables 并入 vars），若不挡就会凭空给实例加一个逐节点漂移的
// isFirstTaskNode —— 那属于 P1 承诺"行为零变化"之外的副作用。
func mergeExecIntoInstance(base, exec map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range base {
		out[k] = v
	}
	for k, v := range exec {
		if strings.HasPrefix(k, "u_") {
			continue
		}
		if k == model.IsFirstTaskNodeKey {
			continue
		}
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

// putTaskVars 向任务行变量**并入**键值（不整体覆写）。
// 建单不变量 ②（issues/121 P1）把 isFirstTaskNode 写在 CreateTask 里，会签簿记等后置变量若用
// `nt.Variables = map[...]` 整体赋值就会把标记顺手抹掉——落库标记正是本案唯一必需的产物，故一律走并入。
func putTaskVars(nt *model.ProcessTask, kv map[string]interface{}) {
	if nt.Variables == nil {
		nt.Variables = map[string]interface{}{}
	}
	for k, v := range kv {
		nt.Variables[k] = v
	}
}

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
