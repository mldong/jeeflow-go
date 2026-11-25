package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// SubProcessDefineNotFoundCode 是 issues/257 裁决 A 的统一业务错误码。
const SubProcessDefineNotFoundCode = 20010002

// SubProcessDefineNotFoundError 的 Error() 只含契约文案，Code() 单独承载业务码；
// 门面据此返回 20010002，不再把所有错误压成 99999999。
type SubProcessDefineNotFoundError struct {
	processName string
}

func (e *SubProcessDefineNotFoundError) Error() string {
	return "子流程" + e.processName + "定义不存在"
}

func (e *SubProcessDefineNotFoundError) Code() int { return SubProcessDefineNotFoundCode }

func subProcessDefineNotFound(name string) error {
	return &SubProcessDefineNotFoundError{processName: name}
}

func subProcessModel(node *model.FlowNode) *model.SubProcessModel {
	return model.ParseSubProcessModel(node)
}

type subProcessDefineFinder interface {
	FindDefineByNameAndVersion(ctx context.Context, name string, version *int) (*model.ProcessDefine, error)
}

func findSubProcessDefine(ctx context.Context, repo spi.ProcessRepository, sub *model.SubProcessModel) (*model.ProcessDefine, error) {
	var def *model.ProcessDefine
	var err error
	if finder, ok := repo.(subProcessDefineFinder); ok {
		def, err = finder.FindDefineByNameAndVersion(ctx, sub.ProcessName, sub.Version)
	} else {
		def, err = findSubProcessDefineByPage(ctx, repo, sub)
	}
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, subProcessDefineNotFound(sub.ProcessName)
	}
	return def, nil
}

func findSubProcessDefineByPage(ctx context.Context, repo spi.ProcessRepository,
	sub *model.SubProcessModel) (*model.ProcessDefine, error) {
	if strings.TrimSpace(sub.ProcessName) == "" {
		return nil, nil
	}
	const pageSize = 100
	var latest *model.DefineRow
	for pageNum := 1; ; pageNum++ {
		query := spi.PageQuery{PageNum: pageNum, PageSize: pageSize}
		query.Conditions = append(query.Conditions,
			spi.Condition{Column: "t.name", Operator: "EQ", Value: sub.ProcessName})
		if sub.Version != nil {
			query.Conditions = append(query.Conditions,
				spi.Condition{Column: "t.version", Operator: "EQ", Value: *sub.Version})
		}
		rows, total, err := repo.PageDefines(ctx, query)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.Name != sub.ProcessName || (sub.Version != nil && row.Version != *sub.Version) {
				continue
			}
			if latest == nil || row.Version > latest.Version ||
				(row.Version == latest.Version && row.ID > latest.ID) {
				latest = row
			}
		}
		if pageNum*pageSize >= total {
			break
		}
	}
	if latest == nil {
		return nil, nil
	}
	return repo.FindDefineByID(ctx, latest.ID)
}

// startProcessInstanceByID 是父节点启动子实例的专用入口：与顶层 start 复用同一实现，
// 唯一差别是写入 parentId/parentNodeName；先 SaveInstance 再发 START 的位序也保持一致。
func (e *EngineImpl) startProcessInstanceByID(ctx context.Context, defineID int64, operator string,
	args map[string]interface{}, parentID int64, parentNodeName string) (*model.ProcessInstance, error) {
	return e.startProcessInstance(ctx, defineID, operator, args, &parentID, parentNodeName)
}

func subProcessChildArgs(parentArgs map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(parentArgs))
	for k, v := range parentArgs {
		if k == KeySubmitType || k == KeyProcessStartNextNodeOperator || k == KeyNextNodeOperator ||
			k == VarBranchCtx || k == VarReapplyCtx || strings.HasPrefix(k, "tf_") {
			continue
		}
		out[k] = v
	}
	return out
}

func subProcessRealOperator(operator string, parent *model.ProcessInstance) string {
	if parent != nil && (strings.EqualFold(operator, KeyAutoExecute) || strings.EqualFold(operator, KeyAdminID)) {
		return parent.Operator
	}
	return operator
}

// applyInitialTasks 对齐 Java StartAndExecuteSupport：只自动办理启动时已存在的初始 DOING
// 快照；每个新任务先加入真实申请人，再以 flow.auto + submitType=0 执行。返回的新任务
// 都是后续执行刚刚持久化的结果，不得再走 SaveTask。
func (e *EngineImpl) applyInitialTasks(ctx context.Context, instanceID int64, applicant string,
	args map[string]interface{}) ([]*model.ProcessTask, error) {
	doing, err := e.repo.FindDoingTasks(ctx, instanceID, nil)
	if err != nil {
		return nil, err
	}
	result := make([]*model.ProcessTask, 0)
	for _, task := range doing {
		if err := e.repo.AddTaskActor(ctx, task.ID, []string{applicant}); err != nil {
			return nil, err
		}
		applyArgs := mergeVars(args, nil)
		applyArgs[KeySubmitType] = int(model.SubmitTypeApply)
		if next := applyArgs[KeyProcessStartNextNodeOperator]; next != nil && fmt.Sprint(next) != "" {
			applyArgs[KeyNextNodeOperator] = next
		}
		// 子任务仍在父事务内未提交，公开 ExecuteProcessTask 的后台查锁会退到 0 号锁并自锁；
		// 这里复用当前事务的内层执行，结果仍走同一收集器返回。
		cctx, sink, persisted := withExecutionResultSinks(ctx)
		if _, err := e.executeProcessTaskInner(cctx, task.ID, KeyAutoExecute, applyArgs); err != nil {
			return nil, err
		}
		tasks, err := e.executionResultTasks(cctx, sink, persisted)
		if err != nil {
			return nil, err
		}
		result = append(result, tasks...)
	}
	return result, nil
}

// subProcessStartHandler 启动真子实例并复用 jeeflow startAndExecute 自办语义。
// 子任务已由子实例自己的 SaveNewTask INSERT + fire，绝不进入父 execution 的普通产出通道；
// 父出边只由子结束时的 EndProcessHandler 推进一次。
func (e *EngineImpl) subProcessStartHandler(ctx context.Context, parent *model.ProcessInstance,
	sub *model.SubProcessModel, operator string, args map[string]interface{}) error {
	define, err := findSubProcessDefine(ctx, e.repo, sub)
	if err != nil {
		return err
	}
	realOperator := subProcessRealOperator(operator, parent)
	childArgs := subProcessChildArgs(args)
	parentBefore, err := e.repo.FindDoingTasks(ctx, parent.ID, nil)
	if err != nil {
		return err
	}
	before := make(map[int64]bool, len(parentBefore))
	for _, task := range parentBefore {
		before[task.ID] = true
	}

	// 隔离子的普通 newTasksSink；API 需要已落库结果时只写 persistedResultSink。
	childCtx, childTasks := withNewTasksSink(ctx)
	child, err := e.startProcessInstanceByID(childCtx, define.ID, realOperator, childArgs, parent.ID, sub.ID)
	if err != nil {
		return err
	}
	initial, err := e.applyInitialTasks(childCtx, child.ID, realOperator, childArgs)
	if err != nil {
		return err
	}
	childDoing, err := e.repo.FindDoingTasks(ctx, child.ID, nil)
	if err != nil {
		return err
	}
	// 子可能在自办初始任务时同步结束并推进父节点；这类新建父任务也要作为已持久化结果返回。
	parentAfter, err := e.repo.FindDoingTasks(ctx, parent.ID, nil)
	if err != nil {
		return err
	}
	newParentTasks := make([]*model.ProcessTask, 0)
	for _, task := range parentAfter {
		if !before[task.ID] {
			newParentTasks = append(newParentTasks, task)
		}
	}
	result := uniquePersistedTasks(newTasksOf(childTasks), initial, childDoing, newParentTasks)
	appendPersistedResult(ctx, result)
	return nil
}

// validateSubProcessTargets 是 issues/257 的写前置守卫：当前任务所有直接出边若指向
// 子流程，先查目标定义；失败时当前任务/实例/历史任务均零写入。
func (e *EngineImpl) validateSubProcessTargets(ctx context.Context, flow *model.FlowModel, current *model.FlowNode) error {
	if flow == nil || current == nil {
		return nil
	}
	for _, target := range followEdges(flow, current.ID) {
		sub := subProcessModel(target)
		if sub == nil {
			continue
		}
		if _, err := findSubProcessDefine(ctx, e.repo, sub); err != nil {
			return err
		}
	}
	return nil
}

// endProcessNode 对齐 SnakerFlow EndProcessHandler：先拒绝遗留主办，再自动办结协办，
// 写子终态并发 END，最后以父 model/instance/variables 执行父 SubProcess 出边一次。
func (e *EngineImpl) endProcessNode(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance,
	node *model.FlowNode, operator string, vars map[string]interface{}, parentTaskID int64) error {
	// 终态守卫：同一子实例被多条结束路径/并发路径触达时，不重复播 END、不重复推进父出边。
	fresh, err := e.repo.FindInstanceByID(ctx, inst.ID)
	if err != nil {
		return err
	}
	if fresh == nil {
		return fmt.Errorf("流程实例不存在")
	}
	if fresh.State != model.InstanceStateDoing {
		return nil
	}
	// 当前聚合根优先：会签合并/拒绝刚改掉的兄弟任务尚未落库，重水合会把它们恢复成 DOING。
	active := inst.GetDoingTasks()
	knownTaskIDs := make(map[int64]bool, len(inst.Tasks))
	for _, task := range inst.Tasks {
		knownTaskIDs[task.ID] = true
	}
	persistedDoing, err := e.repo.FindDoingTasks(ctx, inst.ID, nil)
	if err != nil {
		return err
	}
	for _, task := range persistedDoing {
		if !knownTaskIDs[task.ID] {
			active = append(active, task)
		}
	}
	for _, task := range active {
		if task.TaskType != int(model.TaskTypeSecondary) {
			return fmt.Errorf("存在未完成的主办任务,请确认.")
		}
	}
	activeTaskIDs := make(map[int64]bool, len(active))
	for _, task := range active {
		activeTaskIDs[task.ID] = true
	}
	endArgs := mergeVars(vars, nil)
	endArgs[KeySubmitType] = int(model.SubmitTypeAgree)
	for _, task := range active {
		// 此处已在办理当前任务的同一实例锁/事务内；不能再走公开 ExecuteProcessTask，
		// 否则同实例非重入锁会死锁。复用内层执行保持相同办结/节点/事件语义。
		if _, err := e.executeProcessTaskInner(ctx, task.ID, KeyAutoExecute, endArgs); err != nil {
			return err
		}
	}
	if len(active) > 0 {
		refreshed, err := e.repo.FindInstanceByID(ctx, inst.ID)
		if err != nil {
			return err
		}
		if refreshed != nil {
			for _, task := range refreshed.Tasks {
				index := -1
				for i, current := range inst.Tasks {
					if current.ID == task.ID {
						index = i
						break
					}
				}
				if activeTaskIDs[task.ID] || index < 0 {
					if index < 0 {
						inst.Tasks = append(inst.Tasks, task)
					} else {
						inst.Tasks[index] = task
					}
				}
			}
			inst.Variables = mergeVars(inst.Variables, refreshed.Variables)
		}
	}
	if toIntOf(vars[KeySubmitType]) == int(model.SubmitTypeReject) {
		inst.Reject(e.now())
	} else {
		inst.Finish(e.now())
	}
	inst.Variables = mergeExecIntoInstance(inst.Variables, vars)
	if err := e.saveInstance(ctx, inst); err != nil {
		return err
	}
	if err := e.CancelInstanceTimeActions(ctx, inst.ID); err != nil {
		return err
	}
	// 子终态已落库后才播子 END；父 continuation 必须排在这条事件之后。
	e.fireEvent(ctx, ProcessEvent{Type: EventProcessInstanceEnd, InstanceID: inst.ID,
		TaskID: parentTaskID, NodeID: node.ID, Operator: operator, State: int(inst.State)})

	if inst.ParentID == nil {
		return nil
	}
	parent, err := e.repo.FindInstanceByID(ctx, *inst.ParentID)
	if err != nil {
		return err
	}
	if parent == nil {
		return nil
	}
	parentDef, err := e.repo.FindDefineByID(ctx, parent.DefineID)
	if err != nil {
		return err
	}
	if parentDef == nil {
		return nil
	}
	var parentFlow model.FlowModel
	if err := json.Unmarshal(parentDef.Content, &parentFlow); err != nil {
		return err
	}
	parentNode := findNode(&parentFlow, inst.ParentNodeName)
	sub := subProcessModel(parentNode)
	if sub == nil {
		return nil
	}
	if err := e.lockInstanceRow(ctx, parent.ID); err != nil {
		return err
	}
	parent, err = e.repo.FindInstanceByID(ctx, parent.ID)
	if err != nil {
		return err
	}
	if parent == nil || parent.State != model.InstanceStateDoing {
		return nil
	}
	parentOperator := subProcessRealOperator(operator, inst)
	parentVars := mergeVars(vars, parent.Variables)
	parentVars = mergeExecIntoInstance(parent.Variables, parentVars)
	parent.Variables = parentVars
	// 回父 execution 没有“刚办结的父任务”（SubProcessModel 不是 TaskModel），故血缘传 0，
	// 不能把子任务 ID 写到父任务行。父任务/实例一律使用父 model 与父聚合落库。
	for _, target := range followEdges(&parentFlow, sub.ID) {
		if err := e.executeNode(ctx, &parentFlow, parent, target, parentOperator, parentVars, 0); err != nil {
			return err
		}
	}
	// executeNode 建父任务只 INSERT 任务行，不一定经过父实例保存；这里重新水合后补写父变量，
	// 同时级联父任务状态。重取聚合可避免把上方已同步结束的父实例再用旧 Doing 副本覆盖。
	parent, err = e.repo.FindInstanceByID(ctx, parent.ID)
	if err != nil {
		return err
	}
	if parent == nil {
		return nil
	}
	parent.Variables = mergeExecIntoInstance(parent.Variables, parentVars)
	return e.saveInstance(ctx, parent)
}

func appendPersistedResult(ctx context.Context, tasks []*model.ProcessTask) {
	sink, ok := ctx.Value(persistedResultSinkKey{}).(*[]*model.ProcessTask)
	if ok && sink != nil {
		*sink = append(*sink, tasks...)
	}
}

func uniquePersistedTasks(groups ...[]*model.ProcessTask) []*model.ProcessTask {
	out := make([]*model.ProcessTask, 0)
	seen := make(map[int64]bool)
	for _, group := range groups {
		for _, task := range group {
			if task == nil || task.ID == 0 || seen[task.ID] {
				continue
			}
			seen[task.ID] = true
			out = append(out, task)
		}
	}
	return out
}
