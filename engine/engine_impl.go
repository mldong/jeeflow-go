package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	// issues/137 A · 批二 §3-4（A 案）：实例级 expire_time ＝定义**顶层** expireTime 表达式的
	// **求值结果**（时刻），不是原串。位置逐字对齐 java JeeflowEngineImpl:93-96——
	// 聚合根创建之后、saveInstance 之前：发起腿此后不再有第二次 UpdateInstance（DOING 实例），
	// 挂在 SaveInstance 之后就只是改了内存里的聚合对象，落库那一列仍是 NULL。
	// args 传 vars（发起参数那份，addUserInfo/addAutoGenTitle 已注入 u_* 与 autoGenTitle），
	// 不是原始 args；没配 / 算不出 ⇒ 保持 NULL（见 applyInstanceExpireTime）。
	applyInstanceExpireTime(inst, flow.ExpireTime, vars)
	e.repo.SaveInstance(ctx, inst)
	// PROCESS_INSTANCE_START(1)：实例行 insert 之后 fire（§11.3 触发时机列）
	e.fireEvent(ProcessEvent{Type: EventProcessInstanceStart, InstanceID: inst.ID, Operator: operator})

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
	// issues/127 / spec 11-events §11.7 办理时抄送（tf_ccActors）：本栈此前整条腿缺失。
	// 覆盖面按 §11.7 边界 2 收窄到**只有 executeProcessTask 这一条路径**建 cc——基准＝Java
	// JeeflowEngineImpl 里 handleCcActors 的唯一调用点就在 executeProcessTask，
	// executeAndJumpTask / jumpToEnd / rollbackToOperator 三档都没有这条腿
	// （go 第一轮把漏斗挂在 prepareExecuteTask 上 ⇒ 四档全建，属"单栈超集"跨栈分叉，已纠）。
	//
	// 漏斗挂在"任务行 update 落库之后、TASK_COMPLETE(5) fire 之前"这个钩子位上：
	// ① 与任务更新同一个 ctx（jdbc 仓 WithTx 绑 ctx，集成层包事务即同事务）＝§11.7 边界 1；
	// ② "建 cc 行落库 → 逐人 fire 码 4"的单一漏斗形状不变（HandleCcActors 一条路）。
	// 不带 tf_ccActors ⇒ parseCcActors 给空集，零写入零 fire，行为与历史一致。
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args,
		func(ctx context.Context, instanceID int64) error {
			return e.HandleCcActors(ctx, instanceID, operator, args[KeyCcActors])
		})
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
					// issues/126 案 A 写点：串行会签**推进到的那一位**同样按节点表达式算到期时间
					// （变量源＝实例变量 inst.Variables，对齐 Java createCountersignTasks 走 this.variables）
					applyExpireTime(nt, expireExprOf(curNode), inst.Variables)
					// issues/116：顺序会签推进的新任务同样在建单期并入生效委托代理人
					e.saveNewTask(ctx, nt, e.surrogateProcessName(flow, inst))
					// PROCESS_TASK_START：顺序会签推进新任务落库后 fire（对齐 Java CreateTaskHandler）
					e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID,
						NodeID: curNode.ID, Operator: operator, Actors: nt.ActorIDs})
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
	// §11.7 边界 2：跳转·回退档不挂抄送钩子（cc 钩子传 nil）——tf_ccActors 在这一档不建 cc、
	// 不发 CC_CREATE，与 Java executeAndJumpToEnd（无 handleCcActors）同形。
	_, inst, _, _, err := e.prepareExecuteTask(ctx, taskID, operator, args, nil)
	if err != nil {
		return nil, err
	}
	// 门面 submitType=2 REJECT 唯一入口（对齐 Java executeAndJumpToEnd 语义）
	inst.Reject(time.Now())
	if err := e.repo.UpdateInstance(ctx, inst); err != nil {
		return nil, err
	}
	// 实例终态统一 PROCESS_INSTANCE_END(2)，靠载荷 state 分（§11.6：旧 EventProcessReject
	// 那一支"实例级拒绝"并入 2；任务级退回在 prepareExecuteTask 已发 TASK_REJECT(6)）。
	// 任务退回(6) 与本支(2) 不互斥——两个事实：任务被拒 + 实例进终态。
	e.fireEvent(ProcessEvent{Type: EventProcessInstanceEnd, InstanceID: inst.ID,
		TaskID: taskID, Operator: operator, State: int(inst.State)})
	inst, _ = e.repo.FindInstanceByID(ctx, inst.ID)
	return inst, nil
}

// ─── Jump（ROLLBACK 空 target / JUMP 命名 target，boot2 executeAndJumpTask）─────

func (e *EngineImpl) ExecuteAndJumpTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error) {
	// §11.7 边界 2：JUMP/ROLLBACK 两档都不建 cc（cc 钩子传 nil），与 Java executeAndJumpTask 同形
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args, nil)
	if err != nil {
		return nil, err
	}
	if targetTaskName == "" {
		// issues/121 P2：ROLLBACK 走血缘版——复活血缘前驱（task_parent_id）那条历史行，
		// 参与者＝该行办结人（首任务节点行则取该行 u_userId）。无血缘/守卫不过显式报错，
		// 不再像拓扑版那样"什么都不做、实例保持 DOING 却零待办"。
		if err := e.rollbackToParent(ctx, flow, inst, task, operator); err != nil {
			return nil, err
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
	// §11.7 边界 2：退发起人档不建 cc（cc 钩子传 nil），与 Java executeAndJumpToFirstTaskNode 同形
	task, inst, flow, vars, err := e.prepareExecuteTask(ctx, taskID, operator, args, nil)
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
//
// onTaskUpdated 是"任务行已 update 落库、TASK_COMPLETE(5)/TASK_REJECT(6) 尚未 fire"时刻的
// 可选钩子，签名收实例 id（钩子要同 ctx、要 inst.ID）。**只有 ExecuteProcessTask 传它**
// （传的是抄送漏斗 HandleCcActors），跳转·回退三档传 nil —— spec §11.7 边界 2 要求
// "办理抄送只算 executeProcessTask 一条"，把钩子做成调用方注入而不是写死在公共序言里，
// 覆盖面就钉在类型上：新加一条走 prepareExecuteTask 的动作默认不带 cc。
func (e *EngineImpl) prepareExecuteTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}, onTaskUpdated func(ctx context.Context, instanceID int64) error) (*model.ProcessTask, *model.ProcessInstance, *model.FlowModel, map[string]interface{}, error) {
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
	// spec §11.2 原则 3：任务行 update **成功**之后才 fire（失败直接传播，不发假事件）
	if err := e.repo.UpdateTask(ctx, task); err != nil {
		return nil, nil, nil, nil, err
	}
	// v1.0.1：updateInstance 级联持久化依赖聚合内任务副本为最新状态，
	// CompleteTask 改的是外部任务对象，需同步回聚合根
	syncTaskToAggregate(inst, task)

	// issues/127 / spec §11.7：办理时抄送不在公共序言里无条件跑——由调用方按路径注入钩子
	// （只有 ExecuteProcessTask 注入 cc 漏斗；跳转·回退三档注入 nil ⇒ 本轮不建 cc、不发码 4）。
	// 排在任务行 update 之后、任务事件 fire 之前：与任务更新同一个 ctx（jdbc 仓 WithTx 绑 ctx，
	// 集成层包事务即同事务），且 cc 行落库后才逐人 fire CC_CREATE（§11.2 原则 3）。
	if onTaskUpdated != nil {
		if err := onTaskUpdated(ctx, inst.ID); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	// 任务被办掉这件事：TASK_COMPLETE(5) 与 TASK_REJECT(6) **互斥**（§11.3 码 5/6 注），
	// 退回/拒绝族动作只发 6，具体退法靠载荷 submitType 分（§11.2 原则 2「码粗、载荷细」）。
	submit := toIntOf(vars[KeySubmitType])
	evt := ProcessEvent{
		Type: EventTaskComplete, InstanceID: inst.ID, TaskID: task.ID,
		NodeID: task.TaskName, Operator: operator, SubmitType: &submit,
	}
	if isRejectSubmitType(vars[KeySubmitType]) {
		evt.Type = EventTaskReject
	}
	e.fireEvent(evt)

	// issues/97：实例变量写回排除操作人 u_*，保留 start 注入的发起人 u_*（u_realName 恒为发起人）
	inst.Variables = mergeExecIntoInstance(baseVars, vars)
	e.repo.UpdateInstance(ctx, inst)
	return task, inst, &flow, vars, nil
}

// isRejectSubmitType 办理动作是否属"退回/拒绝"族 ⇒ 走 TASK_REJECT(6) 而非 TASK_COMPLETE(5)。
// 2 REJECT（拒绝即结束）/ 3 ROLLBACK 退回上一步 / 6 ROLLBACK_TO_OPERATOR 退发起人 /
// 20 COUNTERSIGN_DISAGREE 软拒绝——§11.3 码 6 事实列点名"含退发起人、软拒绝、跳转回退"。
// 未提交 submitType（toIntOf 给 -1，引擎直调路径常见）⇒ 不算退回，维持 5 的既有语义。
// ⚠️ 4 JUMP 是"跳到指定节点"，可能是正向跳转，按 5 处理（本轮唯一存疑档，见收口报告）。
func isRejectSubmitType(v interface{}) bool {
	switch toIntOf(v) {
	case int(model.SubmitTypeReject), int(model.SubmitTypeRollback),
		int(model.SubmitTypeRollbackToOperator), int(model.SubmitTypeCountersignDisagree):
		return true
	}
	return false
}

// rollbackToParent 退回上一步（血缘版，规范 04 · 退回上一步）：上一步来源＝当前行的
// TaskParentID，复活那条历史行；不按模型入边拓扑推（拓扑版在分支/回环流会回到本实例
// 没走过的节点）。对外 msg 用固定中文文案、不含引擎内部码（本栈 error 无码位，出口统一 99999999）。
func (e *EngineImpl) rollbackToParent(ctx context.Context, flow *model.FlowModel,
	inst *model.ProcessInstance, task *model.ProcessTask, operator string) error {
	const noLineage = "上一步任务ID为空，无法驳回至上一步处理"
	const guardFail = "无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务"

	var parentID int64
	if task.ParentTaskID != nil {
		parentID = *task.ParentTaskID
	}
	if parentID == 0 {
		return fmt.Errorf(noLineage)
	}
	his, err := e.repo.FindTaskByID(ctx, parentID)
	if err != nil {
		return err
	}
	if his == nil {
		return fmt.Errorf(noLineage)
	}
	prev := findNode(flow, his.TaskName)
	if prev == nil || !canRejected(flow, task.TaskName, prev.ID) {
		return fmt.Errorf(guardFail)
	}
	// 首任务节点那条由发起人提交 ⇒ 参与者取该行 u_userId；其余取该行办结人。
	// 老行没这个键 ⇒ 按 false 处理（宁可派给该行 ActorID，也不用带"仅进行中"判定的现算值）。
	isFirst := his.Variables[model.IsFirstTaskNodeKey] == true
	actor := his.ActorID
	if isFirst {
		if uid, ok := his.Variables["u_userId"].(string); ok && uid != "" {
			actor = uid
		} else {
			actor = inst.Operator
		}
	}
	if actor == "" {
		return fmt.Errorf(noLineage)
	}
	var carry int64
	if his.ParentTaskID != nil {
		carry = *his.ParentTaskID // parent 随行拷贝＝"上一步的上一步"，与 mldong-boot2 一致
	}
	now := time.Now()
	nt := inst.CreateTask(e.nextID(), prev.ID, prev.Text.Value, actor, his.CreateUser,
		formKeyOf(prev), now, carry, isFirst)
	// 复活行只带数据类键：tf_*/csv_*/submitType/taskName/会签簿记都是"上次提交"的残留
	nt.Variables = lineageVars(his.Variables)
	putTaskVars(nt, map[string]interface{}{model.IsFirstTaskNodeKey: isFirst})
	// issues/126 案 A 写点（回退/跳转新建）：到期时间按**被回退掉的那个节点**（＝当前行所属节点
	// task.TaskName，boot2 里的 current）的表达式重算，**不是**复活行落地的节点（prev＝历史行所属
	// 节点；form 等数据类字段仍照 prev 走，boot2 就是这个形状）。基准逐字：
	// ProcessTaskServiceImpl.rejectTask :363 current = model.getNode(currentTask.getTaskName())
	// → :385 expireTime = ((TaskModel)current).getExpireTime()
	// → :387 task.setExpireTime(FlowUtil.processTime(expireTime, hisVariable))（java/php/csharp/rust 同此）
	// 变量源用**随行拷贝那份变量** nt.Variables（＝boot2 的 hisVariable），不是实例变量——
	// 两档搞混会让"表达式是个变量名"这一档跨栈给出不同答案
	applyExpireTime(nt, expireExprOf(findNode(flow, task.TaskName)), nt.Variables)
	pn := e.surrogateProcessName(flow, inst)
	e.saveNewTask(ctx, nt, pn)
	e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID,
		NodeID: prev.ID, Operator: operator, Actors: nt.ActorIDs})
	return nil
}

// canRejected 照 mldong-boot2 NodeModel.canRejected：自 current 的入边回溯，命中 parent 放行；
// 来源是 fork/join/start 时**跳过该条入边、不再深入**（boot2 是 continue，不是穿透），
// 其余来源节点递归下去。subprocess 在 boot2 里被注释掉，等同普通节点。
func canRejected(flow *model.FlowModel, currentID, parentID string) bool {
	for _, edge := range flow.Edges {
		if edge.TargetNodeID != currentID {
			continue
		}
		if edge.SourceNodeID == parentID {
			return true
		}
		src := findNode(flow, edge.SourceNodeID)
		if src == nil {
			continue
		}
		if src.Type == model.TypeFork || src.Type == model.TypeJoin || src.Type == model.TypeStart {
			continue
		}
		if canRejected(flow, src.ID, parentID) {
			return true
		}
	}
	return false
}

// lineageVars 复活行的变量净化：剔控制类残留，保留 f_*/u_*/autoGenTitle/isFirstTaskNode。
func lineageVars(src map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range src {
		if k == "submitType" || k == "taskName" ||
			strings.HasPrefix(k, "tf_") || strings.HasPrefix(k, "csv_") ||
			strings.HasPrefix(k, "loopCounter") || strings.HasPrefix(k, "nrOfInstances") ||
			strings.HasPrefix(k, "operatorList") {
			continue
		}
		out[k] = v
	}
	return out
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

// createTaskWithActors 以显式参与者建任务（会签节点拆分为逐人任务，对齐 Java 会签创建语义）
//
// ⚠️ issues/137 B：**当前本仓零调用者**（全仓 grep 只命中本定义与 surrogate.go:112 的注释——
// 主路径 executeNode → createTask 自己走 resolveActors 解析参与者，没有任何路径把"已算好的
// 显式参与者集合"递进来）。**不删**：它承担**契约形状义务**——引擎对外承诺"给定参与者集合即可按
// 主路径同语义建单"，① 其他语言栈的同名入口以此对照；② 未来的显式指派入口（预派人、外部按
// 组织/角色算好参与者）照这个形状接，而不是绕回"传节点属性让引擎再解析一遍"。
// 零调用者 ≠ 零契约，所以它的行为必须与 createTask 锁死在同一格测试里：
// 见 engine/create_task_with_actors_137_test.go（普通 / 并行会签 / 串行会签 / 未知会签类型兜底 /
// 单参与者 / applicant / 空参与者 七档，比读回持久值 + 事件序列，另有一格建单不变量守卫）。
// 改动 createTask 的建单语义时，请同步看那一格——它红了就是两条路分叉了。
//
// ⚠️ issues/142 A 批（2026-09-30）把两条路的"空参与者"档一起从「不建单」改成「建一条零参与者
// DOING 行」⇒ 上面那格 `TestIssue137BEmptyActorsIsNoop`（钉的是旧形状）会红，
// **期望值本轮一字未改**，按指令列进交付报告"待拍"清单，不在这里自行改判。
//
// parentTaskID：建单不变量 ①——产生这些任务的那个"刚办结的任务"id（发起路径为 0），见 model.CreateTask
func (e *EngineImpl) createTaskWithActors(ctx context.Context, flow *model.FlowModel, node *model.FlowNode, inst *model.ProcessInstance, operator string, vars map[string]interface{}, actors []string, parentTaskID int64) error {
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()
	form := formKeyOf(node)
	// issues/126 案 A：节点到期表达式一次读好复用（未配 → ""，四处写点各自保持该列 NULL）
	expr := expireExprOf(node)
	pn := e.surrogateProcessName(flow, inst)
	// 建单不变量 ②：判据沿用现成的 isFirstTaskNode（start 直接后继），同一次建单只算一次
	isFirst := e.isFirstTaskNode(flow, node)

	// issues/142 A 批 · spec 02 §6.2 第 3 条：**零参与者同样建单**，与 createTask 逐字同形
	// （本函数存在的理由就是"两条路锁死在同一语义上"，见上面那段注释；主路径改建单形状而这里
	// 不动，就是自己承认两条路分叉）。旧形状 `if len(actors) == 0 { return nil }`（原 :511-513）
	// 一并撤掉。串行会签 actors[0] 那一支因此在零参与者时不会被走到（旧代码在此会 panic）。
	if len(actors) == 0 {
		pt := 0
		if IsCountersign(node.Properties["performType"]) && ct != "" {
			pt = 1
		}
		nt := inst.CreateTaskWithActors(e.nextID(), node.ID, node.Text.Value, nil, operator, form, now, parentTaskID, isFirst, pt)
		applyExpireTime(nt, expr, inst.Variables)
		e.saveNewTask(ctx, nt, pn)
		e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		return nil
	}

	if IsCountersign(node.Properties["performType"]) && ct != "" {
		switch ct {
		case "PARALLEL", "":
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				// issues/126 案 A 写点：并行会签**全员**逐条按节点表达式算到期时间
				applyExpireTime(nt, expr, inst.Variables)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
			}
		case "SEQUENTIAL":
			nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst, 1)
			// 会签簿记并入既有变量（整体覆写会丢掉 CreateTask 写好的 isFirstTaskNode 标记）
			putTaskVars(nt, map[string]interface{}{
				prefixKey("nrOfInstances", node.ID): len(actors),
				prefixKey("loopCounter", node.ID):   0,
				prefixKey("operatorList", node.ID):  actors,
			})
			// issues/126 案 A 写点：串行会签**首位成员**按节点表达式算到期时间
			applyExpireTime(nt, expr, inst.Variables)
			e.saveNewTask(ctx, nt, pn)
			e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		default:
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				// issues/126 案 A 写点：未知 countersignType 兜底分支也按逐人建单，同样要算
				applyExpireTime(nt, expr, inst.Variables)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
			}
		}
		return nil
	}
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	// issues/126 案 A 写点：普通建单（显式参与者入口）按节点表达式算到期时间
	applyExpireTime(nt, expr, inst.Variables)
	e.saveNewTask(ctx, nt, pn)
	e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
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

// isKnownNodeType 类型表成员判定——表体就是 model/types.go:40-46 那七档 `snaker:*` 常量，
// 逐字精确匹配：不剥 `snaker:` 前缀、不做大小写归一（G4 义务 1 的归一化是另一批的事，
// spec/02:95-107 owner 2026-10-01 已裁定「历史旧账不管」，本处只兑现义务 2 的「不得静默」）。
//
// ⚠️ 它**只用来决定要不要记那条可诊断日志**，不参与任何分派 ⇒ 行为零变化。
func isKnownNodeType(t string) bool {
	switch t {
	case model.TypeStart, model.TypeEnd, model.TypeTask, model.TypeDecision,
		model.TypeFork, model.TypeJoin, model.TypeCustom:
		return true
	}
	return false
}

// executeNode 节点执行链。parentTaskID ＝本次 execution 刚办结的当前任务 id
// （建单不变量 ①，逐层透传给 decision/fork/join 落到的任务节点；发起路径传 0）
func (e *EngineImpl) executeNode(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance, node *model.FlowNode, operator string, vars map[string]interface{}, parentTaskID int64) error {
	// ── 未知档可诊断日志 · spec/02-flow-definition.md「类型键的三条义务」第 2 条（issues/141 G4）──
	// 「类型不在表里时，必须记一条可诊断日志（带节点 id 与实得类型串）再决定跳过，不允许
	// 『静默丢节点＋连带丢它的出边』」。
	//
	// 为什么发在这一处：go 没有独立的解析阶段——`json.Unmarshal`（StartProcessInstanceByID:60-63、
	// prepareExecuteTask:311）把 nodes 原样收进 model.FlowModel，一个都不删；**类型表只在执行腿
	// 这一处被查**（下面那条 if 链 ＋ switch），未命中一路落到 switch 之后的 `return nil`——
	// 那里就是本栈「丢节点＋丢出边」的现场。所以这一处就是 go 的判点，也是唯一会打日志的一处
	// （不存在第二处查表 ⇒ 不存在同节点双打；同一节点被多条入边各触达一次时，每次触达各记
	// 一条它自己的事实，那不是刷屏）。
	//
	// 为什么必须带**实得类型串原文**：子流程 `snaker:subProcess` 在 go/python/node 六栈按**未知档**
	// 暴露（spec/02:107-111，owner 2026-10-01 二拍「子流程暂不进契约面」⇒ 设计器画出它时，
	// 义务 2 这条日志就是它现状唯一的可诊断面）。逐字打原文、不剥前缀、不归一大小写：
	// `snaker:subProcess`（缺档）／`snaker:Custom`（大小写拼错）／`task`（裸名，本栈类型表只有
	// `snaker:*` 七档、无裸名档）／缺 `type` 键（空串）四类病灶要在同一句里彼此分得开。
	//
	// ⚠️ 只加日志，**不改分派/落穿行为**：记完照旧往下走，未命中仍落到 `return nil`。
	// 义务 2 的后半（指向被丢弃节点的那条边「落穿停住、严禁打崩办理」）由 issues/143 在
	// java/php/c# 落地；本栈属「按 id 现查目标、查不到就停」那一派（followEdges:1152），
	// 行为已经对，本轮不动。
	if !isKnownNodeType(node.Type) {
		log.Printf("[jeeflow] WARNING 流程定义里的节点类型不在类型表里，该节点及其出边将被跳过"+
			"（令牌停在此处：不建行、不推进）: nodeId=%s type=%s", node.ID, node.Type)
	}
	// 任务创建（对齐 Java CreateTaskHandler：不触发节点拦截器——创建任务 ≠ 节点执行完成；
	// 任务完成的拦截器由 ExecuteProcessTask 显式触发，1.8.0 SYNC 同步演进）
	if node.Type == model.TypeTask {
		return e.createTask(ctx, flow, node, inst, operator, vars, parentTaskID)
	}
	// 记录类节点（snaker:custom）**不是任务类**：issues/142 A 批 · spec 02-flow-definition.md
	// §6.1／§6.2（owner 2026-09-29、09-30 两次拍板）。本栈此前与 task 同路走 createTask，
	// 落的是 §6.1 表里点名**禁止的形状①**「当任务类建 DOING 行」（仓内自述见
	// engine_test.go:907-908／927 那段注释）。现在分流到独立一支：执行 clazz →
	// 落一条 task_state=20 的历史行**并真落库** → 令牌沿出边继续流转 → **不 fire 码 3**。
	// 现成对照＝python 的 `_exec_custom_node`（jeeflow/engine.py:781）。
	if node.Type == model.TypeCustom {
		return e.execCustomNode(ctx, flow, node, inst, operator, vars, parentTaskID)
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
		if err := e.repo.UpdateInstance(ctx, inst); err != nil {
			return err
		}
		// PROCESS_INSTANCE_END(2)：实例 state 落库（20 办结 / 45 拒绝）之后 fire，
		// state 进载荷——§11.6「实例终态统一 2，办结/拒绝靠 state 分」
		e.fireEvent(ProcessEvent{Type: EventProcessInstanceEnd, InstanceID: inst.ID,
			NodeID: node.ID, Operator: operator, State: int(inst.State)})
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
	ct, _ := stringFromProps(node.Properties, "countersignType")
	now := time.Now()
	form := formKeyOf(node)
	// issues/126 案 A：节点到期表达式一次读好复用（未配 → ""，四处写点各自保持该列 NULL）
	expr := expireExprOf(node)
	pn := e.surrogateProcessName(flow, inst)
	// 建单不变量 ②（issues/121 P1）：判据沿用现成的 isFirstTaskNode（start 直接后继），一次算好复用
	isFirst := e.isFirstTaskNode(flow, node)

	// issues/142 A 批 · spec 02-flow-definition.md §6.2 第 3 条（owner 2026-09-30 拍）：
	// **任务类零参与者必须建单**。撤掉的旧形状是
	// `if len(actors) == 0 { return nil }`（原 :716-719）——那是 §6.1 点名 python 曾犯、
	// 本轮四栈跟改的**死锁黑洞**：实例停在 state=10、wf_process_task 里一行都没有，
	// 令牌也不前进，谁都办不动。新形状与 java `CreateTaskHandler`（:38-63 无条件建单）同形：
	// 建一条 task_state=10、参与者为**空集**的 DOING 行，靠加派参与者
	// （processTask/addCandidate、taskSurrogate）或 flow.auto/flow.admin 把这一格办起来。
	//
	// 三条不推歪实例状态机的判据（本轮实测过，见 engine/zero_actor_142_test.go）：
	//   - 零参与者行**不插 wf_process_task_actor**（空归属值是 issues/129／141 B 那族病根）；
	//   - 它是一条正常 DOING 行 ⇒ join／并行会签的"还有 doing 就没到齐"判据照旧成立，
	//     不会让令牌越过一格还没办完的节点（也不会被自动推进逻辑反复拾起——
	//     门面 startAndExecute 的自动办结只吃**发起那一次**的 doing 快照，见 facade.go:226-240，
	//     零参与者行不会自增出新行，无死循环）；
	//   - 会签节点解析到零参与者时**不逐人建单**（java PARALLEL 循环 0 次＝零行，仍是黑洞），
	//     统一落到下面这一条零参与者行；串行会签因此不会走到 actors[0]（旧代码在此会 panic）。
	if len(actors) == 0 {
		pt := 0
		if IsCountersign(node.Properties["performType"]) && ct != "" {
			pt = 1
		}
		nt := inst.CreateTaskWithActors(e.nextID(), node.ID, node.Text.Value, nil, operator, form, now, parentTaskID, isFirst, pt)
		applyExpireTime(nt, expr, inst.Variables)
		e.saveNewTask(ctx, nt, pn)
		// 码 3 照发：这条行的事实是"这一格产生了新的待办占位"（java 的 notifyTaskStart
		// 对 persistTasks 里每一条 saveNewTask 都播，不看参与者空不空）
		e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		return nil
	}

	// issue 42：performType 字符串兼容（'1'/'ALL'/'COUNTERSIGN' → 会签，对齐 Java codeOf）
	if IsCountersign(node.Properties["performType"]) && ct != "" {
		switch ct {
		case "PARALLEL":
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				// issues/126 案 A 写点：并行会签**全员**逐条按节点表达式算到期时间
				applyExpireTime(nt, expr, inst.Variables)
				e.saveNewTask(ctx, nt, pn)
				// TASK_CREATE：任务落库后逐个 fire（会签多任务逐个，对齐 Java CreateTaskHandler）
				e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
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
			// issues/126 案 A 写点：串行会签**首位成员**按节点表达式算到期时间
			applyExpireTime(nt, expr, inst.Variables)
			e.saveNewTask(ctx, nt, pn)
			e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		default:
			for _, actor := range actors {
				nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst, 1)
				// issues/126 案 A 写点：未知 countersignType 兜底分支也按逐人建单，同样要算
				applyExpireTime(nt, expr, inst.Variables)
				e.saveNewTask(ctx, nt, pn)
				e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
			}
		}
		return nil
	}
	// 普通任务：一个任务，全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actors[0], operator, form, now, parentTaskID, isFirst)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	// issues/126 案 A 写点：普通建单（引擎主建单入口）按节点表达式算到期时间；
	// 节点没配 ⇒ 这一列保持 NULL（不写 now()、不写 ''、不写 0）
	applyExpireTime(nt, expr, inst.Variables)
	// issues/116：委托代理在**参与者落库前**并入（见 engine/surrogate.go），随任务一起落库
	e.saveNewTask(ctx, nt, pn)
	e.fireEvent(ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
	return nil
}

// execCustomNode 记录类节点（`snaker:custom`／带 `properties.clazz` 的自定义节点）执行腿
// ——issues/142 A 批 · spec 02-flow-definition.md §6.2 三条硬要求（owner 2026-09-30 逐条拍），
// 三条逐条落在这里：
//
//	① 历史行**必须真落库**：走 `repo.SaveTask` 那条 INSERT 通道，不是只 append 进聚合根 tasks。
//	   ⚠️ 这一条**故意不照抄基准自身**——java `CustomModel.exec` 丢弃 `createHistoryTask` 的返回值
//	   只 append 进 `instance.tasks`，而 `persistTasks` 只保存 `exec.getProcessTaskList()`、
//	   `updateInstance` 的级联又只 UPDATE 已有行 ⇒ 那条 DONE 行永远进不了库
//	   （issues/142「A 段两条基准自身的事实」1，java/c# 本轮各自补 INSERT 腿）。
//	   现成对照＝python `engine.py:836 await self.repo.save_task(ht)`。
//	② `clazz` 解析不了 ⇒ **记 WARNING ＋ 照常落历史行 ＋ 令牌继续流转，严禁报错打断建单**
//	   （与 spec/04"节点属性配错不该把流程炸掉"是同一条哲学）。"未注册"与"clazz 为空串"
//	   **分档给不同文案**（§6.2 第 2 条要求可分别诊断；c# 现在把两者合成同一个异常、
//	   覆盖面比 java 宽，本栈不跟）。处理器**自身**返回 error 不在豁免内 ⇒ 原样外抛，
//	   历史行也不落（java 的 exec 顺序就是"反射/调用 → createHistoryTask → runOutTransition"，
//	   调用炸了后面两步都不发生）。
//	③ 记录类腿**不解析参与者**（"参与者为空"这一判据对它不适用）：不 resolveActors、
//	   不建待办、**不 fire 码 3**（`TASK_START` 表达"新待办产生"，记录类不该有；
//	   对照 java：notifyTaskStart 只对 exec.getProcessTaskList() 那批 DOING 单播）。
//	   留痕主体＝当前操作人（java `createHistoryTask` 的 `singletonList(operator)` 同形）。
//
// `clazz` 的解析形状：go 没有 java 的反射语义可依托（夹具 `flows/08-custom-node.json:44`
// 里那个 `com.mldong.jeeflow.test.TestCustomHandler` 是 JVM 类名），故与 **C#／python 同策**
// 走**按名注册表**——挂在既有扩展点 `engine.HandlerRegistry` 上（`RegisterCustom`/`ResolveCustom`），
// 不另造并行注册中心。返回值按 java 的形状写进流程变量：键＝节点 `properties.val`，
// 缺省 `custom_return_val`（`KeyCustomReturnVal`，对齐 java `CustomModel.java:46-48` ＋
// `FlowConst.CUSTOM_RETURN_VAL`）。
func (e *EngineImpl) execCustomNode(ctx context.Context, flow *model.FlowModel, node *model.FlowNode,
	inst *model.ProcessInstance, operator string, vars map[string]interface{}, parentTaskID int64) error {
	// 只认字符串形态：`stringFromProps` 对非字符串会 fmt.Sprint，`"clazz": null` 会读成 "<nil>"
	// 那种假名字（issues/142 B 表同款形状），这里按"没配 clazz"处理。
	clazz, _ := node.Properties["clazz"].(string)
	clazz = strings.TrimSpace(clazz)
	varKey, _ := node.Properties["val"].(string)
	varKey = strings.TrimSpace(varKey)
	if varKey == "" {
		varKey = KeyCustomReturnVal
	}

	var handler ICustomHandler
	if clazz == "" {
		log.Printf("[jeeflow] WARNING custom 节点 nodeId=%s 未配置 clazz ⇒ 不执行处理器，"+
			"只落历史行并继续流转（spec 02 §6.2 第 2 条：属性配错不炸流程）", node.ID)
	} else if e.registry == nil {
		log.Printf("[jeeflow] WARNING custom 节点 nodeId=%s clazz=%s 无法解析：引擎未装配 "+
			"HandlerRegistry（engine.NewHandlerRegistry() + SetRegistry 注册）⇒ 落历史行后继续流转",
			node.ID, clazz)
	} else if handler = e.registry.ResolveCustom(clazz); handler == nil {
		log.Printf("[jeeflow] WARNING custom 节点 nodeId=%s clazz=%s 未注册处理器 ⇒ 落历史行后"+
			"继续流转（注册入口 engine.HandlerRegistry.RegisterCustom(clazz, ICustomHandler)）",
			node.ID, clazz)
	}
	if handler != nil {
		ret, err := handler.Handle(node, inst, operator, vars)
		if err != nil {
			// §6.2 第 2 条尾注：处理器**自身执行失败**不在豁免内，照旧外抛（业务错误 ≠ 配错形状）
			return err
		}
		if ret != nil {
			// 写进执行变量（java `execution.getArgs().put(var, returnValue)` 同形）
			if vars != nil {
				vars[varKey] = ret
			}
			// 并同步落一份到实例变量＋UPDATE 实例行。两条理由：
			//   - 发起路径 `StartProcessInstanceByID` 只在建实例时 SaveInstance、之后没有
			//     updateInstance（java 那边发起收尾有 `repository.updateInstance(instance)`，
			//     JeeflowEngineImpl.java:113）⇒ 不补这一次，SQL 仓里这条变量永远查不到，
			//     而内存仓因为 map 是同一对象引用反而"看得见"——两仓给不同答案就是跨仓分叉；
			//   - 流程停在 custom 之后的任务节点时（不再有 End 节点那次合并）这是唯一让返回值
			//     进入流程变量的落点。
			// 排在 SaveTask(ht) 之后：jdbc 的 updateInstance 级联对任务只 UPDATE 不 INSERT，
			// 历史行得先有自己的 INSERT。
			if inst.Variables == nil {
				inst.Variables = map[string]interface{}{}
			}
			inst.Variables[varKey] = ret
			if err := e.repo.UpdateInstance(ctx, inst); err != nil {
				return err
			}
		}
	}

	ht := inst.CreateHistoryTask(e.nextID(), node.ID, node.Text.Value, operator, time.Now(),
		parentTaskID, e.isFirstTaskNode(flow, node))
	// 真落库（①）：INSERT 走 SaveTask 这条通道，与建待办同一条路
	if err := e.repo.SaveTask(ctx, ht); err != nil {
		return err
	}

	// 令牌沿出边继续流转（java CustomModel 收尾那句 runOutTransition）
	for _, n := range followEdges(flow, node.ID) {
		if err := e.executeNode(ctx, flow, inst, n, operator, vars, parentTaskID); err != nil {
			return err
		}
	}
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

// valueToActors 把变量值转参与者列表：**输出即归一后的集合**。
//
// split=true 时 String 按逗号分割（tf_nextNodeOperator 语义）；否则 String 原样单个（assignee 命中语义）。
//
// issues/142 B 批（spec 06-facade.md §2.11 写点表 f_nextNodeOperator／tf_nextNodeOperator 那一行
// ·owner 2026-09-30 拍「两形同判据」）：逗号串与数组两种形态**同判据**，判据只有
// [spi.NormalizeActors] 那一枚（逐元素 trim、空串/纯空白丢弃、同次调用折叠、落库与比较取 trim
// 后的值）；本函数只负责**形态拆解**（把变量值摊成逐元素候选串）。改前三处都从这一支漏过
// （普查底稿 issues/142 §2 B 表 go 那一行）：
//   - 数组里的 nil／整个 nil 入参被 fmt.Sprintf 成字面量 "<nil>" 落进归属列 ⇒ 现在与空串同档丢弃；
//   - []string 与 []interface{} 两支都不 trim ⇒ " 123 " 与 "123" 判成两个人；
//   - 空串元素照收 ⇒ 正是 issues/129 那族"空归属值读全库"的进水口。
//
// 数字元素照旧字符串化（fmt.Sprint 之后**仍走同一枚归一**，两端空格一样剥掉），不得被静默丢弃；
// ⚠️ 反向哨兵："0" 是有效参与者，判据只吃空值。
func valueToActors(v interface{}, split bool) []string {
	return spi.NormalizeActors(actorValueToRawStrings(v, split)...)
}

// actorValueToRawStrings 形态拆解（**不做判据**）：变量值 → 逐元素候选串。
// 判据一律落在 [spi.NormalizeActors]，这里再写一份就是"两份尺子迟早分叉"（spec §2.11 结尾点名）。
func actorValueToRawStrings(v interface{}, split bool) []string {
	switch t := v.(type) {
	case nil:
		return nil // nil 入参＝没有这一档，不产出候选串（旧形状在这里 Sprintf 出 "<nil>"）
	case string:
		if split {
			return strings.Split(t, ",")
		}
		return []string{t}
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, s := range t {
			if s == nil {
				continue // 数组里的 nil 元素就是"空元素"，与空串同档，绝不串化成 "<nil>"
			}
			if str, ok := s.(string); ok {
				out = append(out, str)
				continue
			}
			out = append(out, fmt.Sprintf("%v", s))
		}
		return out
	default:
		return []string{fmt.Sprintf("%v", t)}
	}
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
