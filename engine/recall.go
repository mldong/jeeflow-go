// 已办撤回（issues/246，规范 04 · 已办撤回）——解析、窗口判据、账本与复活腿（go 腿）。
//
// 基准＝jeeflow-java 参考实现 90469a2：
//   - core/RecallSupport.java（窗口判据＋账本，纯函数）
//   - core/JeeflowEngineImpl.recallTask（判据顺序＋落库顺序＋fire）
//   - domain/ProcessInstance.recallTask（下游废弃＋新行复活）
//   - parser/ModelParser（会签节点／申请节点两条解析期禁配）
//   - parser/impl/TaskParser（开关解析，非法值解析期报错）
//
// 契约正文＝jeeflow-doc spec `04-engine-ops.md`「已办撤回」＋ `06-facade.md` §2.7（tf_recallHistory）
// ＋ `11-events.md` 码 10。对外文案五条逐字跨栈统一（本文件常量即钉子，测试另有自证格）。
//
// 语义一句话：办理人收回**自己刚办结的那一手**，同实例内复活重办。窗口取宽口径（owner 拍）：
// 从该行沿 task_parent_id 血缘产出的全部下游行中**存在任何 FINISHED ⇒ 拦**；否则仍 DOING 的
// 下游行全部废弃（后加签/征询成员行、并行分支多支自然覆盖），同节点新建 DOING 行
// （参与者＝原行名册、parent＝原行 parent、expire_time 同尺），原 FINISHED 行**一律不改写**、
// 只盖 tf_recallHistory 追加账本；落库后 fire 码 10 TASK_RECALL。
//
// # go 与 java 的形状差（移植时逐条对过）
//
//   - java 的判据与写分离在 RecallSupport（纯函数）＋ JeeflowEngineImpl（落库序）；go 同构：
//     本文件纯函数家族 + `EngineImpl.RecallTask`（判据全排前、报错即零写入——内存仓没有回滚，
//     与后加签写入口/退回守卫同一规矩）。
//   - java 的行集合来自聚合根 `instance.getTasks()`；go 等价集合＝`repo.FindHistoryTasks`
//     （该实例全部任务行，含 DOING/FINISHED/99，内存与 jdbc 两仓同形状）。
//   - java 参考实现的复活行走 `repository.saveTask`（未过 saveNewTask/notifyTaskStart，码 3
//     实际未 fire——其 javadoc 注释写"复活行另走码 3"与代码不符）；go 按规范 04「fire 码 3
//     走既有建单腿」的字面契约走 `saveNewTask`＋码 3（委托并入/参与者落库/产出列表同一条腿），
//     差异点已记录在案（java 注释意图优先于其代码现状）。
//   - java 引擎无实例级互斥；go 沿仓内既有并发口径对同一实例串行化（lockInstanceByTask），
//     「判窗口 → 废弃 → 建行」在同一临界区里（spec 04 并发与事务同款声明）。
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

const (
	// PropAllowRecall 节点属性：已办撤回开关（properties 平铺层，缺省 false）。
	// 会签节点／申请节点（start 直接后继）解析期禁配（validateAddSignFlow 同点拦）。
	PropAllowRecall = "allowRecall"
	// KeyRecallHistory 协议键：追加式账本，每条 {operator, time, recalledTaskId}，落在**被撤回的
	// 旧行**变量上（原行不改写＝历史即审批记录，账本是唯一追加痕迹）；recalledTaskId 指向复活新行
	// （从历史记录可一步跳到唯一活待办）。tf_ 前缀使 lineageVars（退回复活腿）天然剔掉它。
	KeyRecallHistory = "tf_recallHistory"
	// KeyAllowRecall 已办撤回节点开关快照（issues/251）：办理时由引擎从节点属性盖到本行
	// 变量，值＝办理时刻的 allowRecall 定义值；查询出口搬运成行顶层 allowRecall 供前端
	// 预判置灰（只投已办行）。撤回校验仍以现读定义为权威，快照仅供预判（漂移为设计内）。
	KeyAllowRecall = "tf_allowRecall"

	// MsgRecallWindow 窗口判据：下游已办结 ⇒ 拦（宽口径的反面档）。
	MsgRecallWindow = "任务[%s]的后续节点已办理，不能撤回"
	// MsgRecallSwitchOff 节点未开开关 ⇒ 拦（静默放行＝伪造撤回能力）。
	MsgRecallSwitchOff = "节点[%s]未开启 allowRecall，不能撤回"
	// MsgRecallNotFinished 目标行非 FINISHED（不存在／进行中／已废弃同档）。
	MsgRecallNotFinished = "任务非已完成状态，不可撤回"
	// MsgRecallNoPermission 非参与者且非特权。
	MsgRecallNoPermission = "无权限撤回该任务"
	// MsgRecallInstanceNotDoing 实例非进行中。
	MsgRecallInstanceNotDoing = "流程实例非进行中状态，不可撤回"
)

// ─── 窗口判据（纯函数，判据前置＝报错即零写入的前半） ────────────────────────

// recallDescendants 按 task_parent_id 血缘收集 target 的全部后代行（BFS）。
// 血缘在建单不变量里必写（0＝无父），故"有后代"与"无后代"都由数据现状决定。
func recallDescendants(tasks []*model.ProcessTask, ancestorID int64) []*model.ProcessTask {
	var result []*model.ProcessTask
	frontier := []int64{ancestorID}
	for len(frontier) > 0 {
		parent := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		for _, t := range tasks {
			if t.ParentTaskID != nil && *t.ParentTaskID == parent {
				result = append(result, t)
				frontier = append(frontier, t.ID)
			}
		}
	}
	return result
}

// validateRecallWindow 窗口判据（owner 2026-11-03 拍宽口径）：后代行中**存在任何 FINISHED** ⇒
// 报错；否则返回其中仍 DOING 的行（供废弃——后加签/征询成员行、并行分支多支都自然覆盖；
// 已废弃(99)/已撤回(30) 行既不拦也不重复废弃）。
func validateRecallWindow(tasks []*model.ProcessTask, target *model.ProcessTask) ([]*model.ProcessTask, error) {
	var doing []*model.ProcessTask
	for _, t := range recallDescendants(tasks, target.ID) {
		if t.IsFinished() {
			return nil, fmt.Errorf(MsgRecallWindow, target.TaskName)
		}
		if t.IsDoing() {
			doing = append(doing, t)
		}
	}
	return doing, nil
}

// readStringMapList 账本读取：内存里是 []map[string]interface{}，经仓储 JSON 往返是 []interface{}——
// 两形都收（与 readAddSignHistory 同一条读取腿的形状）。
func readStringMapList(v interface{}) []map[string]interface{} {
	switch t := v.(type) {
	case []map[string]interface{}:
		return t
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(t))
		for _, item := range t {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// appendRecallHistory 账本追加（只追加不覆盖），落**被撤回的旧行**变量；
// recalledTaskId 指向复活的新行。时间一律全生态格式串（spec §2.4，与 tf_addSignHistory 同尺）。
func appendRecallHistory(target *model.ProcessTask, operator string, resurrectedTaskID int64, now time.Time) {
	if target == nil {
		return
	}
	if target.Variables == nil {
		target.Variables = map[string]interface{}{}
	}
	history := append(readStringMapList(target.Variables[KeyRecallHistory]), map[string]interface{}{
		"operator":       operator,
		"time":           now.Format(addSignTimeLayout),
		"recalledTaskId": resurrectedTaskID,
	})
	target.Variables[KeyRecallHistory] = history
}

// ─── 引擎入口（判据全排前 → 落库序照 java → fire 码 10） ─────────────────────

// RecallTask 已办撤回（issues/246，spec 04 · 已办撤回）：办理人撤回自己已办结的那一手，
// 同实例内复活重办。
//
// 判据顺序照 java JeeflowEngineImpl.recallTask（逐条对过，顺序不可反）：
// 行存在且已办结 → 参与者/特权 → 实例 DOING → 节点开关 → 窗口（下游无任何 FINISHED）。
// 落库顺序：废弃行 updateTask → 复活行 saveNewTask（既有建单腿＋码 3）→ 原行盖
// tf_recallHistory updateTask → 实例 updateTime → fire 码 10 TASK_RECALL。
//
// 参与人判断**不走 isAllowed 族**：go 的 isAllowed 只认 DOING 行，FINISHED 行要按名册判断
// （ActorIDs 即名册——内存仓随行携带，jdbc 仓 FindTaskByID 从参与者表水合，两仓同源）；
// flow.auto / flow.admin 特权放行（与转办同口径）。
func (e *EngineImpl) RecallTask(ctx context.Context, taskID int64, operator string) (*model.ProcessTask, error) {
	unlock := e.lockInstanceByTask(taskID)
	defer unlock()
	var resurrected *model.ProcessTask
	err := e.InTx(ctx, func(ctx context.Context) error {
		var err error
		resurrected, err = e.recallTaskInner(ctx, taskID, operator)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resurrected, nil
}

func (e *EngineImpl) recallTaskInner(ctx context.Context, taskID int64, operator string) (*model.ProcessTask, error) {
	// 判据 1：目标行存在且已办结
	target, err := e.repo.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if target == nil || !target.IsFinished() {
		return nil, errors.New(MsgRecallNotFinished)
	}
	// 判据 2：参与者/特权（FINISHED 行按名册判断，不走只认 DOING 的 isAllowed）
	privileged := strings.EqualFold(operator, KeyAutoExecute) || strings.EqualFold(operator, KeyAdminID)
	if !privileged && !contains(target.ActorIDs, operator) {
		return nil, errors.New(MsgRecallNoPermission)
	}
	// 判据 3/4：实例存在且 DOING
	inst, err := e.repo.FindInstanceByID(ctx, target.ProcessInstanceID)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	if inst.State != model.InstanceStateDoing {
		return nil, errors.New(MsgRecallInstanceNotDoing)
	}
	// 判据 5：节点开关（模型解析期禁配已由 validateAddSignFlow 兜住，这里再判开关本身）
	def, err := e.repo.FindDefineByID(ctx, inst.DefineID)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, errors.New("流程定义不存在")
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return nil, fmt.Errorf("parse flow: %w", err)
	}
	if err := e.validateAddSignFlow(&flow); err != nil {
		return nil, err
	}
	node := findNode(&flow, target.TaskName)
	allow := false
	if node != nil && node.Type == model.TypeTask {
		allow, err = ParseBoolProp(node.Properties[PropAllowRecall], node.ID, PropAllowRecall)
		if err != nil {
			return nil, err
		}
	}
	if node == nil || node.Type != model.TypeTask || !allow {
		return nil, fmt.Errorf(MsgRecallSwitchOff, target.TaskName)
	}
	// 判据 6：窗口（下游任何 FINISHED ⇒ 拦；行集合＝该实例全部任务行）
	tasks, err := e.repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		return nil, err
	}
	doingDescendants, err := validateRecallWindow(tasks, target)
	if err != nil {
		return nil, err
	}
	// ── 判据全过，以下才允许写 ──
	now := time.Now()
	// 写 1：下游全部 DOING 行废弃（后加签/征询成员行、并行分支多支自然覆盖）
	for _, t := range doingDescendants {
		t.Abandon(now)
		if err := e.repo.UpdateTask(ctx, t); err != nil {
			return nil, err
		}
		syncTaskToAggregate(inst, t)
	}
	// 写 2：同节点新建 DOING 行（参与者＝原行名册、parent＝原行 parent、expire_time 同尺）
	var parentID int64
	if target.ParentTaskID != nil {
		parentID = *target.ParentTaskID
	}
	roster := append([]string{}, target.ActorIDs...)
	resurrected := inst.CreateTaskWithActors(e.nextID(), node.ID, target.DisplayName, roster,
		operator, formKeyOf(node), now, parentID, e.isFirstTaskNode(&flow, node))
	applyExpireTime(resurrected, expireExprOf(node), inst.Variables)
	// 复活行走既有建单腿（saveNewTask：委托并入＋参与者落库＋产出列表），落库后 fire 码 3
	if err := e.saveNewTask(ctx, resurrected, e.surrogateProcessName(ctx, &flow, inst)); err != nil {
		return nil, err
	}
	e.fireEvent(ctx, ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: resurrected.ID,
		NodeID: node.ID, Operator: operator, Actors: resurrected.ActorIDs})
	// 写 3：原 FINISHED 行不改写，只盖 tf_recallHistory 追加账本（recalledTaskId 指向复活行）
	appendRecallHistory(target, operator, resurrected.ID, now)
	if err := e.repo.UpdateTask(ctx, target); err != nil {
		return nil, err
	}
	syncTaskToAggregate(inst, target)
	inst.UpdateTime = now
	inst.UpdateUser = operator
	if err := e.saveInstance(ctx, inst); err != nil {
		return nil, err
	}
	// 码 10 TASK_RECALL：撤回事实落库之后 fire 一次（sourceId＝被撤旧行＝TaskID；
	// 载荷 instanceId/taskId/operator；只发 10 不补发 5/6）
	e.fireEvent(ctx, ProcessEvent{Type: EventTaskRecall, InstanceID: inst.ID, TaskID: target.ID,
		Operator: operator})
	return resurrected, nil
}

// StampAllowRecall 办理时把当前节点的 allowRecall 开关快照进行变量（issues/251）：
// 已办行置灰的"节点开关"预判从行上读，查询出口只搬运不解析定义。本栈实例变量全量写
// （CompleteTask p.Variables=vars），下游待办行会残留上一手键——与内置 Java/Python/
// Node 线同形状，出口 DOING 守卫统一无害化。节点属性经 ParseBoolProp 解析（与撤回
// 校验同一把尺子），解析异常保守 false（撤回时校验还会报原文案，快照只影响预判）。
// 非 task 节点（start/end/decision 等不办理的节点）与 nil args 均静默跳过。
func StampAllowRecall(args map[string]interface{}, node *model.FlowNode) {
	if args == nil || node == nil || node.Type != model.TypeTask {
		return
	}
	allow, err := ParseBoolProp(node.Properties[PropAllowRecall], node.ID, PropAllowRecall)
	if err != nil {
		allow = false
	}
	args[KeyAllowRecall] = allow
}
