// 动态协办（aidant，issues/253）——协办行识别、等待闸计数、投影、轮次与账本的纯函数家族
// ＋派生腿／推进闸／离开节点清场腿（go 腿）。
//
// 基准＝jeeflow-java 参考实现（issues/253 实施轮，工作树现读）：
//   - core/AidantSupport.java（判据/账本/投影，纯函数）
//   - core/JeeflowEngineImpl.aidantTask（派生）／completeAidantWithoutDrive（推进闸）
//     ／prepareExecution 里的 gateWait（等待闸，排在 completeTask 之前）
//   - model/TaskModel.abandonAidantOnLeave（离开节点清场，四档都调）
//   - handler/impl/CountersignHandler（会签合并判据显式排除协办行）
//   - facade/JeeflowFacade.taskAidant（门面只做归一与信封）＋detail 的 ext.aidant 投影
//
// 契约正文＝jeeflow-doc spec `04-engine-ops.md`「动态协办（aidant）」＋ `06-facade.md`
// §2.7 协议键／§processTask/aidant／文末失败文案表协办六行。本能力**无节点开关**
// （节点级 taskType=1 解析期禁配，issues/252）、不新增 submitType 档、无 DDL、
// 不新增事件码（协办行建/结走既有码 3/5）。
//
// 语义一句话：主办把"这活"交给别人干——按人派生 task_type=1 的协办子行，**主办行原样保留
// DOING**；协办行办结不驱动流程，节点推进仍由主办行办结触发。与征询的本质差异：征询收的是
// "意见"且强制等收齐；协办交的是"活"，等不等由派生参数 waitCompletion 决定（缺省 false＝不等）。
//
// # 与征询的分辨判据＝task_type（别简化）
//
// 两族子行都是 parent 指向主办行＋同节点同名：协办行 task_type=1、征询成员行 task_type=0。
// 等待闸只数协办行、征询收齐闸只数成员行，各数各的（spec 04「与征询允许同节点并存」）。
// 同名过滤同样不可省：主办办结推进后，下一节点行的 parent 也是主办行 id（建单不变量），
// 只认 parent 会把下游行误当协办行。
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

const (
	// KeyAidantHistory 动态协办账本（issues/253）：{round, fromActor, toActors, reason, time, operator}
	// 只追加，落**主办行**变量（形状照 tf_addSignHistory，键序锁定、time 为 §2.4 字符串）。
	// 离开节点清场未办结协办行时追加一条带 abandoned=true 的条目（照征询撤回的 revoked）。
	KeyAidantHistory = "tf_aidantHistory"
	// KeyAidantReason 派生原因（选填）：盖每条协办行（单行出证自足）；**不占 tf_approvalComment**。
	KeyAidantReason = "tf_aidantReason"
	// KeyAidantWait 等待闸开关：派生时把 waitCompletion 盖在**主办行**变量上，缺省 false＝不等。
	// tf_ 前缀使实例合并自动剥键（不传播，与 tf_allowRecall 同通道）；投影 ext.aidant.wait 同源。
	KeyAidantWait = "tf_aidantWait"
)

// 逐字判据文案（跨栈统一；spec 06 文末失败文案表「协办六行」＋等待闸那句）。
const (
	MsgAidantNoActors     = "actorIds 必填"
	MsgAidantNoPermission = "无权限派生协办任务"
	MsgAidantNotDoing     = "任务非进行中，不可派生协办"
	MsgAidantDuplicate    = "该人已有未办结的协办任务"
	MsgAidantNoRecursion  = "协办任务不能再派生协办"
	MsgAidantTaskNotFound = "任务不存在"
	MsgAidantWaitPending  = "节点[%s]协办未办结（已办结 %d/%d），不能办理"
)

// ─── 协办行识别（纯函数） ─────────────────────────────────────────────────────

// IsAidantRow 协办行判定＝task_type=1（SECONDARY）。这是两族子行唯一的分辨判据。
func IsAidantRow(t *model.ProcessTask) bool {
	return t != nil && t.TaskType == int(model.TaskTypeSecondary)
}

// AidantRows 协办行＝parent 指向主办行**且同节点同名且 task_type=1** 的行。
//
// 同名过滤不可省（照 CollectMemberRows 的理由）：主办办结推进后，下一节点行的 parent 同样是
// 主办行 id（建单不变量），只认 parent 会把下游行误当协办行。task_type 过滤同样不可省：
// 征询成员行也满足 parent＋同名，混进来会让等待闸把"意见未收齐"误报成"协办未办结"。
func AidantRows(tasks []*model.ProcessTask, host *model.ProcessTask) []*model.ProcessTask {
	var out []*model.ProcessTask
	if host == nil || host.ID == 0 {
		return out
	}
	for _, t := range tasks {
		if IsAidantRow(t) && t.ParentTaskID != nil && *t.ParentTaskID == host.ID &&
			t.TaskName == host.TaskName {
			out = append(out, t)
		}
	}
	return out
}

// CountAidant 等待闸原料：在册协办行总数与已办结（FINISHED）数——已废弃(99) 行
// （离开节点清场的）不算在册（spec 04「计数跳过已废弃(99) 行」）。返回 (total, submitted)。
func CountAidant(tasks []*model.ProcessTask, host *model.ProcessTask) (int, int) {
	total, submitted := 0, 0
	for _, t := range AidantRows(tasks, host) {
		if t.TaskState == model.TaskStateAbandoned {
			continue
		}
		total++
		if t.IsFinished() {
			submitted++
		}
	}
	return total, submitted
}

// PendingAidant 未办结协办行（DOING）——等待闸的判据集合，也是离开节点时的清场集合。
func PendingAidant(tasks []*model.ProcessTask, host *model.ProcessTask) []*model.ProcessTask {
	var out []*model.ProcessTask
	for _, t := range AidantRows(tasks, host) {
		if t.IsDoing() {
			out = append(out, t)
		}
	}
	return out
}

// AidantWaitOf 等待闸开关（waitCompletion）：派生时盖在**主办行**变量上，缺省 false＝不等。
// 多轮派生时后一次的值生效（投影 ext.aidant.wait 同源）。非法值按缺省 false
// （照 java Boolean.parseBoolean：只认 "true"，大小写不敏感；不新造文案）。
func AidantWaitOf(host *model.ProcessTask) bool {
	if host == nil || host.Variables == nil {
		return false
	}
	v, ok := host.Variables[KeyAidantWait]
	if !ok || v == nil {
		return false
	}
	if b, isBool := v.(bool); isBool {
		return b
	}
	return strings.EqualFold(fmt.Sprintf("%v", v), "true")
}

// GateAidantWait 等待闸（spec 04「推进闸与等待闸」）：waitCompletion=true 时主办行办结前判
// "不存在 DOING 协办行"，未齐 ⇒ 逐字文案报错、零写入。协办行自身不受本闸约束
// （它办结本来就不驱动流程）；waitCompletion=false（缺省）原样放行。
//
// 必须由引擎排在 CompleteTask 之前——内存仓储无回滚，判据后置时"报错"那一次调用结束后
// 主办待办已经消失（与后加签/退回守卫同一规矩）。
func GateAidantWait(tasks []*model.ProcessTask, host *model.ProcessTask) error {
	if host == nil || IsAidantRow(host) || !AidantWaitOf(host) {
		return nil
	}
	if len(PendingAidant(tasks, host)) == 0 {
		return nil
	}
	total, submitted := CountAidant(tasks, host)
	return fmt.Errorf(MsgAidantWaitPending, host.TaskName, submitted, total)
}

// FilterOutAidant 从行集合里剔掉协办行（task_type=1）——会签合并计数与"本节点还有兄弟待办"
// 判据共用这一枚（spec 04「不计入会签合并计数」＋「缺省 false 主办可随时推进」）。
func FilterOutAidant(tasks []*model.ProcessTask) []*model.ProcessTask {
	out := make([]*model.ProcessTask, 0, len(tasks))
	for _, t := range tasks {
		if !IsAidantRow(t) {
			out = append(out, t)
		}
	}
	return out
}

// ─── 轮次与账本 ───────────────────────────────────────────────────────────────

// AidantHistoryOf 读账本（测试与投影用）。内存里是 []map[string]interface{}，
// 经仓储 JSON 往返是 []interface{}——两形都收（照 readAddSignHistory 同一条读取腿）。
func AidantHistoryOf(task *model.ProcessTask) []map[string]interface{} {
	if task == nil || task.Variables == nil {
		return nil
	}
	switch v := task.Variables[KeyAidantHistory].(type) {
	case []map[string]interface{}:
		return v
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// AidantLastRound 主办行当前轮次＝账本末条 round（每次派生追加一条＝轮次推进；
// 清场记账不消耗轮次）。
func AidantLastRound(host *model.ProcessTask) int {
	history := AidantHistoryOf(host)
	for i := len(history) - 1; i >= 0; i-- {
		if n := toIntOf(history[i]["round"]); n > 0 {
			return n
		}
	}
	return 0
}

// AppendAidantHistory 派生账本追加（只追加不覆盖），落**主办行**变量；六键
// {round, fromActor, toActors, reason, time, operator}（spec 06 §2.7），reason 空串兜底、
// time 一律 §2.4 字符串。
//
// 主办行在派生时不办结，CompleteTask 的变量合并是并入而非整体替换，故账本能活到主办办结之后
// （与转办/征询账本同一条存活理由）。⚠️ 调用方随后必须把主办行再落一次库（快照仓储纪律）。
func AppendAidantHistory(host *model.ProcessTask, operator string, toActors []string,
	round int, reason string, now time.Time) {
	aidantAppend(host, aidantBaseEntry(operator, toActors, round, reason, now))
}

// AppendAidantAbandonHistory 清场账本追加：abandoned=true 标记本条是"离开节点废弃"
// （形状照征询撤回条目的 revoked=true），六键原序不动，toActors＝被废弃的人。
func AppendAidantAbandonHistory(host *model.ProcessTask, operator string, abandonedActors []interface{},
	round int, now time.Time) {
	entry := aidantBaseEntry(operator, nil, round, "", now)
	entry["toActors"] = append([]interface{}{}, abandonedActors...)
	entry["abandoned"] = true
	aidantAppend(host, entry)
}

// aidantBaseEntry 账本条目六键（round/fromActor/toActors/reason/time/operator）。
// Go map 无序——键序由 JSON 出口的字典序承载，与 java LinkedHashMap 的插入序属结构层差异
// （本仓 CollectBuildProjection / collectBaseEntry 同款，语义层零自创）。
func aidantBaseEntry(operator string, toActors []string, round int, reason string, now time.Time) map[string]interface{} {
	return map[string]interface{}{
		"round":     round,
		"fromActor": operator,
		"toActors":  append([]string{}, toActors...),
		"reason":    reason,
		"time":      now.Format(addSignTimeLayout),
		"operator":  operator,
	}
}

func aidantAppend(host *model.ProcessTask, entry map[string]interface{}) {
	if host == nil {
		return
	}
	if host.Variables == nil {
		host.Variables = map[string]interface{}{}
	}
	host.Variables[KeyAidantHistory] = append(AidantHistoryOf(host), entry)
}

// AidantBuildProjection 投影（spec 06 §2.7）：主办行 ext.aidant
// ＝{active, round, total, submitted, actors:[{actor, taskId, state}], wait}；无协办行 ⇒ nil。
// 从 parent＋task_type 现算、不落新存储，跳过 99 废弃行（清场的行不进投影，与征询投影同口径）。
func AidantBuildProjection(tasks []*model.ProcessTask, host *model.ProcessTask) map[string]interface{} {
	rows := AidantRows(tasks, host)
	if len(rows) == 0 {
		return nil
	}
	total, submitted := 0, 0
	actors := make([]map[string]interface{}, 0, len(rows))
	for _, t := range rows {
		if t.TaskState == model.TaskStateAbandoned {
			continue
		}
		total++
		if t.IsFinished() {
			submitted++
		}
		var actor interface{}
		if len(t.ActorIDs) > 0 {
			actor = t.ActorIDs[0]
		}
		actors = append(actors, map[string]interface{}{
			"actor":  actor,
			"taskId": t.ID,
			"state":  t.TaskState,
		})
	}
	return map[string]interface{}{
		"active":    submitted < total,
		"round":     AidantLastRound(host),
		"total":     total,
		"submitted": submitted,
		"actors":    actors,
		"wait":      AidantWaitOf(host),
	}
}

// ─── 离开节点清场腿 ───────────────────────────────────────────────────────────

// AbandonAidantOnNode 离开节点时清场未办结协办行（spec 04：waitCompletion=false 时主办可随时
// 推进，未办结协办行随废弃腿清场、账本记废弃，不留孤儿待办）。
//
// 只对协办行（task_type=1）动手，且**不要求节点开任何开关**——协办无节点开关，普通节点上的
// 协办行同样要清（这是与 abandonAddSignOnNode 的关键差别，后者有 allowAddSign/dynamicCountersign
// 双开关守卫）。按 parent 分组，废弃事实追加到各自主办行的账本上。
// 幂等：已废弃(99)/已办结(20) 的行不再命中，重复调用不重复记账。返回实际废弃的行数。
func (e *EngineImpl) AbandonAidantOnNode(ctx context.Context, inst *model.ProcessInstance,
	nodeName, operator string) (int, error) {
	if inst == nil || nodeName == "" {
		return 0, nil
	}
	// parent → 该主办行下待清场的协办行（order 锁定记账顺序＝行序，照 java LinkedHashMap）
	byHost := map[int64][]*model.ProcessTask{}
	var order []int64
	for _, t := range inst.Tasks {
		if t.TaskName == nodeName && t.IsDoing() && IsAidantRow(t) {
			var p int64
			if t.ParentTaskID != nil {
				p = *t.ParentTaskID
			}
			if _, ok := byHost[p]; !ok {
				order = append(order, p)
			}
			byHost[p] = append(byHost[p], t)
		}
	}
	now := time.Now()
	abandoned := 0
	for _, p := range order {
		rows := byHost[p]
		actors := make([]interface{}, 0, len(rows))
		for _, t := range rows {
			t.Abandon(now)
			t.UpdateUser = operator // java abandon(operator) 同尺：update_user 回写真实清场人
			if err := e.repo.UpdateTask(ctx, t); err != nil {
				return abandoned, err
			}
			syncTaskToAggregate(inst, t)
			if len(t.ActorIDs) > 0 {
				actors = append(actors, t.ActorIDs[0])
			} else {
				actors = append(actors, nil)
			}
			abandoned++
		}
		var host *model.ProcessTask
		for _, t := range inst.Tasks {
			if t.ID == p {
				host = t
				break
			}
		}
		if host != nil {
			AppendAidantAbandonHistory(host, operator, actors, AidantLastRound(host), now)
			if err := e.repo.UpdateTask(ctx, host); err != nil {
				return abandoned, err
			}
			syncTaskToAggregate(inst, host)
		}
	}
	return abandoned, nil
}

// ─── 派生腿（判据全排前，报错即零写入） ───────────────────────────────────────

// AidantTask 动态协办（issues/253，spec 04 · 动态协办）：主办把"这活"交给别人干——按人派生
// task_type=1 协办子行，**主办行原样保留 DOING**，节点推进仍由主办行办结触发。
//
// 判据全部排在任何状态变更之前（报错即零写入——与后加签/征询/撤回同一规矩）：
// 任务存在 → 禁递归（协办行不能再派生）→ 主办行 DOING → actorIds 非空 → 名册鉴权
// （auto/admin 例外）→ 逐人去重（该人已有本主办行下未办结协办行）。
// 落库顺序：协办行 saveNewTask（委托并入 → SaveTask 分配 id → fire 码 3）→ 主办行盖 wait
// 开关＋账本 UpdateTask。
//
// 建单形状照征询成员行腿（inst.CreateTaskWithActors：同节点同名、parent＝主办行、
// expire_time 按节点表达式同尺重算），另落 task_type=1／performType=NORMAL——节点上配的
// performType 可能是会签，协办行一律普通参与（spec 04 派生规则 1）。
func (e *EngineImpl) AidantTask(ctx context.Context, taskID int64, actorIds []string,
	reason string, waitCompletion bool, operator string) ([]*model.ProcessTask, error) {
	unlock := e.lockInstanceByTask(taskID)
	defer unlock()
	var created []*model.ProcessTask
	err := e.InTx(ctx, func(ctx context.Context) error {
		var err error
		created, err = e.aidantTaskInner(ctx, taskID, actorIds, reason, waitCompletion, operator)
		return err
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (e *EngineImpl) aidantTaskInner(ctx context.Context, taskID int64, actorIds []string,
	reason string, waitCompletion bool, operator string) ([]*model.ProcessTask, error) {
	host, err := e.repo.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if host == nil {
		return nil, errors.New(MsgAidantTaskNotFound)
	}
	// 禁递归：协办行携带派生参数 ⇒ 报错（spec 04 派生规则 4）
	if IsAidantRow(host) {
		return nil, errors.New(MsgAidantNoRecursion)
	}
	if !host.IsDoing() {
		return nil, errors.New(MsgAidantNotDoing)
	}
	actors := spi.NormalizeActors(actorIds...)
	if len(actors) == 0 {
		return nil, errors.New(MsgAidantNoActors)
	}
	privileged := strings.EqualFold(operator, KeyAutoExecute) || strings.EqualFold(operator, KeyAdminID)
	// 以参与者表为判据（聚合副本可能滞后于加签/转办的增量写入，与 transfer 同源）
	roster, err := e.repo.FindTaskActors(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !privileged && !contains(roster, operator) {
		return nil, errors.New(MsgAidantNoPermission)
	}
	inst, err := e.repo.FindInstanceByID(ctx, host.ProcessInstanceID)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	// issues/249 同一课：账本必须盖在聚合内的同一对象上——FindTaskByID 在 JDBC 仓是独立副本，
	// 后续 UpdateInstance 的级联会拿聚合里没账本的旧副本抹掉它。这里把 host 重指到聚合内那份。
	for _, t := range inst.Tasks {
		if t.ID == taskID {
			host = t
			break
		}
	}
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
	node := findNode(&flow, host.TaskName)
	if node == nil || node.Type != model.TypeTask {
		return nil, errors.New(MsgAidantNotDoing)
	}
	// 去重：该人已有本主办行下**未办结**协办行 ⇒ 报错（spec 04 派生规则 5）
	existing := AidantRows(inst.Tasks, host)
	for _, actor := range actors {
		for _, t := range existing {
			if t.IsDoing() && contains(t.ActorIDs, actor) {
				return nil, errors.New(MsgAidantDuplicate)
			}
		}
	}
	// ── 判据全过，以下才允许写 ──
	now := time.Now()
	round := AidantLastRound(host) + 1
	isFirst := e.isFirstTaskNode(&flow, node)
	pn := e.surrogateProcessName(ctx, &flow, inst)
	form := formKeyOf(node)
	expr := expireExprOf(node)
	created := make([]*model.ProcessTask, 0, len(actors))
	for _, actor := range actors {
		nt := inst.CreateTaskWithActors(e.nextID(), node.ID, node.Text.Value, []string{actor},
			operator, form, now, host.ID, isFirst)
		nt.TaskType = int(model.TaskTypeSecondary)
		nt.PerformType = int(model.PerformTypeNormal)
		if reason != "" {
			putTaskVars(nt, map[string]interface{}{KeyAidantReason: reason})
		}
		applyExpireTime(nt, expr, inst.Variables)
		// 分支归属与每一次普通建单同一条腿（照征询成员行）：并行范围内的协办行继承父任务归属
		e.attachBranchCtx(&flow, inst, nt, node.ID, host.ID)
		if err := e.saveNewTask(ctx, nt, pn); err != nil {
			return nil, err
		}
		e.fireEvent(ctx, ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID,
			NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		created = append(created, nt)
	}
	// 等待闸开关＋账本落主办行（主办不办结，变量合并序保证账本活到办结后）
	putTaskVars(host, map[string]interface{}{KeyAidantWait: waitCompletion})
	AppendAidantHistory(host, operator, actors, round, reason, now)
	// ⚠️ 严禁覆写任务 actor_id/operator 列（转办那条 ⚠️ 同尺）：进行中任务该列恒无值是家族不变量，
	// 派生协办不改主办行的办理人归属，审计由 update_user＋账本 operator 承载。
	host.UpdateUser = operator
	host.UpdateTime = now
	if err := e.repo.UpdateTask(ctx, host); err != nil {
		return nil, err
	}
	syncTaskToAggregate(inst, host)
	return created, nil
}

// completeAidantWithoutDrive 协办行办结的**推进闸**（issues/253，spec 04「推进闸与等待闸」；
// 参照 snaker SnakerEngineImpl.execute 的「协办任务完成不产生执行对象」）：只完成任务行
// （prepareExecuteTask 内含 CompleteTask／UpdateTask／fire 码 5 或 6），**不跑节点执行、
// 不跑输出变迁**，本次动作产出列表为空（没有 saveNewTask 调用 ⇒ sink 空）。
//
// 四入口同闸：ExecuteProcessTask／ExecuteAndJumpTask／ExecuteAndJumpToEnd／
// ExecuteAndJumpToFirstTaskNode——协办行任何 submitType 都不得驱动流程。
func (e *EngineImpl) completeAidantWithoutDrive(ctx context.Context, taskID int64,
	operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	_, inst, _, _, err := e.prepareExecuteTask(ctx, taskID, operator, args, nil)
	if err != nil {
		return nil, err
	}
	return inst, nil
}