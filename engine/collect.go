// 动态会签（征询模式 dynamicCountersign，issues/247）——成员行识别、闸门计数、投影、
// 轮次与账本的纯函数家族＋建成员行腿（go 腿）。
//
// 基准＝jeeflow-java 参考实现 ffe2749：
//   - core/CollectSupport.java（成员行/闸门计数/投影/账本，纯函数）
//   - facade/JeeflowFacade 的 tryDynamicCountersign／gateDynamicCountersign／taskRevokeCountersign
//     （go 侧对应腿在 facade 包，见 facade/collect.go——java 放门面的分流/闸门/撤回照旧放门面）
//   - model/TaskModel.exec 第三档（征询档推进闸＝复用 AddSignHandler，go 侧落在
//     executeProcessTaskInner 的推进闸位，与后加签档并列）
//
// 契约正文＝jeeflow-doc spec `04-engine-ops.md`「动态会签（征询模式）」＋ `06-facade.md` §2.7
// ＋ `02-flow-definition.md` §4.4（三条解析期禁配）。本能力不新增节点类型、不新增 submitType 档、
// 无 DDL、不新增事件码（成员行建/结走既有码 3/5）、成员意见无表决语义。
//
// 语义一句话：主办提交带 tf_addSignOperators 时**不办结主办行**，按人建成员行收意见；
// 全部成员提交后闸门打开，主办再按普通办理（同意/不同意）决定走向。与后加签的本质差异：
// 后加签主办在拉人时即办结、接力自动推进；征询模式主办挂起、**拍板权始终在主办**。
//
// # 成员行识别（Java 实测踩过的坑，别简化）
//
// 成员行＝parent 指向主办行**且同节点同名**的行。同名过滤不可省：主办办结推进后，下一节点行的
// parent 同样是主办行 id（建单不变量）——只认 parent 会把下游行误当成员，撤回腿会误废弃它们。
package engine

import (
	"context"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

const (
	// PropDynamicCountersign 节点属性：动态会签（征询模式）开关（properties 平铺层，缺省 false）。
	// 与 allowAddSign 互斥、会签节点/申请节点禁配——解析期报错（validateAddSignFlow 同点拦）。
	PropDynamicCountersign = "dynamicCountersign"
	// KeyAddSignReason 协议键（选填）：本次征询的原因，盖每条成员行＋账本条目
	// （单行出证自足，不回查主办行）；选填不写键。
	KeyAddSignReason = "tf_addSignReason"

	// 五条逐字判据文案（跨栈统一；spec 06 文末失败文案表）。
	MsgCollectPending          = "节点[%s]动态会签意见未收齐（已提交 %d/%d），不能办理"
	MsgCollectDuplicate        = "操作人[%s]已在节点[%s]的动态会签中"
	MsgCollectMemberPull       = "动态会签成员任务不能发起加签"
	MsgCollectNoPending        = "节点[%s]没有进行中的动态会签"
	MsgCollectNoPermissionRevoke = "无权限撤回该动态会签"
)

// ─── 成员行识别（纯函数） ─────────────────────────────────────────────────────

// CollectMemberRows 成员行＝parent 指向主办行**且同节点同名**的行（成员不能再拉人 ⇒ 无更深层级）。
// issues/253：协办行（task_type=1）也满足 parent＋同名，但它属另一族——征询收齐闸只数征询
// 成员行（task_type=0），两闸各数各的（spec 04「与征询允许同节点并存」）。
func CollectMemberRows(tasks []*model.ProcessTask, host *model.ProcessTask) []*model.ProcessTask {
	var out []*model.ProcessTask
	if host == nil {
		return out
	}
	for _, t := range tasks {
		if host.ID != 0 && t.ParentTaskID != nil && *t.ParentTaskID == host.ID &&
			t.TaskName == host.TaskName && !IsAidantRow(t) {
			out = append(out, t)
		}
	}
	return out
}

// IsCollectMemberRow 成员行判定：parent 指向**同节点同名**行（成员行禁带拉人参数的判据）。
// issues/253：协办行（task_type=1）不是征询成员行——两族靠 task_type 分辨。
func IsCollectMemberRow(tasks []*model.ProcessTask, row *model.ProcessTask) bool {
	if row == nil || row.ParentTaskID == nil || *row.ParentTaskID == 0 {
		return false
	}
	if IsAidantRow(row) {
		return false
	}
	for _, t := range tasks {
		if t.ID == *row.ParentTaskID && t.TaskName == row.TaskName {
			return true
		}
	}
	return false
}

// CollectPendingMembers 未答成员行（DOING），撤回接口的废弃集合。
func CollectPendingMembers(tasks []*model.ProcessTask, host *model.ProcessTask) []*model.ProcessTask {
	var out []*model.ProcessTask
	for _, t := range CollectMemberRows(tasks, host) {
		if t.IsDoing() {
			out = append(out, t)
		}
	}
	return out
}

// CollectMemberCounts 闸门原料：在册成员行总数与已提交数——已废弃(99) 行（撤回腿清场的）
// 不算在册。返回 (total, submitted)。
func CollectMemberCounts(tasks []*model.ProcessTask, host *model.ProcessTask) (int, int) {
	total, submitted := 0, 0
	for _, t := range CollectMemberRows(tasks, host) {
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

// CollectBuildProjection 投影（spec 06 §2.7）：征询主办行 ext.dynamicCountersign
// ＝{active, round, total, submitted, actors:[{actor, taskId, state}]}；无成员行 ⇒ nil。
// 从成员行现算，跳过 99 废弃行（撤回清场的行不进投影）。
func CollectBuildProjection(tasks []*model.ProcessTask, host *model.ProcessTask) map[string]interface{} {
	members := CollectMemberRows(tasks, host)
	if len(members) == 0 {
		return nil
	}
	total, submitted := 0, 0
	actors := make([]map[string]interface{}, 0, len(members))
	for _, m := range members {
		if m.TaskState == model.TaskStateAbandoned {
			continue
		}
		total++
		if m.IsFinished() {
			submitted++
		}
		var actor interface{}
		if len(m.ActorIDs) > 0 {
			actor = m.ActorIDs[0]
		}
		actors = append(actors, map[string]interface{}{
			"actor":  actor,
			"taskId": m.ID,
			"state":  m.TaskState,
		})
	}
	return map[string]interface{}{
		"active":    submitted < total,
		"round":     CollectLastRound(host),
		"total":     total,
		"submitted": submitted,
		"actors":    actors,
	}
}

// ─── 轮次与账本 ───────────────────────────────────────────────────────────────

// CollectLastRound 主办行当前轮次＝账本条数（每次拉人追加一条＝轮次推进；撤回不消耗）。
func CollectLastRound(host *model.ProcessTask) int {
	history := readAddSignHistory(host)
	if len(history) == 0 {
		return 0
	}
	return toIntOf(history[len(history)-1]["round"])
}

// CollectAppendLaunchHistory 发起会签账本追加（只追加不覆盖），落**主办行**
// （征询模式下主办行未办结，variables.putAll(args) 的合并序保证账本活到办结后）；
// 每条含 reason（选填，空串不写键）。
func CollectAppendLaunchHistory(host *model.ProcessTask, operator string, toActors []string,
	round int, reason string, now time.Time) {
	collectAppendHistory(host, collectBaseEntry(operator, toActors, round, now), reason)
}

// CollectAppendRevokeHistory 撤回会签账本追加：revoked=true 标记本条是撤回（未答废弃），
// toActors＝被撤的人；轮次不消耗（本条不推进轮次，round 记当轮）。
func CollectAppendRevokeHistory(host *model.ProcessTask, operator string, revokedActors []interface{},
	round int, reason string, now time.Time) {
	entry := collectBaseEntry(operator, nil, round, now)
	entry["toActors"] = append([]interface{}{}, revokedActors...)
	entry["revoked"] = true
	collectAppendHistory(host, entry, reason)
}

// collectBaseEntry 账本条目五键固定 camelCase（round/fromActor/toActors/time/operator），
// 形状照 tf_addSignHistory 既有条目（后加签 appendAddSignHistory 同族）。
func collectBaseEntry(operator string, toActors []string, round int, now time.Time) map[string]interface{} {
	return map[string]interface{}{
		"round":     round,
		"fromActor": operator,
		"toActors":  append([]string{}, toActors...),
		"time":      now.Format(addSignTimeLayout),
		"operator":  operator,
	}
}

// collectAppendHistory 落账本（共享 KeyAddSignHistory 那一份追加式账本；reason 选填）。
func collectAppendHistory(host *model.ProcessTask, entry map[string]interface{}, reason string) {
	if host == nil {
		return
	}
	if reason != "" {
		entry["reason"] = reason
	}
	if host.Variables == nil {
		host.Variables = map[string]interface{}{}
	}
	host.Variables[KeyAddSignHistory] = append(readAddSignHistory(host), entry)
}

// ─── 建成员行腿（判据已由门面前置，本腿只负责建与落） ─────────────────────────

// CreateCollectMemberRows 建征询成员行并逐行落库＋fire 码 3（java tryDynamicCountersign
// 的建单段：逐人一行 saveTask＋PROCESS_TASK_START）。每人一行：同节点同名、parent＝主办行、
// 轮次/reason 两戳、分支归属照后加签腿（attachBranchCtx）、到期时间同尺、委托照既有腿并入
// （saveNewTask）。判据（开关/参与人/加给自己/重复/上限/互斥）由门面在调用前拦下——
// 报错即零写入由调用方保证，本腿不再重复判。
func (e *EngineImpl) CreateCollectMemberRows(ctx context.Context, flow *model.FlowModel,
	inst *model.ProcessInstance, node *model.FlowNode, host *model.ProcessTask,
	operator string, ops []string, round int, reason string) ([]*model.ProcessTask, error) {
	now := time.Now()
	form := formKeyOf(node)
	expr := expireExprOf(node)
	// 征询节点不可能是首任务节点（解析期已拦），沿用现成判据算，不写死 false——与后加签腿同形
	isFirst := e.isFirstTaskNode(flow, node)
	pn := e.surrogateProcessName(ctx, flow, inst)
	created := make([]*model.ProcessTask, 0, len(ops))
	for _, actor := range ops {
		nt := inst.CreateTaskWithActors(e.nextID(), node.ID, node.Text.Value, []string{actor},
			operator, form, now, host.ID, isFirst)
		putTaskVars(nt, map[string]interface{}{prefixKey(VarAddSignRound, node.ID): round})
		if reason != "" {
			putTaskVars(nt, map[string]interface{}{KeyAddSignReason: reason})
		}
		applyExpireTime(nt, expr, inst.Variables)
		// 分支归属与每一次普通建单同一条腿（并行范围内的成员行必须继承父任务归属，
		// 否则汇合判据读不到这一支）——照后加签建单腿。
		e.attachBranchCtx(flow, inst, nt, node.ID, host.ID)
		if err := e.saveNewTask(ctx, nt, pn); err != nil {
			return nil, err
		}
		e.fireEvent(ctx, ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID,
			NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
		created = append(created, nt)
	}
	return created, nil
}
