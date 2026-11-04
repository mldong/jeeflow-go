// 动态会签（issues/247）与已办撤回（issues/246）的门面腿——逐字照 java 参考实现
// JeeflowFacade 的 taskRecall / tryDynamicCountersign / gateDynamicCountersign /
// taskRevokeCountersign 四段（ffe2749＋90469a2）。
//
// 分层与 java 同构：判据/账本/成员行识别等纯函数在引擎包（engine/recall.go、engine/collect.go），
// execute 分流/闸门/撤回在门面（java 就是这么分的——发起腿是门面分流而不是引擎执行闸，
// 因为"带人提交不办结主办行"必须发生在 CompleteTask 之前且不进 execute 参数空间语义）。
// 判据全部排在任何状态变更之前——"报错即零写入"与后加签写入口同一规矩。
package facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
)

// taskRecall 已办撤回（issues/246，spec 06 §processTask/recall）：办理人撤回自己已办结的那一手，
// 同实例内复活重办。判据与落库全在引擎（engine.RecallTask），门面只做参数归一
// （operator 硬必填、主键档缺失响亮报错，绝不拿 ''/0 当 id 落库）与信封包装。
// 位序照 java taskRecall：operator 空判在前、主键缺失判在后。
func (f *Facade) taskRecall(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	taskID, _ := toInt64(args["processTaskId"])
	operator := strings.TrimSpace(toStr(args["operator"], ""))
	if operator == "" {
		return nil, errors.New("operator 必填")
	}
	if taskID == 0 {
		return nil, errors.New("processTaskId 缺失")
	}
	resurrected, err := f.engine.RecallTask(ctx, taskID, operator)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"processTaskId": strconv.FormatInt(resurrected.ID, 10),
		"taskState":     resurrected.TaskState,
	}
	return data, nil
}

// tryDynamicCountersign 动态会签发起分流（issues/247，spec 04 · 动态会签）：征询节点带
// tf_addSignOperators 的提交＝只建成员行、**不办结主办行**；返回响应 marker
// （data.dynamicCountersign），非征询提交返回 nil 走普通流转。判据全排前
// （互斥/参与人/加给自己/重复/上限），报错即零写入。
func (f *Facade) tryDynamicCountersign(ctx context.Context, taskID int64, operator string,
	flowArgs map[string]interface{}) (map[string]interface{}, error) {
	host, err := f.repo.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if host == nil || !host.IsDoing() {
		return nil, nil
	}
	ops := engine.OperatorsFromArgs(flowArgs)
	if len(ops) == 0 {
		return nil, nil
	}
	inst, err := f.repo.FindInstanceByID(ctx, host.ProcessInstanceID)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, nil
	}
	def, err := f.repo.FindDefineByID(ctx, inst.DefineID)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, nil
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return nil, err
	}
	node := findNodeIn(&flow, host.TaskName)
	if node == nil || node.Type != model.TypeTask {
		return nil, nil
	}
	collectOn, err := engine.ParseBoolProp(node.Properties[engine.PropDynamicCountersign], node.ID, engine.PropDynamicCountersign)
	if err != nil {
		return nil, err
	}
	if !collectOn {
		return nil, nil
	}
	tasks, err := f.repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		return nil, err
	}
	// 成员行不能冒充主办发起（拉人只限主办行）——放行给闸门腿报它的逐字文案
	if engine.IsCollectMemberRow(tasks, host) {
		return nil, nil
	}
	// 互斥（与 tf_nextNodeOperator 同传＝死参数，报错不静默）
	if v, ok := flowArgs[engine.KeyNextNodeOperator]; ok && v != nil &&
		strings.TrimSpace(fmt.Sprintf("%v", v)) != "" {
		return nil, errors.New(engine.MsgAddSignMutex)
	}
	// 操作人是主办参与人（host 已 DOING；java host.isAllowed＝DOING＋名册＋auto/admin 特权，
	// 文案取 WfErrEnum.NOT_ALLOWED_EXECUTE 逐字）
	if !isPrivilegedOperator(operator) && !containsStr(host.ActorIDs, operator) {
		return nil, errors.New(msgNotAllowedExecute)
	}
	round := engine.CollectLastRound(host) + 1
	maxRounds, err := engine.ParseAddSignMaxRounds(node.Properties[engine.PropAddSignMaxRounds], node.ID)
	if err != nil {
		return nil, err
	}
	if maxRounds > 0 && round > maxRounds {
		return nil, fmt.Errorf(engine.MsgAddSignMaxRounds, node.ID, maxRounds)
	}
	reason := toStr(flowArgs[engine.KeyAddSignReason], "")
	existing := engine.CollectMemberRows(tasks, host)
	for _, op := range ops {
		if op == operator {
			return nil, errors.New(engine.MsgAddSignSelf)
		}
		for _, m := range existing {
			if m.IsDoing() && containsStr(m.ActorIDs, op) {
				return nil, fmt.Errorf(engine.MsgCollectDuplicate, op, node.ID)
			}
		}
	}
	// ── 判据全过：建成员行（同节点同名、parent＝主办行、轮次/原因/分支归属三戳）＋
	// 落库逐行 fire 码 3；主办行盖轮次戳＋追加账本后 updateTask（未办结＝征询挂起）──
	if _, err := f.engine.CreateCollectMemberRows(ctx, &flow, inst, node, host, operator, ops, round, reason); err != nil {
		return nil, err
	}
	engine.CollectAppendLaunchHistory(host, operator, ops, round, reason, time.Now())
	if host.Variables == nil {
		host.Variables = map[string]interface{}{}
	}
	host.Variables[engine.AddSignRoundKey(node.ID)] = round
	if err := f.repo.UpdateTask(ctx, host); err != nil {
		return nil, err
	}
	tasks, err = f.repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		return nil, err
	}
	total, submitted := engine.CollectMemberCounts(tasks, host)
	marker := map[string]interface{}{
		"dynamicCountersign": map[string]interface{}{
			"round":     round,
			"total":     total,
			"submitted": submitted,
		},
	}
	return marker, nil
}

// gateDynamicCountersign 动态会签闸门（issues/247）：征询节点上——主办行未收齐成员意见 ⇒
// 逐字文案报错（**挡全部 submitType，含驳回**；唯一出口＝processTask/revokeDynamicCountersign）；
// 成员行携带拉人参数 ⇒ 报错（拉人只限主办行）。非征询节点原样放行。
func (f *Facade) gateDynamicCountersign(ctx context.Context, taskID int64, flowArgs map[string]interface{}) error {
	task, err := f.repo.FindTaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if task == nil || !task.IsDoing() {
		return nil
	}
	inst, err := f.repo.FindInstanceByID(ctx, task.ProcessInstanceID)
	if err != nil {
		return err
	}
	if inst == nil {
		return nil
	}
	def, err := f.repo.FindDefineByID(ctx, inst.DefineID)
	if err != nil {
		return err
	}
	if def == nil {
		return nil
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return err
	}
	node := findNodeIn(&flow, task.TaskName)
	if node == nil || node.Type != model.TypeTask {
		return nil
	}
	collectOn, err := engine.ParseBoolProp(node.Properties[engine.PropDynamicCountersign], node.ID, engine.PropDynamicCountersign)
	if err != nil {
		return err
	}
	if !collectOn {
		return nil
	}
	tasks, err := f.repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		return err
	}
	if engine.IsCollectMemberRow(tasks, task) {
		if len(engine.OperatorsFromArgs(flowArgs)) > 0 {
			return errors.New(engine.MsgCollectMemberPull)
		}
		return nil
	}
	// 收齐判据＝还有无 DOING 成员行（宽口径计数跳过撤回清场的 99 行）
	if len(engine.CollectPendingMembers(tasks, task)) > 0 {
		total, submitted := engine.CollectMemberCounts(tasks, task)
		return fmt.Errorf(engine.MsgCollectPending, task.TaskName, submitted, total)
	}
	return nil
}

// taskRevokeCountersign 撤回动态会签（issues/247，spec 06 §processTask/revokeDynamicCountersign）：
// 废弃主办行下全部未答成员行，账本追加撤回条目；已交意见行原样保留；闸门自然恢复
// （判据＝还有无 DOING 成员行）；轮次不消耗。独立 action，不进 execute 参数空间
// （撤回不是办结动作）。位序照 java taskRevokeCountersign：operator 空判在前、主键缺失判在后。
func (f *Facade) taskRevokeCountersign(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	taskID, _ := toInt64(args["processTaskId"])
	operator := strings.TrimSpace(toStr(args["operator"], ""))
	if operator == "" {
		return nil, errors.New("operator 必填")
	}
	if taskID == 0 {
		return nil, errors.New("processTaskId 缺失")
	}
	reason := toStr(args["reason"], "")
	host, err := f.repo.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if host == nil {
		return nil, errors.New("任务不存在")
	}
	// 参与人判断不能用 isAllowed 族（只认 DOING 行）——主办已办结时按名册判断，待答判据随后兜底
	if !isPrivilegedOperator(operator) && !containsStr(host.ActorIDs, operator) {
		return nil, errors.New(engine.MsgCollectNoPermissionRevoke)
	}
	inst, err := f.repo.FindInstanceByID(ctx, host.ProcessInstanceID)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	def, err := f.repo.FindDefineByID(ctx, inst.DefineID)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, errors.New("流程定义不存在")
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return nil, err
	}
	node := findNodeIn(&flow, host.TaskName)
	if node == nil || node.Type != model.TypeTask {
		return nil, fmt.Errorf(engine.MsgCollectNoPending, host.TaskName)
	}
	collectOn, err := engine.ParseBoolProp(node.Properties[engine.PropDynamicCountersign], node.ID, engine.PropDynamicCountersign)
	if err != nil {
		return nil, err
	}
	if !collectOn {
		return nil, fmt.Errorf(engine.MsgCollectNoPending, host.TaskName)
	}
	tasks, err := f.repo.FindHistoryTasks(ctx, inst.ID)
	if err != nil {
		return nil, err
	}
	pending := engine.CollectPendingMembers(tasks, host)
	if len(pending) == 0 {
		return nil, fmt.Errorf(engine.MsgCollectNoPending, host.TaskName)
	}
	now := time.Now()
	revokedActors := make([]interface{}, 0, len(pending))
	for _, m := range pending {
		m.Abandon(now)
		m.UpdateUser = operator // java abandon(operator) 同尺：update_user 回写真实撤回人
		if len(m.ActorIDs) > 0 {
			revokedActors = append(revokedActors, m.ActorIDs[0])
		} else {
			revokedActors = append(revokedActors, nil)
		}
		if err := f.repo.UpdateTask(ctx, m); err != nil {
			return nil, err
		}
	}
	engine.CollectAppendRevokeHistory(host, operator, revokedActors, engine.CollectLastRound(host), reason, now)
	if err := f.repo.UpdateTask(ctx, host); err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"revoked": len(revokedActors),
		"actors":  revokedActors,
	}
	return data, nil
}

// isPrivilegedOperator 系统代执行（flow.auto）/ 超级管理员（flow.admin）放行——
// isAllowed 既有约定，撤回/转办/征询共用。
func isPrivilegedOperator(operator string) bool {
	return strings.EqualFold(operator, engine.KeyAutoExecute) || strings.EqualFold(operator, engine.KeyAdminID)
}

// msgNotAllowedExecute java WfErrEnum.NOT_ALLOWED_EXECUTE（20010004）逐字文案——
// 本栈 error 无码位，出口由门面统一 code=99999999 + msg 逐字。
const msgNotAllowedExecute = "当前参与者不能执行该流程任务"
