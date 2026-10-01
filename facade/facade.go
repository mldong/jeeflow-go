// 统一门面（v1.1.0）——"接口即 POST + JSON body"风格的单入口
//
// 集成方只实现一个转发 controller/endpoint：把 body JSON 转成 map 传入 Flow，
// 所有流程能力按 action（boot2/boot3 端点短名）路由。返回统一结构 {code, msg, data}
// （code=0 成功 / 99999999 失败）。
//
// 操作人约定：门面不感知登录态，args["operator"] 显式传入。
//
// ⚠️ id 传参约定（issues/38 E9 对齐 Node）：id 类参数（processDefineId/processTaskId/...）
// 建议以**字符串**传递。集成方若用 encoding/json 把请求体解析为 map，数字默认变 float64
// （53 位尾数），Java 雪花 id（>2^53）在解析层就已丢精度——门面对超 2^53 的 float64
// 显性报错（不静默截断），字符串路径 strconv.ParseInt 精确无损。
package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// UserSearch 用户搜索钩子（v1.2.0，可选）——candidatePage 无模型候选时的用户分页搜索。
// 返回 (rows, total, err)；query 透传 pageNum/pageSize/搜索条件。
type UserSearch func(query map[string]interface{}) ([]map[string]interface{}, int, error)

// Facade 统一门面
type Facade struct {
	engine     *engine.EngineImpl
	repo       spi.ProcessRepository
	extRepo    spi.ProcessExtRepository // 可空：未接入时设计/委托 action 报错
	userSearch UserSearch               // 可空：candidatePage 用户搜索依赖
	orgProv    spi.OrgUserProvider      // 可空：candidatePage candidateGroups 角色取人（v1.6.0）
	metaReader interface {
		ReadByProcessInstance(tableName string, processInstanceID interface{}) (interface{}, error)
	}
}

// SetMetaReader 注入业务数据读取器（issue 30）：需有 ReadByProcessInstance(tableName, processInstanceID)
func (f *Facade) SetMetaReader(reader interface {
	ReadByProcessInstance(tableName string, processInstanceID interface{}) (interface{}, error)
}) *Facade {
	f.metaReader = reader
	return f
}

// SetUserSearch 注入用户搜索钩子
func (f *Facade) SetUserSearch(fn UserSearch) *Facade {
	f.userSearch = fn
	return f
}

// SetOrgUserProvider 注入组织用户提供者（candidatePage candidateGroups 角色取人）
func (f *Facade) SetOrgUserProvider(orgProv spi.OrgUserProvider) *Facade {
	f.orgProv = orgProv
	return f
}

// New 构造门面。
//
// issues/116：ext 非空时一并注入引擎，使「委托代理自动生效」默认可用
// （引擎内置、默认开启；集成方关闭走 engine.WithSurrogateAutoApply(false) /
// eng.SetSurrogateAutoApply(false)，或传 nil 让引擎静默跳过）。
func New(e *engine.EngineImpl, repo spi.ProcessRepository, ext spi.ProcessExtRepository) *Facade {
	if e != nil && ext != nil && e.SurrogateRepository() == nil {
		e.SetSurrogateRepository(ext)
	}
	return &Facade{engine: e, repo: repo, extRepo: ext}
}

// Flow 统一入口（action 清单见规范 06-facade；事件契约见规范 11-events）
func (f *Facade) Flow(action string, args map[string]interface{}) (r map[string]interface{}) {
	// 扩展仓储未配置等内部 panic 收敛为业务错误。
	// issues/137 §3-1（spec 06-facade.md §2.12）：收敛 ≠ 原文外透 —— panic 值同样要过判别式
	// （旧形状 errorResult(fmt.Sprintf("%v", p)) 会把 nil 解引用的
	// 「runtime error: invalid memory address or nil pointer dereference」直接糊到用户脸上）。
	// 门面自己写的那句「未配置 ProcessExtRepository（扩展仓储）」是引擎契约文案，照旧逐字透出。
	defer func() {
		if p := recover(); p != nil {
			r = errorResult(outwardPanicMsg(action, p))
		}
	}()
	if args == nil {
		args = map[string]interface{}{}
	}
	var err error
	var data interface{}
	switch action {
	case "processDefine/page":
		data, err = f.definePage(args)
	case "processDefine/detail":
		data, err = f.defineDetail(args)
	case "processDefine/startAndExecute":
		data, err = f.startAndExecute(args)
	case "processDefine/deploy":
		data, err = f.deploy(args)
	case "processDefine/redeploy":
		err = f.redeploy(args)
	case "processDefine/remove":
		err = f.removeDefine(args)
	case "processDefine/upAndDown":
		err = f.upAndDown(args)
	case "processInstance/page":
		data, err = f.instancePage(args)
	case "processInstance/detail":
		data, err = f.instanceDetail(args)
	case "processInstance/startAndExecute":
		data, err = f.startAndExecute(args)
	case "processInstance/withdraw":
		err = f.withdraw(args)
	case "processTask/execute":
		err = f.execute(args)
	case "processTask/todoList":
		data, err = f.todoList(args)
	case "processTask/doneList":
		data, err = f.doneList(args)
	case "processDesign/page":
		data, err = f.designPage(args)
	case "processDesign/detail":
		data, err = f.designDetail(args)
	case "processDesign/save":
		data, err = f.designSave(args)
	case "processDesign/update":
		err = f.designUpdate(args)
	case "processDesign/updateDefine":
		err = f.designUpdateDefine(args)
	case "processDesign/remove":
		err = f.designRemove(args)
	case "processDesign/deploy":
		data, err = f.designDeploy(args)
	case "processDesign/redeploy":
		data, err = f.designRedeploy(args)
	case "processDesign/listByType":
		data, err = f.designListByType(args)
	case "processInstance/bizData":
		data, err = f.bizData(args)
	case "processSurrogate/page":
		data, err = f.surrogatePage(args)
	case "processSurrogate/save":
		data, err = f.surrogateSave(args)
	case "processSurrogate/update":
		data, err = f.surrogateUpdate(args) // issues/77
	case "processSurrogate/detail":
		data, err = f.surrogateDetail(args) // issues/77
	case "processSurrogate/remove":
		err = f.surrogateRemove(args)
	case "processDefine/getLastByName":
		data, err = f.getLastByName(args)
	case "processInstance/highLight":
		data, err = f.highLight(args)
	case "processInstance/approvalRecord":
		data, err = f.approvalRecord(args)
	case "processInstance/getAssigneeTextData":
		data, err = f.getAssigneeTextData(args)
	case "processInstance/createCCInstance":
		err = f.createCCInstance(args)
	case "processInstance/updateCCStatus":
		err = f.updateCCStatus(args)
	case "processInstance/ccList":
		data, err = f.ccList(args)
	case "processTask/detail":
		data, err = f.taskDetail(args)
	case "processTask/jumpAbleTaskNameList":
		data, err = f.jumpAbleTaskNameList(args)
	case "processTask/candidatePage":
		data, err = f.candidatePage(args)
	case "processTask/surrogate":
		err = f.taskAddActor(args)
	case "processTask/addCandidate":
		err = f.taskAddActor(args)
	case "processTask/transfer":
		err = f.taskTransfer(args)
	case "processTask/removeTaskActor":
		err = f.taskRemoveActor(args)
	case "processTask/latest":
		data, err = f.taskLatest(args)
	case "processInstance/stats/overview":
		data, err = f.statsOverview(args)
	case "processInstance/stats/trend":
		data, err = f.statsTrend(args)
	case "processInstance/stats/group":
		data, err = f.statsGroup(args)
	default:
		return errorResult("未知 action: " + action)
	}
	if err != nil {
		// issues/137 §3-1（spec 06-facade.md §2.12）：门面是所有内部 error 的共性通道，
		// 但"捕获到 ≠ 原文外透"。判别式（isForeignDetail，见 internal_failure.go）判
		// 「这段文案是谁写的」：引擎自己写的契约文案逐字透出（八栈＋十三壳＋前端 toast 都按原文对齐，
		// 收窄就是静默改契约面），运行时/解析器/驱动/第三方 provider 写的原文换成固定文案
		// msgInternalFailure，原文只进日志与错误链（判据与副作用各自可测）。
		return errorResult(outwardFailureMsg(action, err))
	}
	// issues/38 E9 出口统一：id 类字段转 string（对齐 Node 全程 string / Java 集成层
	// Jackson ToStringSerializer）——前端 JS number 无法承载雪花 id（>2^53）
	return okResult(stringifyIDs(data))
}

// ═══ 流程定义 / 实例 ═══

func (f *Facade) startAndExecute(args map[string]interface{}) (interface{}, error) {
	defineID, err := toInt64(args["processDefineId"])
	if err != nil {
		return nil, fmt.Errorf("processDefineId 缺失或非法: %v", err)
	}
	operator := operatorArg(args)
	flowArgs := map[string]interface{}{}
	for k, v := range args {
		if k == "processDefineId" || k == "operator" {
			continue
		}
		flowArgs[k] = v
	}
	inst, err := f.engine.StartProcessInstanceByID(context.Background(), defineID, operator, flowArgs)
	if err != nil {
		return nil, err
	}
	// issues/56 E28 / spec 11-events §11.7：发起时抄送（f_ccActors）。
	// 与办理腿（tf_ccActors）、手动腿（createCCInstance）共用引擎侧单一漏斗 HandleCcActors：
	// 建 wf_process_cc_instance 行 → **落库后**逐抄送人 fire CC_CREATE（基准＝Java handleCcActors
	// → notifyCcCreate，ccActorId 直传事件体）。不带 f_ccActors ⇒ 零写入零 fire，行为与历史一致。
	if err := f.engine.HandleCcActors(context.Background(), inst.ID, operator, flowArgs[engine.KeyCcActorsStart]); err != nil {
		return nil, err
	}
	// startAndExecute：自动完成申请节点（assignee="applicant" → 发起人）
	doing, err := f.repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil {
		return nil, err
	}
	for _, task := range doing {
		_ = f.repo.AddTaskActor(context.Background(), task.ID, []string{operator})
		flowArgs["submitType"] = 0 // APPLY
		// 对齐 boot3：f_nextNodeOperator（发起时预指派人）→ tf_nextNodeOperator（引擎执行参数）
		if v, ok := flowArgs[engine.KeyProcessStartNextNodeOperator]; ok && fmt.Sprintf("%v", v) != "" {
			flowArgs[engine.KeyNextNodeOperator] = v
		}
		if _, err := f.engine.ExecuteProcessTask(context.Background(), task.ID, operator, flowArgs); err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{"processInstanceId": inst.ID}, nil
}

// deploy 版本管理（对齐 boot3）：按 name 查最新定义，存在 version+1 插新记录，否则从 0 起
func (f *Facade) deploy(args map[string]interface{}) (interface{}, error) {
	content, err := contentBytes(args)
	if err != nil {
		return nil, err
	}
	var flow model.FlowModel
	if err := json.Unmarshal(content, &flow); err != nil {
		// issues/139：对外 msg 为固定文案，原始异常只留在错误对象链（Unwrap），不进 msg
		return nil, fixedMsgError(msgReadFlowDefineJSONFailed, err)
	}
	if flow.Name == "" {
		return nil, errors.New("流程定义缺少 name")
	}
	version := 0
	latest, err := f.repo.FindDefineByName(context.Background(), flow.Name)
	if err != nil {
		return nil, err
	}
	if latest != nil {
		version = latest.Version + 1
	}
	def := &model.ProcessDefine{
		Name:        flow.Name,
		DisplayName: flow.DisplayName,
		Type:        flow.Type,
		State:       1,
		Content:     content,
		Version:     version,
		CreateUser:  toStr(args["operator"], "system"),
		UpdateUser:  toStr(args["operator"], "system"),
	}
	if err := f.repo.SaveDefine(context.Background(), def); err != nil {
		return nil, err
	}
	return map[string]interface{}{"processDefineId": def.ID}, nil
}

func (f *Facade) redeploy(args map[string]interface{}) error {
	defineID, err := toInt64(args["processDefineId"])
	if err != nil {
		return fmt.Errorf("processDefineId 缺失或非法: %v", err)
	}
	content, err := contentBytes(args)
	if err != nil {
		return err
	}
	var flow model.FlowModel
	if err := json.Unmarshal(content, &flow); err != nil {
		// issues/139：对外 msg 为固定文案，原始异常只留在错误对象链（Unwrap），不进 msg
		return fixedMsgError(msgReadFlowDefineJSONFailed, err)
	}
	return f.repo.UpdateDefine(context.Background(), &model.ProcessDefine{
		ID:          defineID,
		Name:        flow.Name,
		DisplayName: flow.DisplayName,
		Type:        flow.Type,
		Content:     content,
		UpdateUser:  toStr(args["operator"], "system"),
	})
}

func (f *Facade) removeDefine(args map[string]interface{}) error {
	// issues/28：兼容 {ids} 批量（boot3 前端 IdsParam 惯例）与单 {id}
	ids, err := idListArgs(args)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := f.repo.RemoveDefine(context.Background(), id); err != nil {
			return err
		}
	}
	return nil
}

func (f *Facade) upAndDown(args map[string]interface{}) error {
	// issues/28：兼容 {ids, opType} 批量；opType/state 二选一
	state, err := toInt(firstNonNil(args["opType"], args["state"]))
	if err != nil {
		return fmt.Errorf("opType/state 缺失或非法: %v", err)
	}
	ids, err := idListArgs(args)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := f.repo.UpdateDefineState(context.Background(), id, state); err != nil {
			return err
		}
	}
	return nil
}

// withdraw 撤回（spec 06 §processInstance/withdraw，issues/114）
//
// operator 硬必填：缺失/空串直接报错，**严禁缺省回落 user1**——那会把撤回人静默记成别人，
// 审计链失真且不报错。归属三判据命中任一放行，全不命中拒绝（失败码统一 99999999，细粒度原因走 msg）。
//
// issues/134 案 A：归属判据之后、任何状态改写之前还有**实例状态守卫**——
// 实例 state != 10(进行中) 一律拒（20 已办结 / 40 已终止 / 30 已撤回 / 45、50、99 同拒），
// 被拒时实例与任务行一行都不改、不落库。落点选在本函数：聚合根 model.ProcessInstance.Withdraw
// 是无 error 返回的纯 setter，状态改写序列（doing 任务 → 实例 → UpdateInstance 级联落库）
// 全在门面里，守卫排在它之前才拦得住。次序与 Java 一致（facade 鉴权 → 聚合根 withdraw 抛错）：
// 无关第三人撤已办结单先撞归属判据、拿「无权限撤回该流程实例」，不把实例状态递给无权限的人。
func (f *Facade) withdraw(args map[string]interface{}) error {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return fmt.Errorf("id 缺失或非法: %v", err)
	}
	operator := strings.TrimSpace(toStr(args["operator"], ""))
	if operator == "" {
		return errors.New("operator 必填")
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), instanceID)
	if err != nil || inst == nil {
		return errors.New("流程实例不存在")
	}
	// 撤回：全部 doing 任务置 Withdraw(30) + 实例置 30（v1.0.1：updateInstance 级联落库）
	// 注意：FindInstanceByID 现水合 Tasks（issues/110），此处仍按实例单独查 doing 任务撤回，
	// 且必须把聚合副本重置为仅被撤回项（见下方 inst.Tasks = doing），防级联回写多余任务
	// ——20（已完成）/40（已终止）的任务行因此不被改写，撤回只作用于整单的进行中任务
	now := time.Now()
	doing, err := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	if err != nil {
		return err
	}
	if !f.canWithdrawInstance(inst, doing, operator) {
		return errors.New("无权限撤回该流程实例")
	}
	// issues/134 案 A · 实例状态守卫（八栈同判据，守卫排在任务行层面既有保护之前）：
	// 撤回只允许作用于**进行中(10)**实例，非 10 一律拒——改前对已办结(20)/已终止(40) 的实例
	// 调撤回会把它静默改写成 30（已撤回），已办列表/按状态聚合的统计凭空改历史且零报错。
	// 内部码 20010009：本栈 error 无码位 ⇒ 沿用 20010007/20010008
	// （engine_impl.go rollbackToParent 的 `const noLineage`/`const guardFail`）的形状定义固定
	// 中文文案，出口由门面统一成 code=99999999 + msg 逐字，**码值不进 msg**（issues/121 口径）。
	const notDoing = "流程实例非进行中，无法撤回"
	if inst.State != model.InstanceStateDoing {
		return errors.New(notDoing)
	}
	// issues/53 E25 补正：改状态后的副本必须同步回聚合（UpdateInstance 级联会用聚合内
	// 旧任务副本覆盖已撤回状态——先 updateTask 再 updateInstance 会被覆盖回 DOING）
	// issues/113：撤回写 30（WITHDRAW），不用 Abandon(99)——99 是与废弃共用的码，
	// 混用会让"发起人撤回"和"引擎废弃"在任务表里塌成同一个值
	for _, t := range doing {
		t.Withdraw(now)
		// issues/114：进行中任务的 update_user 同样回写为真实撤回人（实例侧见下）
		t.UpdateUser = operator
	}
	inst.Withdraw(now) // 撤回状态 Withdraw(30) 而非 Reject(45)
	inst.UpdateUser = operator
	inst.Tasks = doing
	if err := f.repo.UpdateInstance(context.Background(), inst); err != nil {
		return err
	}
	// TASK_WITHDRAW(8) / spec §11.3 码 8：撤回把实例 state 写 30 **落库之后** fire，
	// 且**每轮撤回只 fire 一次**（不逐被撤回任务）——否则监听器按事件条数发站内信会翻倍。
	//
	// R2-2 契约改判（规范 11 §11.3 码 2 触发时机列原句）：
	//
	//	「实例 `state` 落库为 20/45 这类"走到终点"的状态之后。**`30`(撤回)/`40`(终止) 不由本支表达**
	//	——各有专属码 8/9；一场撤回同时发 8＋2 会让下游收到"流程已办结"的错通知（node 首版即此形状，已纠）」
	//
	// 加上码 8 行自己的「**撤回只发 8，不补发 2**——2 的语义是"流程走到终点"，撤回不是」与规范 08
	// 场景 32「**撤回(30)/终止(40) 不发 2**，各发 8/9；同轮既发 8 又发 2 ⇒ 红」⇒ 撤回这一支
	// **只 fire 8**。第一轮在此处补发的 PROCESS_INSTANCE_END(state=30) 按裁定摘除：双发会让下游
	// 监听器（站内信/待办清理）按"办结"给发起人推一条错通知。
	// 办结（executeNode TypeEnd → state 20/45）与拒绝（ExecuteAndJumpToEnd → state 45）两支的码 2
	// 是"流程自己走到终点"，本轮不动。
	f.engine.FireEvent(engine.ProcessEvent{
		Type: engine.EventTaskWithdraw, InstanceID: instanceID, Operator: operator,
	})
	return nil
}

// canWithdrawInstance 撤回归属判据（issues/114）——命中任一即放行：
//  1. operator = 实例发起人（wf_process_instance.operator）；
//  2. operator 是该实例任一**进行中**任务的参与者（wf_process_task_actor.actor_id）；
//  3. operator ∈ {flow.auto, flow.admin}。
//
// ⚠️ 判据 1 不可复用引擎 isAllowed：engine_impl.go 的 isAllowed 只判"operator 在不在该任务
// actorIds"+ auto/admin 放行，**不查实例发起人**（八语言同构缺口），故发起人这一支显式补在这里。
// 判据 2/3 与 isAllowed 同口径（任务副本 ActorIDs + 参与者表兜底；两仓的 doing 任务查询都会
// 水合 ActorIDs，仍留仓储兜底以防自定义仓储不水合）。
func (f *Facade) canWithdrawInstance(inst *model.ProcessInstance, doing []*model.ProcessTask, operator string) bool {
	if inst != nil && inst.Operator == operator {
		return true
	}
	if strings.EqualFold(operator, engine.KeyAutoExecute) || strings.EqualFold(operator, engine.KeyAdminID) {
		return true
	}
	for _, t := range doing {
		if t.IsAllowed(operator) {
			return true
		}
		actors, _ := f.repo.FindTaskActors(context.Background(), t.ID)
		if containsStr(actors, operator) {
			return true
		}
	}
	return false
}

// ═══ 流程任务 ═══

func (f *Facade) execute(args map[string]interface{}) error {
	taskID, err := toInt64(args["processTaskId"])
	if err != nil {
		return fmt.Errorf("processTaskId 缺失或非法: %v", err)
	}
	operator := operatorArg(args)
	submitType, err := toInt(args["submitType"])
	if err != nil {
		submitType = 1 // AGREE
	}
	flowArgs := map[string]interface{}{}
	for k, v := range args {
		if k == "processTaskId" || k == "operator" {
			continue
		}
		flowArgs[k] = v
	}
	flowArgs["submitType"] = submitType
	// boot3 execute 分发（spec §11.2）
	switch submitType {
	case 2: // REJECT
		_, err = f.engine.ExecuteAndJumpToEnd(context.Background(), taskID, operator, flowArgs)
	case 3: // ROLLBACK
		_, err = f.engine.ExecuteAndJumpTask(context.Background(), taskID, operator, flowArgs, "")
	case 4: // JUMP
		_, err = f.engine.ExecuteAndJumpTask(context.Background(), taskID, operator, flowArgs, toStr(args["taskName"], ""))
	case 6: // ROLLBACK_TO_OPERATOR
		_, err = f.engine.ExecuteAndJumpToFirstTaskNode(context.Background(), taskID, operator, flowArgs)
	case 20: // COUNTERSIGN_DISAGREE
		flowArgs["countersignDisagreeFlag"] = 1
		_, err = f.engine.ExecuteProcessTask(context.Background(), taskID, operator, flowArgs)
	default: // 0 APPLY / 1 AGREE / 5 重新提交
		_, err = f.engine.ExecuteProcessTask(context.Background(), taskID, operator, flowArgs)
	}
	return err
}

// ═══ 流程设计（需扩展仓储） ═══

func (f *Facade) designPage(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	rows, total, err := ext.PageDesigns(context.Background(), query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = designRowToMap(r)
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

func (f *Facade) designDetail(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	design, err := ext.FindDesignByID(context.Background(), id)
	if err != nil || design == nil {
		return nil, errors.New("流程设计不存在")
	}
	data := map[string]interface{}{
		"id": design.ID, "name": design.Name, "displayName": design.DisplayName,
		"type": design.Type, "icon": design.Icon, "isDeployed": design.IsDeployed,
		"remark": design.Remark,
	}
	hisList, err := ext.ListDesignHis(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if len(hisList) > 0 {
		var graph map[string]interface{}
		if json.Unmarshal(hisList[0].Content, &graph) == nil {
			data["jsonObject"] = graph
		}
	}
	// issues/07：jsonObject 缺失基本信息时从设计表补齐（对齐 boot3 ProcessDesignServiceImpl.findById）
	jo, _ := data["jsonObject"].(map[string]interface{})
	if jo == nil {
		jo = map[string]interface{}{}
	}
	if _, ok := jo["name"]; !ok {
		jo["name"] = design.Name
	}
	if _, ok := jo["displayName"]; !ok {
		jo["displayName"] = design.DisplayName
	}
	if _, ok := jo["type"]; !ok {
		jo["type"] = design.Type
	}
	if _, ok := jo["processDesignId"]; !ok {
		jo["processDesignId"] = design.ID
	}
	data["jsonObject"] = jo
	data["his"] = hisList
	return data, nil
}

func (f *Facade) designSave(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	operator := operatorArg(args)
	id, _ := toInt64(args["id"])
	var design *model.ProcessDesign
	var err error
	if id == 0 {
		design = &model.ProcessDesign{
			Name:        toStr(args["name"], ""),
			DisplayName: toStr(args["displayName"], ""),
			Type:        toStr(args["type"], "approval"),
			Icon:        toStr(args["icon"], ""),
			Remark:      toStr(args["remark"], ""),
			CreateUser:  operator,
			UpdateUser:  operator,
		}
		err = ext.SaveDesign(context.Background(), design)
	} else {
		design, err = ext.FindDesignByID(context.Background(), id)
		if err != nil || design == nil {
			return nil, errors.New("流程设计不存在")
		}
		if v, ok := args["displayName"]; ok {
			design.DisplayName = toStr(v, "")
		}
		if v, ok := args["type"]; ok {
			design.Type = toStr(v, "")
		}
		if v, ok := args["icon"]; ok {
			design.Icon = toStr(v, "")
		}
		if v, ok := args["remark"]; ok {
			design.Remark = toStr(v, "")
		}
		design.UpdateUser = operator
		// 内容快照变更 → 置为未部署（对齐 boot3 updateDefine 语义，issues/08）
		if content, cerr := contentBytes(args); cerr == nil && content != nil {
			design.IsDeployed = 0
		}
		err = ext.UpdateDesign(context.Background(), design)
	}
	if err != nil {
		return nil, err
	}
	// 内容快照（设计稿内容存历史表）
	if content, cerr := contentBytes(args); cerr == nil && content != nil {
		if herr := ext.SaveDesignHis(context.Background(), &model.ProcessDesignHis{
			ProcessDesignID: design.ID,
			Content:         content,
			CreateUser:      operator,
		}); herr != nil {
			return nil, herr
		}
	}
	return map[string]interface{}{"id": design.ID}, nil
}

func (f *Facade) designRemove(args map[string]interface{}) error {
	// issues/28：兼容 {ids} 批量（boot3 前端 IdsParam 惯例）与单 {id}
	ids, err := idListArgs(args)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := f.ext().RemoveDesign(context.Background(), id); err != nil {
			return err
		}
	}
	return nil
}

// designListByType 按类型分组列出流程设计（issue 30，对齐 Java issues/28）：不依赖框架字典
func (f *Facade) designListByType(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	query := spi.PageQuery{PageNum: 1, PageSize: 10000, Conditions: parseMQuery(args)}
	rows, _, err := ext.PageDesigns(context.Background(), query)
	if err != nil {
		return nil, err
	}
	// 每 name 最新 define（version 最大）
	defQuery := spi.PageQuery{PageNum: 1, PageSize: 10000}
	defRows, _, err := f.repo.PageDefines(context.Background(), defQuery)
	if err != nil {
		return nil, err
	}
	latestByName := map[string]*model.DefineRow{}
	for _, r := range defRows {
		if prev, ok := latestByName[r.Name]; !ok || r.Version > prev.Version {
			latestByName[r.Name] = r
		}
	}
	groups := map[string][]map[string]interface{}{}
	for _, d := range rows {
		key := d.Type
		item := map[string]interface{}{
			"processDesignId": d.ID,
			"name":            d.Name,
			"displayName":     d.DisplayName,
			"icon":            d.Icon,
			"remark":          d.Remark,
		}
		if latest, ok := latestByName[d.Name]; ok {
			item["processDefineId"] = latest.ID
			item["processDefineState"] = latest.State
		}
		if his, err := ext.ListDesignHis(context.Background(), d.ID); err == nil && len(his) > 0 {
			var graph map[string]interface{}
			if json.Unmarshal(his[0].Content, &graph) == nil {
				item["jsonObject"] = graph
			}
		}
		groups[key] = append(groups[key], item)
	}
	return groups, nil
}

// bizData 按流程实例回显业务数据（issue 30，对齐 Java issues/28）：metaReader 注入式，未注入清晰报错
func (f *Facade) bizData(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(firstNonNil(args["processInstanceId"], args["id"]))
	if err != nil {
		return nil, errors.New("processInstanceId 缺失")
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), instanceID)
	if err != nil || inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	def, err := f.repo.FindDefineByID(context.Background(), inst.DefineID)
	if err != nil || def == nil {
		return nil, errors.New("流程定义不存在")
	}
	var meta struct {
		RelTableName string `json:"relTableName"`
		Name         string `json:"name"`
	}
	if json.Unmarshal(def.Content, &meta) != nil {
		return nil, errors.New("流程定义解析失败")
	}
	tableName := strings.TrimSpace(meta.RelTableName)
	if tableName == "" {
		tableName = strings.TrimSpace(meta.Name)
	}
	if tableName == "" {
		return nil, errors.New("流程定义未配置 relTableName")
	}
	if f.metaReader == nil {
		return nil, errors.New("业务数据读取器未注册（facade.SetMetaReader(...)）")
	}
	return f.metaReader.ReadByProcessInstance(tableName, instanceID)
}

func (f *Facade) designDeploy(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	design, err := ext.FindDesignByID(context.Background(), id)
	if err != nil || design == nil {
		return nil, errors.New("流程设计不存在")
	}
	hisList, err := ext.ListDesignHis(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if len(hisList) == 0 {
		return nil, errors.New("流程设计没有内容，无法发布")
	}
	// 发布：复用 deploy 版本管理
	defineID, err := f.deploy(map[string]interface{}{
		"content":  hisList[0].Content,
		"operator": toStr(args["operator"], "system"),
	})
	if err != nil {
		return nil, err
	}
	design.IsDeployed = 1
	design.UpdateUser = toStr(args["operator"], "system")
	if err := ext.UpdateDesign(context.Background(), design); err != nil {
		return nil, err
	}
	return defineID, nil
}

// designUpdate 修改流程设计基本信息（对齐 boot3 ProcessDesignController.update，不写设计稿快照）
func (f *Facade) designUpdate(args map[string]interface{}) error {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return fmt.Errorf("id 缺失或非法: %v", err)
	}
	design, err := ext.FindDesignByID(context.Background(), id)
	if err != nil || design == nil {
		return errors.New("流程设计不存在")
	}
	if v, ok := args["name"]; ok {
		design.Name = toStr(v, "")
	}
	if v, ok := args["displayName"]; ok {
		design.DisplayName = toStr(v, "")
	}
	if v, ok := args["type"]; ok {
		design.Type = toStr(v, "")
	}
	if v, ok := args["icon"]; ok {
		design.Icon = toStr(v, "")
	}
	if v, ok := args["remark"]; ok {
		design.Remark = toStr(v, "")
	}
	design.UpdateUser = toStr(args["operator"], "system")
	return ext.UpdateDesign(context.Background(), design)
}

// designUpdateDefine 更新流程设计定义（设计稿保存，issues/08）：content 快照入库 + 同步基本信息 + 置未部署
func (f *Facade) designUpdateDefine(args map[string]interface{}) error {
	ext := f.ext()
	designID, err := toInt64(args["processDesignId"])
	if err != nil {
		return fmt.Errorf("processDesignId 缺失或非法: %v", err)
	}
	design, err := ext.FindDesignByID(context.Background(), designID)
	if err != nil || design == nil {
		return errors.New("流程设计不存在")
	}
	content, cerr := contentBytes(args)
	if cerr != nil || content == nil {
		return errors.New("content 缺失")
	}
	// 与最新一条相同则不重复入库（对齐 boot3 updateDefine）
	hisList, err := ext.ListDesignHis(context.Background(), designID)
	if err != nil {
		return err
	}
	if len(hisList) == 0 || !bytes.Equal(hisList[0].Content, content) {
		if err := ext.SaveDesignHis(context.Background(), &model.ProcessDesignHis{
			ProcessDesignID: designID,
			Content:         content,
			CreateUser:      toStr(args["operator"], "system"),
		}); err != nil {
			return err
		}
	}
	// 同步设计基本信息（jsonObject 里的 name/displayName/type）+ 内容变更 → 未部署
	var flow model.FlowModel
	if json.Unmarshal(content, &flow) == nil {
		if flow.Name != "" {
			design.Name = flow.Name
		}
		if flow.DisplayName != "" {
			design.DisplayName = flow.DisplayName
		}
		if flow.Type != "" {
			design.Type = flow.Type
		}
	}
	design.IsDeployed = 0
	design.UpdateUser = toStr(args["operator"], "system")
	return ext.UpdateDesign(context.Background(), design)
}

// designRedeploy 重新部署流程定义（issues/08）：替换最新定义内容 + 置已部署（对齐 boot3 redeploy）
func (f *Facade) designRedeploy(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	designID, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	design, err := ext.FindDesignByID(context.Background(), designID)
	if err != nil || design == nil {
		return nil, errors.New("流程设计不存在")
	}
	hisList, err := ext.ListDesignHis(context.Background(), designID)
	if err != nil {
		return nil, err
	}
	if len(hisList) == 0 {
		return nil, errors.New("流程设计没有内容，无法发布")
	}
	content := hisList[0].Content
	var flow model.FlowModel
	if err := json.Unmarshal(content, &flow); err != nil {
		// issues/139：对外 msg 为固定文案，原始异常只留在错误对象链（Unwrap），不进 msg
		return nil, fixedMsgError(msgReadFlowDefineJSONFailed, err)
	}
	if flow.Name == "" {
		return nil, errors.New("流程定义缺少 name")
	}
	// 按 name 取最新定义：有则替换内容（version 不变），无则新建（对齐 boot3 redeploy）
	last, lerr := f.repo.FindDefineByName(context.Background(), flow.Name)
	var defineID int64
	if lerr != nil || last == nil {
		def, derr := f.deploy(map[string]interface{}{
			"content":  content,
			"operator": toStr(args["operator"], "system"),
		})
		if derr != nil {
			return nil, derr
		}
		if m, ok := def.(map[string]interface{}); ok {
			defineID, _ = m["processDefineId"].(int64)
		}
	} else {
		if err := f.repo.UpdateDefine(context.Background(), &model.ProcessDefine{
			ID:          last.ID,
			Name:        flow.Name,
			DisplayName: flow.DisplayName,
			Type:        flow.Type,
			Content:     content,
			// issues/59：保留原 version（替换语义，不递增）；缺失时 JDBC 兜底会误写 1
			Version:    last.Version,
			UpdateUser: toStr(args["operator"], "system"),
		}); err != nil {
			return nil, err
		}
		defineID = last.ID
	}
	design.IsDeployed = 1
	design.UpdateUser = toStr(args["operator"], "system")
	if err := ext.UpdateDesign(context.Background(), design); err != nil {
		return nil, err
	}
	return map[string]interface{}{"processDefineId": defineID}, nil
}

// ═══ 委托代理（需扩展仓储） ═══

func (f *Facade) surrogatePage(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	if v, ok := args["operator"]; ok {
		query.Filters = map[string]interface{}{"operator": toStr(v, "")}
	}
	rows, total, err := ext.PageSurrogates(context.Background(), query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = surrogateRowToMap(r)
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

func (f *Facade) surrogateSave(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	operator := operatorArg(args)
	id, _ := toInt64(args["id"])
	var surrogate *model.ProcessSurrogate
	var err error
	if id == 0 {
		surrogate = &model.ProcessSurrogate{
			Operator:   operator, // 授权人 = 操作人（新建必有）
			CreateUser: operator,
			UpdateUser: operator,
		}
		applySurrogateFields(surrogate, args, operator)
		err = ext.SaveSurrogate(context.Background(), surrogate)
	} else {
		surrogate, err = ext.FindSurrogateByID(context.Background(), id)
		if err != nil || surrogate == nil {
			return nil, errors.New("委托记录不存在")
		}
		applySurrogateFields(surrogate, args, operator)
		err = ext.UpdateSurrogate(context.Background(), surrogate)
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"id": surrogate.ID}, nil
}

// surrogateUpdate 委托更新（issues/77）：按 id 全字段更新，id 不存在报错
func (f *Facade) surrogateUpdate(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	surrogate, err := ext.FindSurrogateByID(context.Background(), id)
	if err != nil || surrogate == nil {
		return nil, errors.New("委托记录不存在")
	}
	operator := operatorArg(args)
	applySurrogateFields(surrogate, args, operator)
	err = ext.UpdateSurrogate(context.Background(), surrogate)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"id": surrogate.ID}, nil
}

// surrogateDetail 委托详情（issues/77）：按 id 查单条，返回行结构（时间格式化）
func (f *Facade) surrogateDetail(args map[string]interface{}) (interface{}, error) {
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	surrogate, err := f.ext().FindSurrogateByID(context.Background(), id)
	if err != nil || surrogate == nil {
		return nil, errors.New("委托记录不存在")
	}
	return surrogateRowToMap(surrogate), nil
}

// applySurrogateFields 委托写入公共字段。授权人（operator）仅在显式传入时覆盖，
// 避免 update 时清空原授权人（前端编辑表单不带 operator；集成层注入时 operator=授权人，覆盖无害）
func applySurrogateFields(s *model.ProcessSurrogate, args map[string]interface{}, operator string) {
	s.ProcessName = toStr(args["processName"], "")
	if v, ok := args["operator"]; ok {
		s.Operator = toStr(v, "")
	}
	s.Surrogate = toStr(args["surrogate"], "")
	s.StartTime = parseSurrogateTime(args["startTime"])
	s.EndTime = parseSurrogateTime(args["endTime"])
	s.Enabled = parseSurrogateEnabled(args["enabled"])
	s.UpdateUser = operator
}

// parseSurrogateEnabled 委托启用位解析（issues/116 判据 d："enabled 只有 1 生效，
// 脏值不得默认当启用"）：
//   - 未传（nil）→ 1，契约默认启用（processSurrogate/save 的 enabled 可省）；
//   - bool → true=1 / false=0（前端开关组件偶发传布尔，不落入脏值分支）；
//   - 传了但不可解析为整数（"abc"、{}…）→ **0 停用**。
//
// 此前走 toIntDef(args["enabled"], 1)，脏值回落 1＝"垃圾值当启用"，与 C# 的
// ToInt(default=1) 同侧、与 PHP (int)'abc'→0 反侧（issues/116 §5）；本轮按契约
// canonical 统一到"脏值停用"。
func parseSurrogateEnabled(v interface{}) int {
	if v == nil {
		return 1
	}
	if b, ok := v.(bool); ok {
		if b {
			return 1
		}
		return 0
	}
	n, err := toInt(v)
	if err != nil {
		return 0
	}
	return n
}

// parseSurrogateTime 解析委托时间入参：兼容 yyyy-MM-dd HH:mm:ss（前端 RangePicker/SPEC 契约）
// 与 ISO T（issues/77）
func parseSurrogateTime(v interface{}) *time.Time {
	str, ok := v.(string)
	if !ok || strings.TrimSpace(str) == "" {
		return nil
	}
	str = strings.TrimSpace(str)
	// 按本地时区解析（wall-clock，对齐其余五语言的 naive 语义与 MySQL DATETIME 列），
	// time.Parse 的 UTC 语义会使时间范围与库内时间错位 8 小时（两线一致性 issues/103）
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, perr := time.ParseInLocation(layout, str, time.Local); perr == nil {
			return &t
		}
	}
	return nil
}

// surrogateRowToMap 委托行：时间格式化（issues/77，对齐 designRowToMap / SPEC）
func surrogateRowToMap(s *model.ProcessSurrogate) map[string]interface{} {
	return map[string]interface{}{
		"id": s.ID, "processName": s.ProcessName, "operator": s.Operator, "surrogate": s.Surrogate,
		"startTime": fmtTime(s.StartTime), "endTime": fmtTime(s.EndTime),
		"enabled":    s.Enabled,
		"createTime": fmtTimeV(s.CreateTime), "createUser": s.CreateUser,
		"updateTime": fmtTimeV(s.UpdateTime), "updateUser": s.UpdateUser,
	}
}

func (f *Facade) surrogateRemove(args map[string]interface{}) error {
	// issues/95：前端「我的委托」行内/批量删除统一发 {ids}，与 define/design remove 同惯例
	ids, err := idListArgs(args)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := f.ext().RemoveSurrogate(context.Background(), id); err != nil {
			return err
		}
	}
	return nil
}

// ═══ 视图端点（v1.2.0） ═══

func (f *Facade) getLastByName(args map[string]interface{}) (interface{}, error) {
	name := toStr(args["processDefineName"], "")
	def, err := f.repo.FindDefineByName(context.Background(), name)
	if err != nil || def == nil {
		return nil, fmt.Errorf("流程定义不存在: %s", name)
	}
	graph := map[string]interface{}{}
	if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
		// 前端表单渲染/流程图依赖（issues/05）
		return map[string]interface{}{
			"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
			"type": def.Type, "state": def.State, "version": def.Version,
			"jsonObject": graph,
		}, nil
	}
	return map[string]interface{}{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
	}, nil
}

func (f *Facade) highLight(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), instanceID)
	if err != nil || inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	active := []string{}
	history := []string{}
	edges := []string{}
	// 活跃节点 = 进行中任务
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, t := range doing {
		if !containsStr(active, t.TaskName) {
			active = append(active, t.TaskName)
		}
	}
	// 历史节点 = 全部任务（排除活跃）+ 模型路径补全
	his, _ := f.repo.FindHistoryTasks(context.Background(), instanceID)
	for _, t := range his {
		if !containsStr(active, t.TaskName) && !containsStr(history, t.TaskName) {
			history = append(history, t.TaskName)
		}
	}
	// 路径补全：start 沿 edges 递归，遇活跃节点停止；决策分支按表达式求值过滤（issues/06）
	nodeProgress := map[string]interface{}{}
	def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
	if def != nil {
		var flow model.FlowModel
		if json.Unmarshal(def.Content, &flow) == nil {
			nodeProgress = f.buildNodeProgress(&flow, his)
			f.collectPath(&flow, "start", "", active, &history, &edges, map[string]bool{}, inst.Variables, his)
		}
	}
	return map[string]interface{}{
		"activeNodeNames":  active,
		"historyNodeNames": history,
		"historyEdgeNames": edges,
		"nodeProgress":     nodeProgress,
	}, nil
}

// buildNodeProgress 节点成员进度（issue 41，对齐 boot3 highLight）：按任务状态 + 会签变量组装。
// 会签节点带 type（PARALLEL/SEQUENTIAL）；done 按任务完成状态逐人标记，active = 进行中任务首位；
// 动态参与人节点（无静态 actorIds）不返回；name 缺省（引擎不持有宿主用户体系，前端降级显示 id）
func (f *Facade) buildNodeProgress(flow *model.FlowModel, tasks []*model.ProcessTask) map[string]interface{} {
	progress := map[string]interface{}{}
	names := []string{}
	seen := map[string]bool{}
	for _, t := range tasks {
		if !seen[t.TaskName] {
			seen[t.TaskName] = true
			names = append(names, t.TaskName)
		}
	}
	for _, name := range names {
		var ts []*model.ProcessTask
		for _, t := range tasks {
			if t.TaskName == name {
				ts = append(ts, t)
			}
		}
		vars := map[string]interface{}{}
		if len(ts) > 0 && ts[0].Variables != nil {
			vars = ts[0].Variables
		}
		// 完整办理人列表：会签变量 operatorList_{node} 优先（顺序会签全量），否则任务 actorIds 并集
		members := toStringSlice2(vars["operatorList_"+name])
		if len(members) == 0 {
			set := map[string]bool{}
			for _, t := range ts {
				for _, a := range t.ActorIDs {
					set[a] = true
				}
			}
			for a := range set {
				members = append(members, a)
			}
		}
		if len(members) == 0 {
			continue // 动态参与人：无静态成员，不返回
		}
		doneSet := map[string]bool{}
		for _, t := range ts {
			if t.TaskState == model.TaskStateDone {
				for _, a := range t.ActorIDs {
					doneSet[a] = true
				}
			}
		}
		activeActor := ""
		for _, t := range ts {
			if t.TaskState == model.TaskStateDoing && len(t.ActorIDs) > 0 {
				activeActor = t.ActorIDs[0]
				break
			}
		}
		// 会签判定：定义节点属性（引擎创建任务时 PerformType 未落任务表，取模型为准）
		node := findNodeIn(flow, name)
		var nodeProps map[string]interface{}
		if node != nil {
			nodeProps = node.Properties
		}
		csType := ""
		if v, ok := nodeProps["countersignType"].(string); ok {
			csType = v
		}
		isCs := csType != "" || engine.IsCountersign(nodeProps["performType"])
		// 姓名走 UserProvider SPI 解析（issue 43/E15）：goroutine 并行批量，查不到缺省空串
		nameMap := map[string]string{}
		if up := f.engine.UserProvider(); up != nil {
			var mu sync.Mutex
			var wg sync.WaitGroup
			for _, id := range members {
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					if u, err := up.GetUser(id); err == nil && u != nil && u.RealName != "" {
						mu.Lock()
						nameMap[id] = u.RealName
						mu.Unlock()
					}
				}(id)
			}
			wg.Wait()
		}
		memberList := []map[string]interface{}{}
		for _, id := range members {
			m := map[string]interface{}{"id": id, "name": nameMap[id]}
			if doneSet[id] {
				m["done"] = true
			} else if id == activeActor {
				m["active"] = true
			}
			memberList = append(memberList, m)
		}
		item := map[string]interface{}{"members": memberList}
		if isCs && csType != "" {
			item["type"] = csType
		}
		progress[name] = item
	}
	return progress
}

// collectPath 从节点沿输出边递归（遇活跃节点停止），补全历史节点与边；
// 决策节点输出边带 expr 时用表达式求值过滤（对齐 boot3 recursionModel，issues/06）
func (f *Facade) collectPath(flow *model.FlowModel, nodeID, edgeName string, active []string,
	history *[]string, edges *[]string, visited map[string]bool,
	vars map[string]interface{}, historyTasks []*model.ProcessTask) {
	if visited[nodeID] {
		return
	}
	visited[nodeID] = true
	if edgeName != "" && !containsStr(*edges, edgeName) {
		*edges = append(*edges, edgeName)
	}
	src := findNodeIn(flow, nodeID)
	for _, e := range flow.Edges {
		if e.SourceNodeID != nodeID {
			continue
		}
		// 决策节点：输出边表达式求值过滤——false 的分支未实际执行，不收集
		if src != nil && src.Type == "snaker:decision" {
			if expr, ok := e.Properties["expr"].(string); ok && expr != "" {
				if ok2, _ := f.evalDecisionExpr(flow, src, expr, vars, historyTasks); !ok2 {
					continue
				}
			}
		}
		target := findNodeIn(flow, e.TargetNodeID)
		if target == nil {
			continue
		}
		if !containsStr(active, target.ID) && !containsStr(*history, target.ID) {
			*history = append(*history, target.ID)
		}
		if containsStr(active, target.ID) {
			continue // 遇活跃节点停止深入
		}
		f.collectPath(flow, target.ID, e.ID, active, history, edges, visited, vars, historyTasks)
	}
}

// evalDecisionExpr 决策输出边表达式求值（args = 实例变量 + 决策节点前置任务变量，与引擎同源）
func (f *Facade) evalDecisionExpr(flow *model.FlowModel, decision *model.FlowNode, expr string,
	vars map[string]interface{}, historyTasks []*model.ProcessTask) (bool, error) {
	args := map[string]interface{}{}
	for k, v := range vars {
		args[k] = v
	}
	// 前置任务变量：决策节点输入的第一个源节点对应的历史任务
	for _, e := range flow.Edges {
		if e.TargetNodeID == decision.ID {
			for _, t := range historyTasks {
				if t.TaskName == e.SourceNodeID {
					for k, v := range t.Variables {
						args[k] = v
					}
					break
				}
			}
			break
		}
	}
	result, err := f.engine.EvalExpr(expr, args)
	if err != nil {
		return false, nil
	}
	b, _ := result.(bool)
	return b, nil
}

func findNodeIn(flow *model.FlowModel, id string) *model.FlowNode {
	for i := range flow.Nodes {
		if flow.Nodes[i].ID == id {
			return &flow.Nodes[i]
		}
	}
	return nil
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (f *Facade) approvalRecord(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	his, err := f.repo.FindHistoryTasks(context.Background(), instanceID)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]interface{}, 0, len(his))
	for _, t := range his {
		rows = append(rows, map[string]interface{}{
			"taskName": t.TaskName, "displayName": t.DisplayName,
			"taskType": t.TaskType, "performType": t.PerformType,
			"taskState": t.TaskState, "operator": t.ActorID,
			"finishTime": fmtTime(t.FinishTime),
			"ext":        t.Variables, // issues/15：前端读 ext.tf_approvalComment；issues/124 variable 原串出口下线
		})
	}
	return rows, nil
}

func (f *Facade) getAssigneeTextData(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	includeNodeName := true
	if v, ok := args["includeNodeName"].(bool); ok {
		includeNodeName = v
	}
	rows := []map[string]interface{}{}
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, t := range doing {
		actors, _ := f.repo.FindTaskActors(context.Background(), t.ID)
		for _, actor := range actors {
			label := actor
			if includeNodeName {
				label = t.DisplayName + ":" + actor
			}
			rows = append(rows, map[string]interface{}{"label": label, "value": actor})
		}
	}
	return rows, nil
}

func (f *Facade) createCCInstance(args map[string]interface{}) error {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return fmt.Errorf("processInstanceId 缺失或非法: %v", err)
	}
	operator := operatorArg(args)
	// issues/141 G10「空抄送人不建 cc 行」（spec 06 §2.10）＋ issues/142 B 批（spec 06 §2.11）：
	// 手动腿与引擎腿（f_/tf_ 两条）走**同一枚判据** [spi.NormalizeActors]，本栈的解析入口
	// 就是 toStringSlice2——逗号串与数组**两形同判据**（空串/纯空白/数组里的 nil 一律丢弃，
	// 落库与比较值取 trim 后的串）。
	// 归一后**丢完为空 ⇒ 与上面"空 actorIds"档同判**（沿用既有 `actorIds 缺失`，不新造错误码
	// 或文案）：不建行、不 fire 码 4。
	// ⚠️ 反向哨兵：只吃空值，"0" 这类"看起来像空"的正常 id 不得被丢掉。
	actors := toStringSlice2(args["actorIds"])
	if len(actors) == 0 {
		return errors.New("actorIds 缺失")
	}
	// issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：手动腿与引擎腿（f_/tf_ 两条）共用
	// 同一条判据（spec §11.7「三条入口同一支」）——同一 (实例, 人) 已有 cc 行时跳过，
	// 不新增行、不重置未读、不更新原行时间。
	created, err := f.repo.CreateCcInstanceIfAbsent(context.Background(), instanceID, operator, actors...)
	if err != nil {
		return err
	}
	// CC_CREATE（spec §11.2 原则 1 ＋ §11.3 码 4 ＋ §11.6）：手动抄送腿同样必须 fire——
	// 码值表达"新增了一条抄送记录"这个事实。fire 排在 cc 行落库之后（§11.2 原则 3）；
	// 集成层严禁再自己补发（§11.1）。
	// 入参＝**实际新建的子集**而不是原始 actors（issues/141 G2）：重复抄送没发生"创建"
	// ⇒ 不发码 4；子集为空整支不 fire（不空转、也不照旧全量 fire）。
	for _, actor := range created {
		f.engine.FireEvent(engine.ProcessEvent{
			Type: engine.EventCCCreate, InstanceID: instanceID, CcActorID: actor,
		})
	}
	return nil
}

func (f *Facade) updateCCStatus(args map[string]interface{}) error {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return fmt.Errorf("processInstanceId 缺失或非法: %v", err)
	}
	// issues/142 B 批（spec 06-facade.md §2.11 写点表「updateCCStatus 的 operator」那一行）：
	// **入参归一后再比**——operator 取 trim 后的值，否则 " 123 " 打不中库里 trim 后的行（静默
	// no-op 报成功），而空 operator 会把 state=1 打到历史 actor_id 为空串 的脏行上（issues/129 病根）。
	// operatorArg 已把"空串/缺键"回落 demo 缺省 user1（issues/129 案 A），这里补的是 trim 那半。
	operator := singleActorArg(operatorArg(args))
	return f.repo.UpdateCcStatus(context.Background(), instanceID, operator)
}

// ccList 我的抄送分页（v1.3.0）：operator 作为抄送人过滤
func (f *Facade) ccList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	actorID := operatorArg(args)
	rows, total, err := f.repo.PageCcInstances(context.Background(), query, actorID)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, ccRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

func (f *Facade) taskDetail(args map[string]interface{}) (interface{}, error) {
	taskID, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	operator := operatorArg(args)
	task, err := f.repo.FindTaskByID(context.Background(), taskID)
	if err != nil || task == nil {
		return nil, errors.New("任务不存在")
	}
	actors, _ := f.repo.FindTaskActors(context.Background(), taskID)
	// issues/82-5：任务级 ext.isFirstTaskNode（前端 detail.vue 双兜底 record.ext?.isFirstTaskNode）
	// issues/121 P1：改成「行上值优先、缺键才回退现算」——引擎建单时已把标记落库（已办结的历史行同样
	// 有效）；存量行没这个键才回退下面那条带 doing 判定的现算，现算只够展示用，引擎内部判定不得用它。
	tExt := map[string]interface{}{}
	for k, v := range task.Variables {
		tExt[k] = v
	}
	doing := task.TaskState == model.TaskStateDoing
	rowFirst, hasRowFirst := rowFirstTaskNode(tExt)
	tExt[engine.KeyIsFirstTaskNode] = rowFirst
	vo := map[string]interface{}{
		"id": task.ID, "processInstanceId": task.ProcessInstanceID,
		"taskName": task.TaskName, "displayName": task.DisplayName,
		"taskType": task.TaskType, "performType": task.PerformType,
		"taskState": task.TaskState, "operator": task.ActorID,
		"formKey": task.FormKey, "taskActorIdList": actors,
		"executable":   task.IsAllowed(operator),
		"ext":          tExt,
		"taskFormData": formDataOf(task.Variables, "tf_"), // issues/15 + 对齐 java taskDetail 顶层出口（issues/124 G4）
	}
	// taskModel：流程定义中对应节点
	inst, _ := f.repo.FindInstanceByID(context.Background(), task.ProcessInstanceID)
	if inst != nil {
		def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
		if def != nil {
			graph := map[string]interface{}{}
			if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
				vo["jsonObject"] = graph // issues/05
				if !hasRowFirst {
					// 存量行没有落库标记 ⇒ 才回退现算（"仅进行中"口径，只对展示够用）
					tExt[engine.KeyIsFirstTaskNode] = doing && task.TaskName == firstTaskNodeIDOf(graph)
				}
			}
			var flow model.FlowModel
			if json.Unmarshal(def.Content, &flow) == nil {
				for _, n := range flow.Nodes {
					if n.ID == task.TaskName {
						tm := map[string]interface{}{
							"name": n.ID, "displayName": n.Text.Value, "type": n.Type,
						}
						// issues/62：taskModel 补 form/ext（节点字段权限，对齐 boot2）
						if v, ok := n.Properties["form"]; ok {
							tm["form"] = v
						}
						if v, ok := n.Properties["field"]; ok {
							tm["ext"] = v
						}
						vo["taskModel"] = tm
						break
					}
				}
			}
		}
	}
	return vo, nil
}

func (f *Facade) jumpAbleTaskNameList(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return nil, fmt.Errorf("processInstanceId 缺失或非法: %v", err)
	}
	done, _ := f.repo.FindDoneTasks(context.Background(), instanceID, nil)
	rows := []map[string]interface{}{}
	seen := map[string]bool{}
	for _, t := range done {
		if t.PerformType == 1 { // COUNTERSIGN
			continue
		}
		if !seen[t.TaskName] {
			seen[t.TaskName] = true
			rows = append(rows, map[string]interface{}{"label": t.DisplayName, "value": t.TaskName})
		}
	}
	return rows, nil
}

func (f *Facade) candidatePage(args map[string]interface{}) (interface{}, error) {
	taskID, err := toInt64(args["processTaskId"])
	if err != nil {
		taskID, err = toInt64(args["id"])
	}
	if err != nil {
		return nil, errors.New("processTaskId 缺失")
	}
	task, err := f.repo.FindTaskByID(context.Background(), taskID)
	if err != nil || task == nil {
		return nil, errors.New("任务不存在")
	}
	inst, _ := f.repo.FindInstanceByID(context.Background(), task.ProcessInstanceID)
	if inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	// 模型候选解析：当前任务的后继任务节点的 candidateUsers / candidateGroups 配置
	var candidates []string
	def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
	if def != nil {
		var flow model.FlowModel
		if json.Unmarshal(def.Content, &flow) == nil {
			candidates = f.nextTaskCandidates(&flow, task.TaskName)
		}
	}
	if len(candidates) > 0 {
		// 候选命中 → 用户信息映射（UserProvider 兜底）
		// issues/80：行键对齐前端 UserSelect（valueField='id'）——补 id 键，保留 userId 兼容旧消费方
		rows := []map[string]interface{}{}
		for _, c := range candidates {
			rows = append(rows, map[string]interface{}{"id": c, "userId": c, "realName": c})
		}
		return pageData(1, 10, len(rows), rows), nil
	}
	// 无模型候选 → 用户分页搜索（依赖 UserSearch 钩子）
	if f.userSearch == nil {
		return nil, errors.New("未配置 UserSearch（用户搜索钩子）")
	}
	rows, total, err := f.userSearch(args)
	if err != nil {
		return nil, err
	}
	return pageData(toIntDef(args["pageNum"], 1), toIntDef(args["pageSize"], 10), total, rows), nil
}

// nextTaskCandidates 找当前任务节点的后继任务节点，收集 candidateUsers / candidateGroups（逗号分割）。
// candidateGroups 按角色取人（OrgUserProvider.findByRole，v1.6.0 对齐 boot4 GlobalCandidateHandler）。
func (f *Facade) nextTaskCandidates(flow *model.FlowModel, taskName string) []string {
	var result []string
	collect := func(node *model.FlowNode) {
		if v, ok := node.Properties["candidateUsers"].(string); ok && v != "" {
			for _, s := range strings.Split(v, ",") {
				s = strings.TrimSpace(s)
				if s != "" && !containsStr(result, s) {
					result = append(result, s)
				}
			}
		}
		if f.orgProv != nil {
			if v, ok := node.Properties["candidateGroups"].(string); ok && v != "" {
				for _, roleCode := range strings.Split(v, ",") {
					roleCode = strings.TrimSpace(roleCode)
					if roleCode == "" {
						continue
					}
					if ids, err := f.orgProv.FindByRole(roleCode); err == nil {
						for _, id := range ids {
							if id != "" && !containsStr(result, id) {
								result = append(result, id)
							}
						}
					}
				}
			}
		}
	}
	visited := map[string]bool{}
	var walk func(id string)
	walk = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		for _, e := range flow.Edges {
			if e.SourceNodeID != id {
				continue
			}
			target := findNodeIn(flow, e.TargetNodeID)
			if target == nil {
				continue
			}
			if target.Type == model.TypeTask || target.Type == model.TypeCustom {
				collect(target)
				continue
			}
			if target.Type == model.TypeFork || target.Type == model.TypeJoin ||
				target.Type == model.TypeDecision {
				walk(target.ID)
			}
		}
	}
	walk(taskName)
	return result
}

func (f *Facade) taskAddActor(args map[string]interface{}) error {
	taskID, err := taskIDArg(args)
	if err != nil {
		return err
	}
	// issues/142 B 批（spec 06 §2.11）：actorIds 的逗号串与数组**两形同判据**，归一已在
	// toStringSlice2 里落地（同一枚 spi.NormalizeActors：trim＋丢空＋折叠）。
	// 丢完为空 ⇒ 沿用既有"actorIds 缺失"信封（§2.11 硬要求③，不新造错误码/文案）。
	actors := toStringSlice2(args["actorIds"])
	if len(actors) == 0 {
		return errors.New("actorIds 缺失")
	}
	return f.repo.AddTaskActor(context.Background(), taskID, actors)
}

// taskIDArg 任务主键入参（spec 06-facade.md §2.11「主键类参数另判一档」）。
//
// 这一档与"归属值为空 ⇒ 丢弃"是**两件事**：归属值可有可无，主键没有就是调用方写错了，
// 静默接受等于把脏行钉进表里。所以 processTaskId 缺失/空串/非数值一律走既有
// 「processTaskId 缺失或非法」信封报错，**显式 0 也响亮报错**（不得拿 空串/0 当 id 落库）。
// ⚠️ 严禁把 [spi.NormalizeActors] 那套判据套到主键上——那是归属值的尺子。
func taskIDArg(args map[string]interface{}) (int64, error) {
	id, err := toInt64(args["processTaskId"])
	if err != nil {
		return 0, fmt.Errorf("processTaskId 缺失或非法: %v", err)
	}
	if id == 0 {
		return 0, errors.New("processTaskId 缺失或非法: 不得为 0")
	}
	return id, nil
}

// taskTransfer 转办（spec 06 §processTask/transfer，issues/115）：只摘 fromActor 那一行参与者、
// toActor 追加，**沿用同一 processTaskId**（待办从 A 挪到 B，不新建任务，高亮图/节点进度不变），
// 并在该任务行上留痕三件（submitType=7 槽位 + tf_transferHistory 追加式账本 + tf_approvalComment 末跳文案）。
//
// 与 surrogate/addCandidate 的区别：那两个 action 只追加（加签，原参与人保留可办），本 action 才摘人。
// 鉴权：operator == fromActor（只能转自己那一条待办），或 operator ∈ {flow.auto, flow.admin}。
func (f *Facade) taskTransfer(args map[string]interface{}) error {
	taskID, err := taskIDArg(args)
	if err != nil {
		return err
	}
	operator := strings.TrimSpace(toStr(args["operator"], ""))
	if operator == "" {
		return errors.New("operator 必填")
	}
	// issues/142 B 批（spec 06 §2.11 写点表 transfer 那一行）：fromActor/toActor **归一后再参与
	// 删除与插入**，走与集合侧同一枚判据 [spi.NormalizeActors]（singleActorArg）。
	// 必填档不变：丢完为空 ⇒ 既有「fromActor/toActor 必填」信封（硬要求③不新造错误码）。
	fromActor := singleActorArg(args["fromActor"])
	if fromActor == "" {
		return errors.New("fromActor 必填")
	}
	toActor := singleActorArg(args["toActor"])
	if toActor == "" {
		return errors.New("toActor 必填")
	}
	reason := toStr(args["reason"], "")
	if operator != fromActor &&
		!strings.EqualFold(operator, engine.KeyAutoExecute) &&
		!strings.EqualFold(operator, engine.KeyAdminID) {
		return errors.New("无权限转办该任务")
	}
	ctx := context.Background()
	task, err := f.repo.FindTaskByID(ctx, taskID)
	if err != nil || task == nil {
		return errors.New("任务不存在")
	}
	if task.TaskState != model.TaskStateDoing {
		return errors.New("任务非进行中，不可转办")
	}
	// 参与者以关系表为准（内存/SQL 两仓同源），任务副本 ActorIDs 一并去重纳入（仓储不水合时的兜底）
	actors, err := f.repo.FindTaskActors(ctx, taskID)
	if err != nil {
		return err
	}
	participants := dedupStrs(append(append([]string{}, actors...), task.ActorIDs...))
	// spec 06 §2.11 硬要求②「落库与比较一律取 trim 后的值」：库里历史行的值是**修复前落下的未
	// trim 原值**（" 123 "），入参归一后（"123"）必须判成同一个人。比对取归一形、**删除用行上的
	// 原值**——只拿归一值去 DELETE 会"判成同一人却一条没删"，转办报成功而原人的待办还在（假成功）。
	var fromRaw, kept []string
	alreadyTo := false
	for _, p := range participants {
		n := singleActorArg(p)
		if n == "" {
			kept = append(kept, p) // 历史空归属脏行不参与转办（不摘也不加），交给仓储写侧兜底
			continue
		}
		if n == fromActor {
			fromRaw = append(fromRaw, p)
			continue
		}
		if n == toActor {
			alreadyTo = true
		}
		kept = append(kept, p)
	}
	if len(fromRaw) == 0 {
		return errors.New("原办理人不是该任务参与人")
	}
	if alreadyTo {
		return errors.New("目标人已是该任务参与人")
	}
	// 摘原人 + 加新人：RemoveTaskActor 只删 fromActor 那些行（同一人的未 trim 脏行一并摘掉），
	// 同任务其余参与人（会签其他成员、加签来的人）不受影响
	if err := f.repo.RemoveTaskActor(ctx, taskID, fromRaw); err != nil {
		return err
	}
	if err := f.repo.AddTaskActor(ctx, taskID, []string{toActor}); err != nil {
		return err
	}
	now := time.Now()
	// 留痕三件（Go 的审批记录即任务行：approvalRecord 透出 variable/ext）——槽位就是任务行本身，
	// B 办结时 submitType 必被他的办理参数覆盖，没有追加式账本则多跳只剩末跳、办结后转办整体消失。
	// ① submitType=7 当前槽位（B 办结前记录直接读作"转办"）；办理人记谁由 update_user +
	//    tf_transferHistory[].operator 承载——**严禁覆写 actor_id 列**（契约 06 §transfer 留痕⚠️：
	//    进行中任务该列恒无值是既有不变量，写进去会让撤回/终止单凭空冒进被摘人的「我已办」）；
	// ② tf_transferHistory 跨跳持久账本，每跳 append 只追加不覆盖（B 办结后仍在）；
	// ③ tf_approvalComment 末跳可读文案，前端审批意见既有读取位（issues/15），多跳只留末跳。
	// 单跳便捷键 tf_transferTo/tf_transferReason 一并写，省前端一次遍历。
	vars := task.Variables
	if vars == nil {
		vars = map[string]interface{}{}
	}
	vars[engine.KeySubmitType] = int(model.SubmitTypeTransfer)
	vars["tf_transferTo"] = toActor
	vars["tf_transferReason"] = reason
	vars["tf_approvalComment"] = transferComment(fromActor, toActor, reason)
	vars["tf_transferHistory"] = appendTransferRecord(vars["tf_transferHistory"], map[string]interface{}{
		"submitType": int(model.SubmitTypeTransfer),
		"fromActor":  fromActor,
		"toActor":    toActor,
		"reason":     reason,
		"time":       now.Format(timeFmt),
		"operator":   operator,
	})
	task.Variables = vars
	// actor_id/operator 列**不写**（契约 06 §transfer 留痕⚠️，Node 实测复现）：进行中任务该列恒无值
	// 是本家族既有不变量；写进被摘走的人，该单一经撤回/终止会凭空出现在他从没办过的「我已办」
	// 列表（pageDoneTasks 按 state <> 10 AND operator = ? 过滤）。办理人由 update_user + 账本 operator 承载。
	task.UpdateTime = now
	task.UpdateUser = operator
	// 内存仓 UpdateTask 会用任务副本的 ActorIDs 覆盖参与者表：必须回传变更后的清单，
	// 否则上面的摘人/加人被旧副本回滚（SQL 仓 UpdateTask 不碰参与者表，同值回写无害）。
	// kept ＝ 参与者清单里**除去 fromRaw 那些行**的其余原值（逐字与 SQL 仓此刻的行集同形），
	// 追加的是归一后的 toActor ⇒ 两仓落库形状一致。
	task.ActorIDs = dedupStrs(append(kept, toActor))
	if err := f.repo.UpdateTask(ctx, task); err != nil {
		return err
	}
	// TASK_TRANSFER(7) / spec §11.3：任务参与者被替换并**落库之后** fire 一次
	// （fromActor/toActor/operator 直传载荷，监听器免反查参与者表）。
	f.engine.FireEvent(engine.ProcessEvent{
		Type: engine.EventTaskTransfer, InstanceID: task.ProcessInstanceID, TaskID: task.ID,
		NodeID: task.TaskName, Operator: operator, FromActor: fromActor, ToActor: toActor,
	})
	return nil
}

// taskRemoveActor 摘除参与人（issues/115 残留 · 门面第 47 个 action，
// spec 06-facade.md §processTask/removeTaskActor）。SPI 侧 RemoveTaskActor 早就是必选方法、
// 两仓都实现，本 action 补的只是"上门面"这一段（Java 基准 taskRemoveActor 同批）。
//
// 三个兄弟 action 的分工先钉死，免得后来人把三条混用：
//   - processTask/surrogate、processTask/addCandidate（本栈同体，都落 taskAddActor）＝**只加**；
//   - processTask/transfer＝**换人**（摘 A 并加 B，写 submitType=7 + tf_transferHistory 留痕 + fire 码 7）；
//   - 本 action＝**只摘不加、零留痕**：删掉 actorIds 在本任务的参与者行，不新建任务、不写任何任务变量、
//     不覆写任务 actor_id/operator 列，也**不 fire 事件**——issues/132 §11.3 定稿的事件集里没有"摘人"
//     这一码，码 7 TASK_TRANSFER 的语义是"参与者被替换"，只摘不加却发码 7 等于把没发生的转办写进事件流。
//
// 守卫次序（spec 同节末尾钉死，逐栈一致，门禁按 msg 断言，不接受本栈自行重排）：
// operator 必填 → processTaskId/actorIds 缺失 → 任务不存在 → 无权限 → 非进行中 → 摘空下限 → 落库。
// operator 排在最前：参数全缺时若先报缺参数，鉴权缺口会被参数报错藏起来。
func (f *Facade) taskRemoveActor(args map[string]interface{}) error {
	// operator 硬必填（严禁缺省回落 user1，与 transfer/withdraw 同口径）。归一走本栈既有单点
	// singleActorArg → spi.NormalizeActors（trim＋丢空＋折叠），不另抄第二把尺子（§2.11）。
	operator := singleActorArg(args["operator"])
	if operator == "" {
		return errors.New("operator 必填")
	}
	// 缺参数档文案是本 action 的**专属**逐字串 `processTaskId/actorIds 缺失`（spec 语义 8：
	// 缺键/空串/纯空白/0/负数同一句），**不是** taskIDArg 那句「processTaskId 缺失或非法」；
	// 主键的**判据**仍复用 taskIDArg（空串/非数值/显式 0/负数都在那里响亮报错，
	// §2.11「主键类参数另判一档」），只把 msg 折进本族文案。
	// ⚠️ 唯一不折的一档：超 2^53 的 float 是"精度已经丢了"另一种事实，必须原样透出
	// taskIDArg 的既有报错——java `toLong`、node `toId`、python `_to_int` 都留着这条护栏
	// （issues/38 E9·82）；把它伪装成"参数没传"，调用方就看不出自己传的是浮点雪花 id。
	// actorIds 两形（数组/逗号串）过同一枚单点 toStringSlice2；归一后丢完为空 ⇒ 同一条文案。
	// 两条都不落库，空串元素也绝不会被喂进 DELETE（历史 actor_id='' 脏行因此安全）。
	taskID, err := taskIDArg(args)
	if err != nil {
		if strings.Contains(err.Error(), "超出 float64 精确范围") {
			return err
		}
		return errors.New("processTaskId/actorIds 缺失")
	}
	// 负数那一档 taskIDArg 管不到（它只显性拒 0，`toInt64(-1)` 是合法 int64）⇒ 在这里补判一次。
	// 不加这一句，负数会一路走到 FindTaskByID 再落「任务不存在」，与 spec 语义 8 的
	// 「缺键/空串/纯空白/0/负数同一句缺参数文案」分叉（兄弟 action 沿用 taskIDArg 现状，不回改）。
	if taskID < 0 {
		return errors.New("processTaskId/actorIds 缺失")
	}
	actors := toStringSlice2(args["actorIds"])
	if len(actors) == 0 {
		return errors.New("processTaskId/actorIds 缺失")
	}
	ctx := context.Background()
	task, err := f.repo.FindTaskByID(ctx, taskID)
	if err != nil || task == nil {
		return errors.New("任务不存在")
	}
	// 归属判据同 transfer：被摘集合必须含操作人本人（入参已归一，比较才咬得上），或 operator 是
	// flow.auto / flow.admin 哨兵（大小写不敏感沿用本栈既有 strings.EqualFold 写法）。
	// transfer 能"摘 A 加 B"是因为 A 就是操作人本人；本 action 不得成为借道摘他人的口子。
	targets := make(map[string]bool, len(actors))
	for _, a := range actors {
		targets[a] = true
	}
	if !targets[operator] &&
		!strings.EqualFold(operator, engine.KeyAutoExecute) &&
		!strings.EqualFold(operator, engine.KeyAdminID) {
		return errors.New("无权限摘除该任务参与人")
	}
	// 前置态：仅进行中（DOING=10）任务可摘人。已办结/废弃/撤回的历史参与人行是 approvalRecord 的
	// 取证依据（它读全状态任务行），摘它等于改写审批历史。
	if task.TaskState != model.TaskStateDoing {
		return errors.New("任务非进行中，不可摘除参与人")
	}
	// 以参与者关系表为判据（内存/SQL 两仓同源），任务副本 ActorIDs 一并去重纳入（仓储不水合时的兜底，
	// 与 transfer 同形状）——不这么做的话"库里没人"会被当成"摘空也没关系"，下限判据直接失效。
	current, err := f.repo.FindTaskActors(ctx, taskID)
	if err != nil {
		return err
	}
	rows := dedupStrs(append(append([]string{}, current...), task.ActorIDs...))
	// 语义 6「匹配取归一值、DELETE 取行上的原值」（§2.11 硬要求②的**删除腿**）：库里的行可能是
	// 修复前落下的未 trim 原值 " leader "，入参 "leader" 必须判成同一个人**并真删掉它**——所以
	// 匹配用归一形、喂给仓储的是那一行的原值。只拿归一值去 DELETE 会"判成同一人却一条没删"，
	// 门面报成功而被摘的人待办还在，是**假成功**（本栈 transfer 已按同一形状修过，见上面那段注释）。
	//
	// 语义 5「不得摘空」的下限按**能办单的人数**算（remaining 只数归一后非空的行）：历史
	// actor_id=''/纯空白脏行谁也办不了单，拿它撑住下限等于让"摘空"伪装成成功。
	// 判据是**集合差**（当前参与者 − 归一后入参），不是"入参条数"——否则 actorIds 里混进非参与者
	// 的 id 就能绕过这条下限。
	var toDelete []string
	remaining := 0
	for _, row := range rows {
		n := singleActorArg(row)
		if n == "" {
			continue // 归一后为空的历史脏行：既不匹配任何入参，也不计入"一个人"
		}
		if targets[n] {
			toDelete = append(toDelete, row)
			continue
		}
		remaining++
	}
	if len(toDelete) > 0 && remaining == 0 {
		return errors.New("至少需保留一名参与人")
	}
	// 语义 7「幂等」：一个都没命中 ⇒ 空操作、成功信封（前端双点/集成层重放第二次不再报错）。
	// 需要"人不在任务里就报错"请用 transfer（它有「原办理人不是该任务参与人」那档判据）。
	if len(toDelete) > 0 {
		if err := f.repo.RemoveTaskActor(ctx, taskID, toDelete); err != nil {
			return err
		}
	}
	return nil
}

// transferComment 转办末跳可读文案（tf_approvalComment 槽位，前端审批意见的既有读取位 issues/15）：
// 形如「A 转办给 B（原因…）」，无原因时「A 转办给 B」。
func transferComment(fromActor, toActor, reason string) string {
	r := strings.TrimSpace(reason)
	if r == "" {
		return fromActor + " 转办给 " + toActor
	}
	return fromActor + " 转办给 " + toActor + "（" + r + "）"
}

// appendTransferRecord tf_transferHistory 追加（只追加不覆盖，spec 06 §transfer 留痕②）。
// 既有值三种来源都要容错：本仓内存写入的 []interface{}、其他代码可能构造的 []map[string]interface{}、
// SQL 仓 variable 列 JSON 回读的 []interface{}（元素为 map[string]interface{}）。
// 统一归一为 []interface{}{map[string]interface{}{...}}——与 JSON 落地形态同构，便于七栈读到同一形状。
func appendTransferRecord(cur interface{}, rec map[string]interface{}) []interface{} {
	out := make([]interface{}, 0, 1)
	switch v := cur.(type) {
	case []interface{}:
		out = append(out, v...)
	case []map[string]interface{}:
		for _, m := range v {
			out = append(out, m)
		}
	}
	return append(out, rec)
}

// dedupStrs 去重保序（空串剔除）
func dedupStrs(list []string) []string {
	seen := make(map[string]bool, len(list))
	out := make([]string, 0, len(list))
	for _, s := range list {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// removeStr 剔除指定值（其余顺序不变）
func removeStr(list []string, s string) []string {
	out := make([]string, 0, len(list))
	for _, v := range list {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}

func (f *Facade) taskLatest(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return nil, fmt.Errorf("processInstanceId 缺失或非法: %v", err)
	}
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	if len(doing) == 0 {
		return nil, nil
	}
	t := doing[0]
	return map[string]interface{}{
		"id": t.ID, "taskName": t.TaskName, "displayName": t.DisplayName,
		"taskState": t.TaskState, "operator": t.ActorID,
	}, nil
}

// toStringSlice2 把 actorIds（数组或逗号串）转列表，**输出即归一后的集合**。
//
// issues/142 B 批（spec 06-facade.md §2.11「两形同判据」·owner 2026-09-30 拍
// 「八栈一起收：两形同判据＋写侧兜底＋trim＋哨兵」）：本函数的三个入参形态
// （[]string／[]interface{}／逗号串）**一律过同一枚判据** [spi.NormalizeActors]——
// 逐元素 trim、空串/纯空白丢弃、同次调用折叠、落库与比较取 trim 后的值。
// 改前三形三样尺子（普查底稿 issues/142 §2 B 表 go 那一行）：
//   - []string 不 trim、空串照收 ⇒ " 123 " 与 "123" 判成两个人、actor_id 为空串 直接落库；
//   - []interface{} 丢 nil 但 "" 照收、不 trim；
//   - 只有逗号串那一支才 TrimSpace＋丢空。
//
// issues/141 G10 附带的一档保留：[]interface{} 里的 **nil 元素**在这里就丢掉——以前
// fmt.Sprintf("%v", nil) 得到字面量 "<nil>"，归一判据（它只吃空串/纯空白）认不出来，
// 于是 `[null]` 这种 JSON 形状能带出一条 actor_id='<nil>' 的行。nil 元素本身就是
// "空元素"，与空串同档；引擎漏斗（engine.parseCcActors／engine.valueToActors）同判。
// **形态拆解**（怎么把 JSON 值摊成候选串）留在各调用点，**判据**只有 spi.NormalizeActors 那一枚
// ——两份判据迟早分叉（spec §2.11 结尾点名 php 那轮的实测教训）。
//
// ⚠️ 入参形态策略不变：只收数组与逗号串，裸标量（例如 JSON 数字解析成的 float64）仍返回
// 空集 ⇒ 调用方按"参数缺失"报错，不静默 Sprintf 成 "1.2345e+04" 这种花名。
//
// ⚠️ 反向哨兵：判据只吃空值，"0" 是有效参与者不得被丢掉，"0" 与 "00" 是两个不同的人。
func toStringSlice2(v interface{}) []string {
	var raw []string
	switch t := v.(type) {
	case []string:
		raw = append(raw, t...)
	case []interface{}:
		for _, s := range t {
			if s == nil {
				continue // nil 元素＝空元素，绝不串化成 "<nil>"
			}
			raw = append(raw, fmt.Sprintf("%v", s))
		}
	case string:
		raw = append(raw, strings.Split(t, ",")...)
	}
	return spi.NormalizeActors(raw...)
}

// singleActorArg 单人归属入参归一（spec 06-facade.md §2.11 写点表：transfer 的
// fromActor/toActor、updateCCStatus 的 operator）：**走与集合侧同一枚判据**
// [spi.NormalizeActors]，返回 trim 后的值；空串/纯空白/nil ⇒ 空串，由调用方按
// 自己既有的"必填"档报错（§2.11 硬要求③「沿用各栈既有的缺参数错误信封，不新造错误码」）。
func singleActorArg(v interface{}) string {
	if list := spi.NormalizeActors(toStr(v, "")); len(list) > 0 {
		return list[0]
	}
	return ""
}

// ═══ 工具 ═══

func (f *Facade) ext() spi.ProcessExtRepository {
	if f.extRepo == nil {
		panic("未配置 ProcessExtRepository（扩展仓储）")
	}
	return f.extRepo
}

func contentBytes(args map[string]interface{}) ([]byte, error) {
	content, ok := args["content"]
	if !ok || content == nil {
		// issues/31：兼容 boot3 顶层 JSON（无 content 字段）——非保留字段序列化为内容快照
		copy := make(map[string]interface{}, len(args))
		for k, v := range args {
			if k != "processDesignId" && k != "operator" {
				copy[k] = v
			}
		}
		if len(copy) == 0 {
			return nil, errors.New("content 缺失")
		}
		bs, err := json.Marshal(copy)
		if err != nil {
			return nil, errors.New("content 序列化失败")
		}
		return bs, nil
	}
	switch v := content.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case map[string]interface{}, []interface{}:
		// content 为对象（前端直接传 JSON 对象）：序列化为 JSON
		bs, err := json.Marshal(v)
		if err != nil {
			return nil, errors.New("content 序列化失败")
		}
		return bs, nil
	default:
		return nil, errors.New("content 必须是字符串或字节数组")
	}
}

func pageData(pageNum, pageSize, total int, rows interface{}) map[string]interface{} {
	totalPage := 0
	if total > 0 {
		totalPage = (total + pageSize - 1) / pageSize
	}
	return map[string]interface{}{
		"pageNum": pageNum, "pageSize": pageSize,
		"recordCount": total, "totalPage": totalPage, "rows": rows,
	}
}

func okResult(data interface{}) map[string]interface{} {
	return map[string]interface{}{"code": 0, "msg": "成功", "data": data}
}

// ═══ 出口 id stringify（issues/38 E9） ═══════════════════════════════════════

// isIDKey id 类字段名判定（对齐 Java 实体 id 命名）：精确 'id' 或以 'Id' 结尾
// （processDefineId/processInstanceId/processTaskId/processDesignId/parentId/taskParentId/...）
func isIDKey(k string) bool {
	return k == "id" || strings.HasSuffix(k, "Id")
}

// toIDString id 值转字符串：nil 保持 nil；字符串直通；数字转十进制字符串。
// 防御 float64（引擎内部理论不走此型）：仅整数值转换。
func toIDString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		if t == math.Trunc(t) {
			return strconv.FormatInt(int64(t), 10)
		}
	}
	return fmt.Sprintf("%v", v)
}

// derefValue 指针取背值（非指针原样返回；nil 指针给 nil，兜底 fmt 出 "<nil>" 与历史形状一致）。
// stringifyIDs 的 id 分支需要它：可空 id 列（*int64 的 task_parent_id / parent_id）不解引用就会被
// toIDString 兜底成**指针地址**（issues/121 P1 激活 task_parent_id 后暴露）。
func derefValue(v interface{}) interface{} {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return v
	}
	if rv.IsNil() {
		return nil
	}
	return rv.Elem().Interface()
}

// stringifyIDs 递归把返回结构中 id 类字段值统一转字符串（反射兼容
// map[string]interface{} / []interface{} / []map[string]interface{} 等真实类型）
func stringifyIDs(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return v
		}
		out := make(map[string]interface{}, rv.Len())
		for _, k := range rv.MapKeys() {
			ks := k.String()
			val := rv.MapIndex(k).Interface()
			if isIDKey(ks) {
				// id 键同样先解引用：*int64 这类可空 id 列（task_parent_id / parent_id）直接进
				// toIDString 会落到兜底 fmt.Sprintf("%v", ptr) 吐**指针地址**。issues/121 P1 把
				// task_parent_id 从"恒 nil"写成"恒有值"，正是这条路被踩中——VO 必须带真 id。
				out[ks] = toIDString(derefValue(val))
			} else {
				out[ks] = stringifyIDs(val)
			}
		}
		return out
	case reflect.Slice, reflect.Array:
		// []byte 原样（Content 等二进制字段，json 序列化为 base64）
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}
		out := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = stringifyIDs(rv.Index(i).Interface())
		}
		return out
	case reflect.Ptr:
		if rv.IsNil() {
			return v
		}
		return stringifyIDs(rv.Elem().Interface())
	case reflect.Struct:
		// issues/58 E30：结构体（含 *Struct 切片元素）转 map（json tag 名），
		// id 类字段字符串化——time.Time 等无导出字段的结构体原样返回
		hasExported := false
		for i := 0; i < rv.NumField(); i++ {
			if rv.Type().Field(i).IsExported() {
				hasExported = true
				break
			}
		}
		if !hasExported {
			return v
		}
		m := map[string]interface{}{}
		for i := 0; i < rv.NumField(); i++ {
			f := rv.Type().Field(i)
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag := f.Tag.Get("json"); tag != "" {
				if n := strings.Split(tag, ",")[0]; n != "" && n != "-" {
					name = n
				}
			}
			fv := rv.Field(i)
			val := stringifyIDs(fv.Interface())
			if isIDKey(name) {
				val = toIDString(fv.Interface())
			}
			m[name] = val
		}
		return m
	default:
		return v
	}
}

func errorResult(msg string) map[string]interface{} {
	return map[string]interface{}{"code": 99999999, "msg": msg}
}

// msgReadFlowDefineJSONFailed 流程定义 JSON 解析失败的**逐字固定**对外文案（issues/139）。
// 与 Java 参考实现同源：jeeflow-core ModelParser.java:47
// `throw new RuntimeException("读取流程定义 JSON 失败", e)` —— 门面出口
// JeeflowFacade.java:177 `error(e.getMessage())` 取到的就是这句固定文案，cause 不进 msg。
const msgReadFlowDefineJSONFailed = "读取流程定义 JSON 失败"

// facadeFixedError 对外 msg 与内部 cause **分离**的错误（issues/139）。
//
// 为什么不在包装文案上动手（`fmt.Errorf("固定文案: %w", err)` 做不到）：门面顶层出口取的就是
// `Error()`，Error() 里拼了什么，msg 字段就是什么——`%w` 只保证错误链可解，不让 Error() 变短。
// 要"msg 逐字固定、异常只留在错误对象链"，就得让 Error() 本身只返回固定文案，cause 靠 Unwrap
// 交给调用方/日志。这正好复刻 Java 那侧 getMessage()/getCause() 的分工。
//
// issues/137 §3-1（spec 06-facade.md §2.12）起它还多一个身份：**判别式第 2 条「契约异常族」在 go 侧的
// 唯一落点**。go 没有异常类型，本模块也只有这一个具名 error 类型（核法：grep 锚定行首的
// `^func .*) Error\(\) string`，全仓只命中下面那一行；别用不锚定的形状去 grep，会命中注释里的转述
// 自造假阳性），而它的语义恰好就是"这段 msg 是引擎写给外面看的、原文只在链上"
// ⇒ isForeignDetail 见到它一律逐字透出、
// **且不下探它的 cause**（cause 里躺着的正是 *json.SyntaxError 那类外来类型，下探就会把
// msgReadFlowDefineJSONFailed 这条跨栈逐字契约文案误判成内部细节）。不另造第二个载体类型。
type facadeFixedError struct {
	msg   string
	cause error
}

func (e *facadeFixedError) Error() string { return e.msg }

// Unwrap 保留原始异常于错误链（errors.Is/As/Unwrap 均可取到），仅供调用方与日志使用。
func (e *facadeFixedError) Unwrap() error { return e.cause }

// fixedMsgError 以逐字固定的 msg 出口，cause 只进错误链不进 msg（issues/139）。
func fixedMsgError(msg string, cause error) error {
	return &facadeFixedError{msg: msg, cause: cause}
}

func toStr(v interface{}, def string) string {
	if v == nil {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// operatorArg 归属/操作人入参归一化（issues/129 案 A · spec 06-facade.md §2.5）——
// 与 Java JeeflowFacade.operatorArg 同形，别在这里发明第三种语义。
//
// 空串与**缺键同档**：传 ""（或全空白串）视同未传，一并回落 demo 缺省 user1。
// 以前 toStr(args["operator"], "user1") 只在**键缺失/为 null** 时兜缺省，于是
// {"operator":""} 原样落进仓储的归属参数 ⇒ 本栈内存仓把"空串"读成"不过滤"
// （memory/repository.go 的 `if actorID != ""`），"我的实例/我的已办"读出**全库**
// （实测 25 行 vs user1 的 4 行，行上是别人的 operator）。
// 门面归一化是第一层，仓储的归属兜底是第二层（memory/repository.go 四条 page +
// repository/jdbc 的 buildWhere 与四条 page），两层都要在——只修门面那半不算修完。
//
// ⚠️ 只归一"归属/操作人"这一族入参；issues/114 立的 operator **硬必填**族
// （processInstance/withdraw、processTask/transfer）严禁走这里——回落 user1 会把
// 撤回人/转办人静默记成别人，审计链失真。
func operatorArg(args map[string]interface{}) string {
	s := toStr(args["operator"], "")
	if strings.TrimSpace(s) == "" {
		return "user1"
	}
	return s
}

func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		// encoding/json 默认把 JSON 数字解析为 float64——只有 53 位尾数。
		// Java 雪花 id（≈2.08e18）> 2^53 时 float64 必然已丢精度，静默截断会产生
		// 错误 id（define not found: ...288）。显性报错，要求以字符串传递（issues/38 E9 对齐 Node）
		if math.Abs(t) > 1<<53 {
			return 0, fmt.Errorf("id %v 超出 float64 精确范围（2^53），请以字符串传递", t)
		}
		return int64(t), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	case nil:
		return 0, errors.New("缺失")
	default:
		return 0, fmt.Errorf("非法数值: %v", v)
	}
}

func toInt(v interface{}) (int, error) {
	n, err := toInt64(v)
	return int(n), err
}

func toIntDef(v interface{}, def int) int {
	if v == nil {
		return def
	}
	if n, err := toInt(v); err == nil {
		return n
	}
	return def
}

// ═══ 基础分页/详情（v1.5.0 补齐，对齐 Java 门面）═══

// definePage 流程定义分页
func (f *Facade) definePage(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	rows, total, err := f.repo.PageDefines(context.Background(), query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, defineRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// defineDetail 流程定义详情
func (f *Facade) defineDetail(args map[string]interface{}) (interface{}, error) {
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	def, err := f.repo.FindDefineByID(context.Background(), id)
	if err != nil || def == nil {
		return nil, errors.New("流程定义不存在")
	}
	graph := map[string]interface{}{}
	if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
		// 前端表单渲染/流程图依赖（issues/05）
		return map[string]interface{}{
			"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
			"type": def.Type, "state": def.State, "version": def.Version,
			"jsonObject": graph,
		}, nil
	}
	return map[string]interface{}{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
	}, nil
}

// instancePage 我发起的流程实例分页（operator 过滤）
func (f *Facade) instancePage(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	operator := operatorArg(args)
	rows, total, err := f.repo.PageInstances(context.Background(), query, operator)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, instanceRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// instanceDetail 流程实例详情（含任务列表）
func (f *Facade) instanceDetail(args map[string]interface{}) (interface{}, error) {
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), id)
	if err != nil || inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	instExt := inst.Variables
	if instExt == nil { // issues/124：变量为空的实例 ext 出 {} 而非 null
		instExt = map[string]interface{}{}
	}
	data := map[string]interface{}{
		"id": inst.ID, "parentId": inst.ParentID, "processDefineId": inst.DefineID,
		"state": inst.State, "parentNodeName": inst.ParentNodeName,
		"businessNo": inst.BusinessNo, "operator": inst.Operator,
		"ext": instExt, "formData": formDataOf(inst.Variables, "f_"), // issues/15 + issues/124：ext 是变量唯一对外出口
		"createTime": inst.CreateTime, "createUser": inst.CreateUser,
	}
	var graph map[string]interface{}
	if def0, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID); def0 != nil {
		data["displayName"] = def0.DisplayName // issues/15
		data["name"] = def0.Name
		data["version"] = def0.Version
		graph = map[string]interface{}{}
		if json.Unmarshal(def0.Content, &graph) == nil && len(graph) > 0 {
			data["jsonObject"] = graph // issues/05
		}
	}
	// 任务列表（issues/05-4）：全量 tasks + activeTaskList（仅 DOING）+ 任务行 ext/isFirstTaskNode
	firstTaskNodeID := firstTaskNodeIDOf(graph)
	tasks := make([]map[string]interface{}, 0, len(inst.Tasks))
	activeTaskList := make([]map[string]interface{}, 0)
	for _, t := range inst.Tasks {
		vo := f.taskVo(t)
		ext := map[string]interface{}{}
		for k, v := range t.Variables {
			ext[k] = v
		}
		doing := t.TaskState == model.TaskStateDoing
		// issues/121 P1：行上值优先（引擎建单时写入，已办结的历史行同样有效）；缺键（存量行）才回退
		// 现算——现算带"仅进行中"判定，只够展示用，不能当引擎判据（issues/121 §4）
		rowFirst, hasRowFirst := rowFirstTaskNode(ext)
		if !hasRowFirst {
			rowFirst = doing && t.TaskName == firstTaskNodeID
		}
		ext[engine.KeyIsFirstTaskNode] = rowFirst
		vo["ext"] = ext
		tasks = append(tasks, vo)
		if doing {
			activeTaskList = append(activeTaskList, vo)
		}
	}
	data["tasks"] = tasks
	data["activeTaskList"] = activeTaskList
	return data, nil
}

// rowFirstTaskNode 读出口的行上值（issues/121 P1「行上值优先、缺键才回退现算」的前半）：
// 返回 (行上布尔值, 行上是否有值)。键缺失/为 null（存量老行，引擎还没写标记时落的数据）⇒ 第二返回 false，
// 调用方必须回退现算，不能把"未定义"读成 false。值形态兼容 JSON 反序列化后的 bool 与字符串 "true"。
func rowFirstTaskNode(ext map[string]interface{}) (bool, bool) {
	v, ok := ext[engine.KeyIsFirstTaskNode]
	if !ok || v == nil {
		return false, false
	}
	switch b := v.(type) {
	case bool:
		return b, true
	case string:
		return strings.EqualFold(strings.TrimSpace(b), "true"), true
	case json.Number:
		n, _ := b.Int64()
		return n != 0, true
	}
	return false, true
}

// firstTaskNodeIDOf 流程 JSON 中第一个任务节点 id（issues/05-4 isFirstTaskNode 用）
func firstTaskNodeIDOf(graph map[string]interface{}) string {
	nodes, _ := graph["nodes"].([]interface{})
	for _, n := range nodes {
		node, _ := n.(map[string]interface{})
		if node != nil && node["type"] == "snaker:task" {
			id, _ := node["id"].(string)
			return id
		}
	}
	return ""
}

// todoList 我的待办分页（operator 作为抄送…待办人过滤，对齐 Java pta.actor_id EQ）
func (f *Facade) todoList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	actorID := operatorArg(args)
	rows, total, err := f.repo.PageTodoTasks(context.Background(), query, actorID)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, taskRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// doneList 我的已办分页（operator 过滤，非进行中任务）
func (f *Facade) doneList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10), Conditions: parseMQuery(args)}
	operator := operatorArg(args)
	rows, total, err := f.repo.PageDoneTasks(context.Background(), query, operator)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, taskRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// taskVo 任务行 VO（instanceDetail 任务列表用，对齐 Java taskVo）
func (f *Facade) taskVo(t *model.ProcessTask) map[string]interface{} {
	return map[string]interface{}{
		"id": t.ID, "processInstanceId": t.ProcessInstanceID, "taskName": t.TaskName,
		"displayName": t.DisplayName, "taskType": t.TaskType, "performType": t.PerformType,
		"taskState": t.TaskState, "operator": t.ActorID, "finishTime": t.FinishTime,
		"expireTime": t.ExpireTime, "formKey": t.FormKey, "taskParentId": t.ParentTaskID,
		"createTime": t.CreateTime, "createUser": t.CreateUser,
		"updateTime": t.UpdateTime, "updateUser": t.UpdateUser, "taskActorIdList": t.ActorIDs,
		"taskFormData": formDataOf(t.Variables, "tf_"), // issues/15
	}
}

// parseMQuery m_ 前缀查询参数解析（issues/05-5，对齐 Java JeeflowQueryParser）：
// m_EQ_taskName → t.task_name EQ；m_pd_LIKE_displayName → pd.display_name LIKE
func parseMQuery(args map[string]interface{}) []spi.Condition {
	var out []spi.Condition
	for key, value := range args {
		if !strings.HasPrefix(key, "m_") {
			continue
		}
		if value == nil {
			continue
		}
		if sv, ok := value.(string); ok && sv == "" {
			continue
		}
		parts := strings.Split(key[2:], "_")
		if len(parts) < 2 {
			continue
		}
		var column, operator string
		if len(parts) == 2 {
			// 无别名 → 默认主表别名 t（对齐 Java，白名单列均带表别名）
			operator = parts[0]
			column = "t." + toUnderscore(parts[1])
		} else {
			operator = parts[1]
			column = parts[0] + "." + toUnderscore(parts[2])
		}
		out = append(out, spi.Condition{Column: column, Operator: strings.ToUpper(operator), Value: value})
	}
	return out
}

func toUnderscore(camel string) string {
	var b strings.Builder
	for _, c := range camel {
		if c >= 'A' && c <= 'Z' {
			b.WriteByte('_')
			b.WriteRune(c + 32)
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// formDataOf issues/15：取 vars 中 prefix 前缀字段，输出「带前缀 + 去前缀副本」（对齐 boot3 getFormData/getTaskFormData）
func formDataOf(vars map[string]interface{}, prefix string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range vars {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
			out[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return out
}

// ═══ 行输出转换（issues/05-2 字段契约 + 05-3 时间格式）═══

const timeFmt = "2006-01-02 15:04:05"

func fmtTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.Format(timeFmt)
}

func fmtTimeV(t time.Time) string {
	return t.Format(timeFmt)
}

// defineRowToMap 定义行：时间格式化
func defineRowToMap(r *model.DefineRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "name": r.Name, "displayName": r.DisplayName, "type": nullOut(r.Type),
		"state": r.State, "version": r.Version,
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
	}
}

// nullOut 把分页行 DTO 的可空字符串列（*string）投成出口值：nil → JSON null，
// 有值 → 原串（与改动前逐字节一致）。遵 spec 06 §2.4「可空列合法出口为 null/缺键，'' 违规」，
// 绝不把 SQL NULL 伪造成空串或 Go 零值串化（"<nil>"）（issues/141 G3）。
func nullOut(p *string) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

// designRowToMap 设计行：时间格式化（issues/63）
func designRowToMap(r *model.ProcessDesign) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "name": r.Name, "displayName": r.DisplayName,
		"type": r.Type, "icon": r.Icon, "isDeployed": r.IsDeployed, "remark": r.Remark,
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
	}
}

// instanceRowToMap 实例行：ext（实例变量对象）+ displayName/version（定义）
func instanceRowToMap(r *model.InstanceRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "parentId": r.ParentID, "processDefineId": r.DefineID,
		"state": r.State, "parentNodeName": r.ParentNodeName, "businessNo": nullOut(r.BusinessNo),
		"operator": nullOut(r.Operator), "expireTime": fmtTime(r.ExpireTime),
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": nullOut(r.DefineName), "processDefineDisplayName": nullOut(r.DefineDisplayName),
		"processDefineVersion": r.DefineVersion,
		"ext":                  r.Variables, "displayName": nullOut(r.DefineDisplayName), "version": r.DefineVersion,
	}
}

// taskRowToMap 任务行：ext（任务变量，空回退实例变量）+ instanceExt + version
func taskRowToMap(r *model.TaskRow) map[string]interface{} {
	instanceExt := parseVarMap(r.InstanceVariable)
	ext := r.Variables
	// issues/121 P1：引擎建单必写的控制键不算「任务变量非空」，否则新建任务的 ext
	// 永远不再回退实例变量（issues/82-3 既有契约）。
	if len(ext) == 0 || (len(ext) == 1 && ext[model.IsFirstTaskNodeKey] != nil) {
		ext = instanceExt
	}
	return map[string]interface{}{
		"id": r.ID, "processInstanceId": r.ProcessInstanceID, "taskName": r.TaskName,
		"displayName": r.DisplayName, "taskType": r.TaskType, "performType": r.PerformType,
		"taskState": r.TaskState, "operator": nullOut(r.Operator), "finishTime": fmtTime(r.FinishTime),
		"expireTime": fmtTime(r.ExpireTime), "formKey": nullOut(r.FormKey), "taskParentId": r.TaskParentID,
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": nullOut(r.ProcessDefineName), "processDefineDisplayName": nullOut(r.ProcessDefineDisplayName),
		"instanceCreateTime": fmtTimeV(r.InstanceCreateTime),
		"ext":                ext, "instanceExt": instanceExt, "version": r.DefineVersion,
		"taskFormData": formDataOf(r.Variables, "tf_"), // issues/15
	}
}

// ccRowToMap 抄送行：ext（实例变量对象）+ displayName/version（定义）
func ccRowToMap(r *model.CcInstanceRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "parentId": r.ParentID, "processDefineId": r.DefineID,
		"state": r.State, "parentNodeName": r.ParentNodeName, "businessNo": nullOut(r.BusinessNo),
		"operator": nullOut(r.Operator), "expireTime": fmtTime(r.ExpireTime),
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": nullOut(r.DefineName), "processDefineDisplayName": nullOut(r.DefineDisplayName),
		"processDefineVersion": r.DefineVersion,
		"ext":                  r.Variables, "displayName": nullOut(r.DefineDisplayName), "version": r.DefineVersion,
	}
}

// parseVarMap JSON 字符串 → map（坏 JSON 返回空 map）
func parseVarMap(s string) map[string]interface{} {
	m := map[string]interface{}{}
	if s == "" {
		return m
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

// idListArgs 删除/启停类 action 的批量主键：mldong IdsParam 惯例下 {ids} 数组优先，
// 兼容单 {id}；两者皆缺失、空数组或含非法值一律报错（issues/95，对齐 Java idListArgs）。
func idListArgs(args map[string]interface{}) ([]int64, error) {
	if raw, ok := asList(args["ids"]); ok {
		if len(raw) == 0 {
			return nil, fmt.Errorf("id 缺失或非法")
		}
		out := make([]int64, 0, len(raw))
		for _, v := range raw {
			id, err := toInt64(v)
			if err != nil {
				return nil, fmt.Errorf("id 缺失或非法: %v", err)
			}
			out = append(out, id)
		}
		return out, nil
	}
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, fmt.Errorf("id 缺失或非法: %v", err)
	}
	return []int64{id}, nil
}

// asList 宽松取列表（{ids: [...]} 或单值）
func asList(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case []int64:
		out := make([]interface{}, len(t))
		for i, x := range t {
			out[i] = x
		}
		return out, true
	case []int:
		out := make([]interface{}, len(t))
		for i, x := range t {
			out[i] = x
		}
		return out, true
	case []string:
		out := make([]interface{}, len(t))
		for i, x := range t {
			out[i] = x
		}
		return out, true
	}
	return nil, false
}

// firstNonNil 取第一个非 nil 参数
func firstNonNil(vals ...interface{}) interface{} {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

// ═══════════════════════════════════════
// 统计 3 动作（v1.8.25，issues/103）
// ═══════════════════════════════════════

var (
	defaultStateIn    = []int{10, 20, 30, 40, 45, 50}
	defaultStatsLimit = 10
	validGranularity  = map[string]bool{"hour": true, "day": true, "week": true, "month": true}
	validDimension    = map[string]bool{
		"state": true, "define": true, "category": true,
		"approver": true, "applicant": true, "node": true,
		"stuckNode": true, "stuckApprover": true, "durationBucket": true,
	}
)

// statsOverview 总览统计
func (f *Facade) statsOverview(args map[string]interface{}) (interface{}, error) {
	ctx := context.Background()
	start := parseSurrogateTime(args["start"])
	end := parseSurrogateTime(args["end"])

	// B：stateIn 入参（int[]，缺省 defaultStateIn），作用于六个状态计数
	stateIn := parseStateIn(args["stateIn"])
	if stateIn == nil {
		stateIn = defaultStateIn
	}
	allInst, err := f.repo.QueryInstancesForStats(ctx, stateIn, "create_time", start, end)
	if err != nil {
		return nil, err
	}
	instByState := map[int]int{}
	for _, r := range allInst {
		instByState[r.State]++
	}
	total := len(allInst)
	inProgress := instByState[int(model.InstanceStateDoing)]
	completed := instByState[int(model.InstanceStateDone)]
	withdrawn := instByState[int(model.InstanceStateWithdraw)]
	rejected := instByState[int(model.InstanceStateReject)]
	suspended := instByState[int(model.InstanceStatePending)]

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	todayEnd := todayStart.AddDate(0, 0, 1)
	// E：todayNew 恒按服务器当日、不过滤 state / 不受 stateIn 影响（对齐内置线 countTodayNew）
	todayInst, err := f.repo.QueryInstancesForStats(ctx, nil, "create_time", &todayStart, &todayEnd)
	if err != nil {
		return nil, err
	}
	todayNew := len(todayInst)

	pending, overdue, err := f.repo.StatsPendingAndOverdueCount(ctx)
	if err != nil {
		return nil, err
	}

	taskTotal, countersign, onTime, onTimeDenom, err := f.repo.StatsCompletedTaskAggregate(ctx)
	if err != nil {
		return nil, err
	}
	countersignRate := 0.0
	if taskTotal > 0 {
		countersignRate = statsRound4(float64(countersign) / float64(taskTotal))
	}
	onTimeRate := 0.0
	if onTimeDenom > 0 {
		onTimeRate = statsRound4(float64(onTime) / float64(onTimeDenom))
	}

	avgDurationSeconds, err := f.repo.StatsAvgCompletedDurationSeconds(ctx, start, end)
	if err != nil {
		return nil, err
	}

	rejectRate := statsRound4(float64(rejected) / float64(max(1, completed+rejected)))

	return map[string]interface{}{
		"total":              total,
		"inProgress":         inProgress,
		"completed":          completed,
		"rejected":           rejected,
		"withdrawn":          withdrawn,
		"suspended":          suspended,
		"todayNew":           todayNew,
		"avgDurationSeconds": avgDurationSeconds,
		"rejectRate":         rejectRate,
		"pendingTaskCount":   pending,
		"overdueTaskCount":   overdue,
		"countersignRate":    countersignRate,
		"onTimeRate":         onTimeRate,
	}, nil
}

// statsTrend 趋势统计
func (f *Facade) statsTrend(args map[string]interface{}) (interface{}, error) {
	ctx := context.Background()
	start := parseSurrogateTime(args["start"])
	end := parseSurrogateTime(args["end"])
	granularity := toStr(args["granularity"], "")
	// C：start/end/granularity 均必填（对齐内置线 20010012 缺参语义），不静默回退默认桶
	if start == nil || end == nil || granularity == "" {
		return nil, fmt.Errorf("trend 缺少必填参数：start/end/granularity")
	}
	if !validGranularity[granularity] {
		return nil, fmt.Errorf("granularity 参数非法，允许值：hour/day/week/month")
	}

	// 实例侧无 state 过滤（对齐内置线 countInstanceStartedByBucket）
	insts, err := f.repo.QueryInstancesForStats(ctx, nil, "create_time", start, end)
	if err != nil {
		return nil, err
	}

	doneState := int(model.TaskStateDone)
	finishedTasks, err := f.repo.QueryTasksForStats(ctx, &doneState, start, end)
	if err != nil {
		return nil, err
	}

	buckets := statsEnumerateBuckets(start, end, granularity)
	type bucketCounts struct{ started, finished int }
	bucketMap := map[string]*bucketCounts{}
	for _, b := range buckets {
		bucketMap[b] = &bucketCounts{}
	}
	for _, row := range insts {
		bk := statsBucketKey(row.CreateTime, granularity)
		if bc, ok := bucketMap[bk]; ok {
			bc.started++
		}
	}
	for _, row := range finishedTasks {
		if row.FinishTime == nil {
			continue
		}
		bk := statsBucketKey(*row.FinishTime, granularity)
		if bc, ok := bucketMap[bk]; ok {
			bc.finished++
		}
	}

	series := make([]map[string]interface{}, 0, len(buckets))
	for _, b := range buckets {
		bc := bucketMap[b]
		series = append(series, map[string]interface{}{
			"bucket":   b,
			"started":  bc.started,
			"finished": bc.finished,
		})
	}
	// A：data 本体为裸数组（去掉 {granularity, series} 包装，对齐契约 spec 06 §4.2 / 内置线）
	return series, nil
}

// statsGroup 分组统计
func (f *Facade) statsGroup(args map[string]interface{}) (interface{}, error) {
	ctx := context.Background()
	start := parseSurrogateTime(args["start"])
	end := parseSurrogateTime(args["end"])
	dimension := toStr(args["dimension"], "define")
	limit := toIntDef(args["limit"], defaultStatsLimit)
	if !validDimension[dimension] {
		return nil, fmt.Errorf("dimension 参数非法，允许值：state/define/category/approver/applicant/node/stuckNode/stuckApprover/durationBucket")
	}

	var rows []map[string]interface{}

	switch dimension {
	case "define":
		rawRows, err := f.repo.StatsDefineGroup(ctx, start, end, limit)
		if err != nil {
			return nil, err
		}
		rows = rawRows

	case "state":
		insts, err := f.repo.QueryInstancesForStats(ctx, nil, "create_time", start, end)
		if err != nil {
			return nil, err
		}
		grouped := map[int]int{}
		for _, r := range insts {
			grouped[r.State]++
		}
		type kv struct {
			key   string
			count int
		}
		entries := make([]kv, 0, len(grouped))
		for k, v := range grouped {
			entries = append(entries, kv{strconv.Itoa(k), v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].count > entries[j].count })
		if len(entries) > limit {
			entries = entries[:limit]
		}
		rows = make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			rows = append(rows, map[string]interface{}{
				"key": e.key, "label": nil, "count": e.count, "avgDurationSeconds": nil,
			})
		}

	case "category":
		insts, err := f.repo.QueryInstancesForStats(ctx, nil, "create_time", start, end)
		if err != nil {
			return nil, err
		}
		defineTypes := map[int64]string{}
		for _, r := range insts {
			if _, ok := defineTypes[r.DefineID]; !ok {
				def, err := f.repo.FindDefineByID(ctx, r.DefineID)
				if err != nil {
					return nil, err
				}
				if def != nil {
					defineTypes[r.DefineID] = def.Type
				} else {
					defineTypes[r.DefineID] = ""
				}
			}
		}
		grouped := map[string]int{}
		for _, r := range insts {
			tp := defineTypes[r.DefineID]
			grouped[tp]++
		}
		type kv struct {
			key   string
			count int
		}
		entries := make([]kv, 0, len(grouped))
		for k, v := range grouped {
			entries = append(entries, kv{k, v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].count > entries[j].count })
		if len(entries) > limit {
			entries = entries[:limit]
		}
		rows = make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			rows = append(rows, map[string]interface{}{
				"key": e.key, "label": nil, "count": e.count, "avgDurationSeconds": nil,
			})
		}

	case "approver":
		doneState := int(model.TaskStateDone)
		tasks, err := f.repo.QueryTasksForStats(ctx, &doneState, start, end)
		if err != nil {
			return nil, err
		}
		grouped := map[string]int{}
		for _, r := range tasks {
			if r.Operator == "" {
				continue
			}
			grouped[r.Operator]++
		}
		type kv struct {
			key   string
			count int
		}
		entries := make([]kv, 0, len(grouped))
		for k, v := range grouped {
			entries = append(entries, kv{k, v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].count > entries[j].count })
		if len(entries) > limit {
			entries = entries[:limit]
		}
		rows = make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			rows = append(rows, map[string]interface{}{
				"key": e.key, "label": nil, "count": e.count, "avgDurationSeconds": nil,
			})
		}

	case "applicant":
		insts, err := f.repo.QueryInstancesForStats(ctx, nil, "create_time", start, end)
		if err != nil {
			return nil, err
		}
		grouped := map[string]int{}
		for _, r := range insts {
			if r.Operator == "" {
				continue
			}
			grouped[r.Operator]++
		}
		type kv struct {
			key   string
			count int
		}
		entries := make([]kv, 0, len(grouped))
		for k, v := range grouped {
			entries = append(entries, kv{k, v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].count > entries[j].count })
		if len(entries) > limit {
			entries = entries[:limit]
		}
		rows = make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			rows = append(rows, map[string]interface{}{
				"key": e.key, "label": nil, "count": e.count, "avgDurationSeconds": nil,
			})
		}

	case "node":
		doneState := int(model.TaskStateDone)
		tasks, err := f.repo.QueryTasksForStats(ctx, &doneState, start, end)
		if err != nil {
			return nil, err
		}
		type nodeAgg struct {
			count    int
			totalDur int64
		}
		grouped := map[string]*nodeAgg{}
		for _, r := range tasks {
			if r.DisplayName == "" {
				continue
			}
			var dur int64
			if r.FinishTime != nil && r.CreateTime != nil {
				dur = int64(r.FinishTime.Sub(*r.CreateTime).Seconds())
			}
			agg := grouped[r.DisplayName]
			if agg == nil {
				agg = &nodeAgg{}
				grouped[r.DisplayName] = agg
			}
			agg.count++
			agg.totalDur += dur
		}
		type kv struct {
			key string
			agg *nodeAgg
		}
		entries := make([]kv, 0, len(grouped))
		for k, v := range grouped {
			entries = append(entries, kv{k, v})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].agg.count > entries[j].agg.count })
		if len(entries) > limit {
			entries = entries[:limit]
		}
		rows = make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			var avg interface{}
			if e.agg.count > 0 {
				avg = int(math.Round(float64(e.agg.totalDur) / float64(e.agg.count)))
			}
			rows = append(rows, map[string]interface{}{
				"key": e.key, "label": nil, "count": e.agg.count, "avgDurationSeconds": avg,
			})
		}

	case "stuckNode":
		rawRows, err := f.repo.StatsStuckNodeGroup(ctx, limit)
		if err != nil {
			return nil, err
		}
		rows = rawRows
		for _, m := range rows {
			if _, ok := m["label"]; !ok {
				m["label"] = nil
			}
			if _, ok := m["avgDurationSeconds"]; !ok {
				m["avgDurationSeconds"] = nil
			}
		}

	case "stuckApprover":
		rawRows, err := f.repo.StatsStuckApproverGroup(ctx, limit)
		if err != nil {
			return nil, err
		}
		rows = rawRows
		for _, m := range rows {
			if _, ok := m["label"]; !ok {
				m["label"] = nil
			}
			if _, ok := m["avgDurationSeconds"]; !ok {
				m["avgDurationSeconds"] = nil
			}
		}

	case "durationBucket":
		durations, err := f.repo.StatsCompletedInstanceDurations(ctx, start, end)
		if err != nil {
			return nil, err
		}
		var sameDay, d1to3, d3to7, over7d int
		for _, dur := range durations {
			switch {
			case dur < 86400:
				sameDay++
			case dur < 259200:
				d1to3++
			case dur < 604800:
				d3to7++
			default:
				over7d++
			}
		}
		keys := []string{"sameDay", "1to3d", "3to7d", "over7d"}
		counts := []int{sameDay, d1to3, d3to7, over7d}
		rows = make([]map[string]interface{}, len(keys))
		for i := range keys {
			rows[i] = map[string]interface{}{
				"key": keys[i], "label": nil, "count": counts[i], "avgDurationSeconds": nil,
			}
		}
	}

	// A：data 本体为裸数组（去掉 {dimension, rows} 包装，对齐契约 spec 06 §4.2 / 内置线）
	return rows, nil
}

// ── 统计 helper ──

// parseStateIn 解析 stateIn 入参（int 切片；兼容 JSON []interface{} 数字 / []int / 数字字符串）
func parseStateIn(v interface{}) []int {
	switch t := v.(type) {
	case []int:
		if len(t) == 0 {
			return nil
		}
		return t
	case []interface{}:
		out := make([]int, 0, len(t))
		for _, e := range t {
			switch n := e.(type) {
			case int:
				out = append(out, n)
			case int64:
				out = append(out, int(n))
			case float64:
				out = append(out, int(n))
			case string:
				if i, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
					out = append(out, i)
				}
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return nil
}

// statsEnumerateBuckets 枚举连续时间桶标签列表
func statsEnumerateBuckets(start, end *time.Time, granularity string) []string {
	now := time.Now()
	s := now.AddDate(0, 0, -30)
	e := now
	if start != nil {
		s = *start
	}
	if end != nil {
		e = *end
	}

	var buckets []string
	switch granularity {
	case "hour":
		cursor := time.Date(s.Year(), s.Month(), s.Day(), s.Hour(), 0, 0, 0, s.Location())
		for !cursor.After(e) {
			buckets = append(buckets, cursor.Format("2006-01-02 15:00"))
			cursor = cursor.Add(time.Hour)
		}
	case "day":
		sd := time.Date(s.Year(), s.Month(), s.Day(), 0, 0, 0, 0, s.Location())
		ed := time.Date(e.Year(), e.Month(), e.Day(), 0, 0, 0, 0, e.Location())
		for !sd.After(ed) {
			buckets = append(buckets, sd.Format("2006-01-02"))
			sd = sd.AddDate(0, 0, 1)
		}
	case "week":
		sd := time.Date(s.Year(), s.Month(), s.Day(), 0, 0, 0, 0, s.Location())
		weekday := sd.Weekday()
		offset := int(weekday) - 1
		if offset < 0 {
			offset = 6
		}
		sd = sd.AddDate(0, 0, -offset)
		ed := time.Date(e.Year(), e.Month(), e.Day(), 0, 0, 0, 0, e.Location())
		for !sd.After(ed) {
			buckets = append(buckets, statsWeekKey(sd))
			sd = sd.AddDate(0, 0, 7)
		}
	case "month":
		sd := time.Date(s.Year(), s.Month(), 1, 0, 0, 0, 0, s.Location())
		ed := time.Date(e.Year(), e.Month(), 1, 0, 0, 0, 0, e.Location())
		for !sd.After(ed) {
			buckets = append(buckets, sd.Format("2006-01"))
			sd = sd.AddDate(0, 1, 0)
		}
	}
	return buckets
}

// statsBucketKey 把 time.Time 按 granularity 转成桶标签
func statsBucketKey(t time.Time, granularity string) string {
	switch granularity {
	case "hour":
		return t.Format("2006-01-02 15:00")
	case "day":
		return t.Format("2006-01-02")
	case "week":
		return statsWeekKey(t)
	case "month":
		return t.Format("2006-01")
	default:
		return ""
	}
}

// statsWeekKey ISO 周标签：YYYY-Www
func statsWeekKey(t time.Time) string {
	year, week := t.ISOWeek()
	return fmt.Sprintf("%d-W%02d", year, week)
}

// statsRound4 四舍五入到 4 位小数
func statsRound4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
