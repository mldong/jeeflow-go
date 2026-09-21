// DDD 聚合根行为——对标 Java 版 domain/ProcessInstance + domain/ProcessTask
package model

import "time"

// 业务号变量 key（engine 包 KeyBusinessNo 同值）
const BusinessNoKey = "BUSINESS_NO"

// IsFirstTaskNodeKey 行变量「本行节点是否 start 直接后继」的键（engine 包 KeyIsFirstTaskNode 同值，
// 对齐 Java FlowConst.IS_FIRST_TASK_NODE / issues/121 P1）——建单时落库，门面出口行上值优先读它
const IsFirstTaskNodeKey = "isFirstTaskNode"

// ─── 聚合根：ProcessInstance ────────────────────────────────────────────────────

// NewProcessInstance 工厂方法——创建流程实例
func NewProcessInstance(id, defineID int64, operator string, vars map[string]interface{}, now time.Time) *ProcessInstance {
	inst := &ProcessInstance{
		ID: id, DefineID: defineID, State: InstanceStateDoing,
		Operator: operator, Variables: vars,
		CreateTime: now, UpdateTime: now, CreateUser: operator, UpdateUser: operator,
	}
	if v, ok := vars[BusinessNoKey]; ok {
		inst.BusinessNo = toString(v)
	}
	return inst
}

// CompleteTask 完成任务（子实体状态转换 + 实例变量合并）
func (p *ProcessInstance) CompleteTask(task *ProcessTask, operator string, vars map[string]interface{}, now time.Time) {
	task.Finish(operator, vars, now)
	p.Variables = vars
	p.UpdateTime = now
	p.UpdateUser = operator
}

// AbandonTask 废弃单个任务
func (p *ProcessInstance) AbandonTask(task *ProcessTask, now time.Time) {
	task.Abandon(now)
	p.UpdateTime = now
}

// AbandonAllDoing 废弃所有进行中任务，返回被废弃的任务列表（供调用方持久化）
func (p *ProcessInstance) AbandonAllDoing(now time.Time) []*ProcessTask {
	var abandoned []*ProcessTask
	for _, t := range p.Tasks {
		if t.IsDoing() {
			t.Abandon(now)
			abandoned = append(abandoned, t)
		}
	}
	p.UpdateTime = now
	return abandoned
}

// Finish 流程完成
func (p *ProcessInstance) Finish(now time.Time) {
	p.State = InstanceStateDone
	p.UpdateTime = now
}

// Reject 驳回流程
func (p *ProcessInstance) Reject(now time.Time) {
	p.State = InstanceStateReject
	p.UpdateTime = now
}

// Withdraw 撤回流程（issues/53 E25：withdraw 用 Withdraw(30)，与 reject 区分）
func (p *ProcessInstance) Withdraw(now time.Time) {
	p.State = InstanceStateWithdraw
	p.UpdateTime = now
}

// AddVariable 追加变量
func (p *ProcessInstance) AddVariable(vars map[string]interface{}) {
	for k, v := range vars {
		p.Variables[k] = v
	}
}

// GetDoingTasks 获取进行中任务
func (p *ProcessInstance) GetDoingTasks() []*ProcessTask {
	var result []*ProcessTask
	for _, t := range p.Tasks {
		if t.IsDoing() {
			result = append(result, t)
		}
	}
	return result
}

// GetDoneTasks 获取已完成任务
func (p *ProcessInstance) GetDoneTasks() []*ProcessTask {
	var result []*ProcessTask
	for _, t := range p.Tasks {
		if t.IsFinished() {
			result = append(result, t)
		}
	}
	return result
}

// IsAllTasksFinished 所有任务是否都已完成（用于 join 合并判断）
func (p *ProcessInstance) IsAllTasksFinished() bool {
	for _, t := range p.Tasks {
		if t.IsDoing() {
			return false
		}
	}
	return true
}

// CreateTask 创建任务（子实体工厂，引擎唯一建单入口）——performType：0 普通 / 1 会签（issues/52 E24 落库对齐 Java）
//
// 建单不变量（规范「引擎操作 04 · 退回上一步」/ issues/121 P1，对齐 Java ProcessTask.create）：
// 所有建任务路径（普通 / 会签 / 串行会签下一位 / 回退建新）都过本工厂，两条不变量在此强制兑现，漏不掉：
// ① 必写 ParentTaskID——值＝产生这条任务的那个"刚办结的任务"id（execution 上下文携带的当前任务 id）；
//
//	发起流程那条 execution 没有当前任务 ⇒ 调用方传 0，落库 0（不是 NULL，与规范 01 那列注释口径一致）。
//
// ② 必写行变量 IsFirstTaskNodeKey（bool）——该行节点是否为 start 直接后继。判据由调用方用**现成**的
//
//	EngineImpl.isFirstTaskNode（对齐 Java FlowUtil.isFirstTaskName）算好传入，本层不另造拓扑遍历。
//	落库理由：门面出口「行上值优先、缺键才回退现算」的回退版带 doing 判定 ⇒ 已办结历史行恒 false，
//	而血缘版回退要读那条历史行决定参与者，故标记必须随行存活（issues/121 §4）。
func (p *ProcessInstance) CreateTask(id int64, taskName, displayName, actor, operator, formKey string,
	now time.Time, parentTaskID int64, isFirstTaskNode bool, performType ...int) *ProcessTask {
	pt := 0
	if len(performType) > 0 {
		pt = performType[0]
	}
	parent := parentTaskID
	task := &ProcessTask{
		ID: id, ProcessInstanceID: p.ID,
		TaskName: taskName, DisplayName: displayName, TaskState: TaskStateDoing,
		ActorIDs: []string{actor},
		FormKey:  formKey, PerformType: pt,
		// 建单不变量 ①：血缘指针（0 也落库，不写 NULL）
		ParentTaskID: &parent,
		// 建单不变量 ②：首节点标记随行写入（会签簿记等后续变量只能并入、不得整体覆写）
		Variables:  map[string]interface{}{IsFirstTaskNodeKey: isFirstTaskNode},
		CreateTime: now, UpdateTime: now, CreateUser: operator, UpdateUser: operator,
	}
	p.Tasks = append(p.Tasks, task)
	return task
}

// ─── 子实体：ProcessTask ────────────────────────────────────────────────────────

// Finish 完成任务
func (t *ProcessTask) Finish(operator string, vars map[string]interface{}, now time.Time) {
	t.TaskState = TaskStateDone
	t.ActorID = operator
	t.FinishTime = &now
	t.UpdateTime = now
	t.UpdateUser = operator
	t.Variables = vars
}

// Abandon 废弃任务
func (t *ProcessTask) Abandon(now time.Time) {
	t.TaskState = TaskStateAbandoned
	t.UpdateTime = now
}

// Withdraw 随实例撤回任务（区别于 Abandon：撤回是发起人主动收回，废弃是引擎清理）
func (t *ProcessTask) Withdraw(now time.Time) {
	t.TaskState = TaskStateWithdraw
	t.UpdateTime = now
}

// IsDoing 是否进行中
func (t *ProcessTask) IsDoing() bool { return t.TaskState == TaskStateDoing }

// IsFinished 是否已完成
func (t *ProcessTask) IsFinished() bool { return t.TaskState == TaskStateDone }

// IsAllowed 操作人是否有权限处理该任务
func (t *ProcessTask) IsAllowed(operator string) bool {
	for _, a := range t.ActorIDs {
		if a == operator {
			return true
		}
	}
	return false
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
