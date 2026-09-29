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

// CreateTask 创建任务（子实体工厂，DOING 行的唯一成形点；"单人"便捷壳，
// 显式参与者集合走 CreateTaskWithActors，记录类节点的 DONE 行走 CreateHistoryTask）
// ——performType：0 普通 / 1 会签（issues/52 E24 落库对齐 Java）
//
// 建单不变量（规范「引擎操作 04 · 退回上一步」/ issues/121 P1，对齐 Java ProcessTask.create）：
// 所有建任务路径（普通 / 会签 / 串行会签下一位 / 回退建新）都过本工厂或其显式集合形态
// （CreateTaskWithActors，二者同一份实现），两条不变量在此强制兑现，漏不掉；
// 记录类节点的 DONE 行（CreateHistoryTask）同样兑现这两条：
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
	return p.CreateTaskWithActors(id, taskName, displayName, []string{actor},
		operator, formKey, now, parentTaskID, isFirstTaskNode, performType...)
}

// CreateTaskWithActors 以**显式参与者集合**建 DOING 任务行——签名形状逐字对齐 Java
// `ProcessInstance.createTask(TaskModel, displayName, List<String> actorIds, operator, ...)`
// （domain/ProcessInstance.java:263：工厂收的是参与者列表，不是单个 actor），
// 与 `CreateTask` 共用同一份实现（后者只是"单人"便捷壳，语义逐字不变）。
//
// issues/142 A 批 · spec 02 §6.2 第 3 条（owner 2026-09-30 拍「任务类零参与者必须建单」）：
// **actors 为空集也照样建这一行**（java `CreateTaskHandler` 无条件建单同形）——
// 一条 `task_state=10`、参与者为空的行，是"这一格停在任务节点上"的可见事实，
// 可由加派参与者（processTask/addCandidate、taskSurrogate）或 flow.auto/flow.admin 办动。
// 空集的落库形状：`wf_process_task_actor` **一行都不插**（不是插一条 actor_id 为空串的——
// 空归属值正是 issues/129／141 B 表那族"空 actor_id 读全库"的上游进水口，
// 同 [spi.NormalizeActors] 的写侧义务）。
func (p *ProcessInstance) CreateTaskWithActors(id int64, taskName, displayName string, actors []string,
	operator, formKey string, now time.Time, parentTaskID int64, isFirstTaskNode bool, performType ...int) *ProcessTask {
	pt := 0
	if len(performType) > 0 {
		pt = performType[0]
	}
	actorIDs := actors
	if len(actorIDs) == 0 {
		// 空集显式成形：既不是 nil 也不是 []string{""}
		actorIDs = []string{}
	}
	parent := parentTaskID
	task := &ProcessTask{
		ID: id, ProcessInstanceID: p.ID,
		TaskName: taskName, DisplayName: displayName, TaskState: TaskStateDoing,
		ActorIDs: actorIDs,
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

// CreateHistoryTask 创建**历史/已完成**任务行（记录类节点专用，issues/142 A 批 ·
// spec 02-flow-definition.md §6.2；形状基准＝jeeflow-java
// `ProcessInstance.createHistoryTask(CustomModel, operator, parentTaskId, isFirstTaskNode)`
// （domain/ProcessInstance.java:425）＝ `ProcessTask.create(...)` ＋ `setTaskState(FINISHED)`）。
//
// 与 `CreateTask` 的四处差异，每一处都是"记录类"的定义，不是随手改的：
//   - `TaskState = 20`（DONE）——§6.1 禁止形状①「当任务类建 DOING 行」的反面；
//     行状态是 DONE ⇒ 谁也办不动，不会出现在待办里。
//   - 参与者＝**当前操作人**（留痕主体，`ActorIDs=[operator]`），不是"给谁办"。
//     ⚠️ 这与禁止形状②不同类：②是"任务类解析不到参与者时兜底挂操作人、伪造一条他不该收到的
//     **待办**"；这里行本身就是 DONE，挂的是留痕归属。operator 为空串 ⇒ 参与者落空集
//     （比 java 的 `singletonList(operator)` 保守，理由同上：不往 actor_id 灌空归属值）。
//     `ActorID`（落库 `wf_process_task.operator` 列）同样取 operator——java 那边这一列留 NULL，
//     python 与本栈 `Finish()` 都写 operator，于是这条留痕在"我已办"分页里查得到；
//     本轮按"本栈 DONE 行一律带 operator"的既有不变量走（分叉已写进交付报告）。
//   - 无 form（`FormKey=""`）、无到期时间（`ExpireTime=nil`）、`PerformType=0`（不是会签）、
//     `TaskType=0`（java 传 null；本栈这一列是非指针 int，0＝主办档。
//     记录档 `TaskTypeRecord=2` 要不要落到这一列**未拍**，见报告"扫到没动"）。
//   - `FinishTime = now`：java 的 createHistoryTask 只 setTaskState 不设这一列，python 设了；
//     本栈 PageDoneTasks／StatsCompletedTaskAggregate／平均耗时都以 finish_time 取值，
//     留 NULL 会让这条留痕在"已办/耗时"口径里凭空消失 ⇒ 跟 python。
//
// 建单不变量 ①②（issues/121 P1）**同样适用**——java 那边是同一句注释。
func (p *ProcessInstance) CreateHistoryTask(id int64, taskName, displayName, operator string,
	now time.Time, parentTaskID int64, isFirstTaskNode bool) *ProcessTask {
	parent := parentTaskID
	actorIDs := []string{}
	if operator != "" {
		actorIDs = []string{operator}
	}
	finish := now
	task := &ProcessTask{
		ID: id, ProcessInstanceID: p.ID,
		TaskName: taskName, DisplayName: displayName,
		TaskState:  TaskStateDone,
		ActorID:    operator,
		ActorIDs:   actorIDs,
		FinishTime: &finish,
		// 建单不变量 ①②：历史行同样带血缘指针与首节点标记（规范 04 · 退回上一步）
		ParentTaskID: &parent,
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
