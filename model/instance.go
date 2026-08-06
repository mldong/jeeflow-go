// DDD 聚合根行为——对标 Java 版 domain/ProcessInstance + domain/ProcessTask
package model

import "time"

// 业务号变量 key（engine 包 KeyBusinessNo 同值）
const BusinessNoKey = "BUSINESS_NO"

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

// CreateTask 创建任务（子实体工厂）——performType：0 普通 / 1 会签（issues/52 E24 落库对齐 Java）
func (p *ProcessInstance) CreateTask(id int64, taskName, displayName, actor, operator, formKey string, now time.Time, performType ...int) *ProcessTask {
	pt := 0
	if len(performType) > 0 {
		pt = performType[0]
	}
	task := &ProcessTask{
		ID: id, ProcessInstanceID: p.ID,
		TaskName: taskName, DisplayName: displayName, TaskState: TaskStateDoing,
		ActorIDs: []string{actor},
		FormKey:  formKey, PerformType: pt,
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
