// Package model 域类型定义——对标 Java 版 domain + model 包
package model

import "time"

// ─── Flow Model (LogicFlow JSON) ──────────────────────────────────────────────

type FlowModel struct {
	Name        string     `json:"name"`
	DisplayName string     `json:"displayName"`
	Type        string     `json:"type"`
	Nodes       []FlowNode `json:"nodes"`
	Edges       []FlowEdge `json:"edges"`
}

type FlowNode struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	X          float64                `json:"x"`
	Y          float64                `json:"y"`
	Properties map[string]interface{} `json:"properties"`
	Text       struct {
		Value string `json:"value"`
	} `json:"text"`
}

type FlowEdge struct {
	ID           string                 `json:"id"`
	SourceNodeID string                 `json:"sourceNodeId"`
	TargetNodeID string                 `json:"targetNodeId"`
	Properties   map[string]interface{} `json:"properties"`
	Text         *struct {
		Value string `json:"value"`
	} `json:"text,omitempty"`
}

// ─── Node Type Constants ──────────────────────────────────────────────────────

const (
	TypeStart    = "snaker:start"
	TypeEnd      = "snaker:end"
	TypeTask     = "snaker:task"
	TypeDecision = "snaker:decision"
	TypeFork     = "snaker:fork"
	TypeJoin     = "snaker:join"
	TypeCustom   = "snaker:custom"
)

// ─── Domain Types ─────────────────────────────────────────────────────────────

type ProcessDefine struct {
	ID          int64
	Name        string
	DisplayName string
	Type        string
	State       int
	Content     []byte
	Version     int
	CreateTime  time.Time
	CreateUser  string
	UpdateTime  time.Time
	UpdateUser  string
}

type InstanceState int

const (
	InstanceStateDoing     InstanceState = 10
	InstanceStateDone      InstanceState = 20
	InstanceStateWithdraw  InstanceState = 30
	InstanceStateInterrupt InstanceState = 40
	InstanceStateReject    InstanceState = 45
	InstanceStatePending   InstanceState = 50
	InstanceStateAbandon   InstanceState = 99
)

type TaskState int

const (
	TaskStateDoing     TaskState = 10
	TaskStateDone      TaskState = 20
	TaskStateWithdraw  TaskState = 30
	TaskStateInterrupt TaskState = 40
	TaskStatePending   TaskState = 50
	TaskStateAbandoned TaskState = 99
)

// ─── 字典枚举（v1.4.0，对齐 Java enums，值与 boot3 字典一致） ────────────────

// DefineState 流程定义状态（wf_process_define_state）
type DefineState int

const (
	DefineStateDisable DefineState = 0
	DefineStateEnable  DefineState = 1
)

// SubmitType 流程提交类型（wf_process_submit_type）
type SubmitType int

const (
	SubmitTypeApply               SubmitType = 0
	SubmitTypeAgree               SubmitType = 1
	SubmitTypeReject              SubmitType = 2
	SubmitTypeRollback            SubmitType = 3
	SubmitTypeJump                SubmitType = 4
	SubmitTypeReApply             SubmitType = 5
	SubmitTypeRollbackToOperator  SubmitType = 6
	SubmitTypeCountersignDisagree SubmitType = 20
)

// TaskType 任务类型（wf_process_task_type）
type TaskType int

const (
	TaskTypeMajor     TaskType = 0
	TaskTypeSecondary TaskType = 1
	TaskTypeRecord    TaskType = 2
)

// PerformType 任务参与方式（wf_process_task_perform_type）
type PerformType int

const (
	PerformTypeNormal      PerformType = 0
	PerformTypeCountersign PerformType = 1
)

// CountersignType 会签类型（wf_countersign_type）
type CountersignType int

const (
	CountersignTypeParallel   CountersignType = 0
	CountersignTypeSequential CountersignType = 1
)

type ProcessInstance struct {
	ID             int64
	ParentID       *int64
	DefineID       int64
	State          InstanceState
	ParentNodeName string
	BusinessNo     string
	Operator       string
	ExpireTime     *time.Time
	Variables      map[string]interface{}
	Tasks          []*ProcessTask
	CreateTime     time.Time
	CreateUser     string
	UpdateTime     time.Time
	UpdateUser     string
}

type ProcessTask struct {
	ID                int64
	ProcessInstanceID int64
	TaskName          string
	DisplayName       string
	TaskType          int
	PerformType       int
	TaskState         TaskState
	ActorID           string
	ActorIDs          []string
	FinishTime        *time.Time
	ExpireTime        *time.Time
	FormKey           string
	ParentTaskID      *int64
	Variables         map[string]interface{}
	CreateTime        time.Time
	CreateUser        string
	UpdateTime        time.Time
	UpdateUser        string
}

type UserInfo struct {
	UserID   string
	RealName string
	DeptID   string
	DeptName string
	PostID   string
	PostName string
}

// CcInstanceRow 抄送实例行数据（ccList 分页，v1.3.0，对齐 Java InstanceRow 字段）
type CcInstanceRow struct {
	ID                int64
	ParentID          *int64
	DefineID          int64
	State             InstanceState
	ParentNodeName    string
	BusinessNo        string
	Operator          string
	ExpireTime        *time.Time
	Variables         map[string]interface{}
	CreateTime        time.Time
	CreateUser        string
	UpdateTime        time.Time
	UpdateUser        string
	DefineName        string
	DefineDisplayName string
	DefineVersion     int
}

// ─── 核心表分页行数据（v1.5.0，对齐 Java DefineRow/InstanceRow/TaskRow） ─────

// DefineRow 流程定义行数据（pageDefines 分页）
type DefineRow struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	Type        string    `json:"type"`
	State       int       `json:"state"`
	Version     int       `json:"version"`
	CreateTime  time.Time `json:"createTime"`
	CreateUser  string    `json:"createUser"`
	UpdateTime  time.Time `json:"updateTime"`
	UpdateUser  string    `json:"updateUser"`
}

// InstanceRow 流程实例行数据（pageInstances 分页）
type InstanceRow struct {
	ID                int64                  `json:"id"`
	ParentID          *int64                 `json:"parentId"`
	DefineID          int64                  `json:"processDefineId"`
	State             InstanceState          `json:"state"`
	ParentNodeName    string                 `json:"parentNodeName"`
	BusinessNo        string                 `json:"businessNo"`
	Operator          string                 `json:"operator"`
	ExpireTime        *time.Time             `json:"expireTime"`
	Variables         map[string]interface{} `json:"variables"`
	CreateTime        time.Time              `json:"createTime"`
	CreateUser        string                 `json:"createUser"`
	UpdateTime        time.Time              `json:"updateTime"`
	UpdateUser        string                 `json:"updateUser"`
	DefineName        string                 `json:"defineName"`
	DefineDisplayName string                 `json:"defineDisplayName"`
	DefineVersion     int                    `json:"defineVersion"`
}

// TaskRow 任务行数据（pageTodoTasks / pageDoneTasks 分页）
type TaskRow struct {
	ID                       int64                  `json:"id"`
	ProcessInstanceID        int64                  `json:"processInstanceId"`
	TaskName                 string                 `json:"taskName"`
	DisplayName              string                 `json:"displayName"`
	TaskType                 int                    `json:"taskType"`
	PerformType              int                    `json:"performType"`
	TaskState                TaskState              `json:"taskState"`
	Operator                 string                 `json:"operator"`
	FinishTime               *time.Time             `json:"finishTime"`
	ExpireTime               *time.Time             `json:"expireTime"`
	FormKey                  string                 `json:"formKey"`
	TaskParentID             *int64                 `json:"taskParentId"`
	Variables                map[string]interface{} `json:"variables"`
	CreateTime               time.Time              `json:"createTime"`
	CreateUser               string                 `json:"createUser"`
	UpdateTime               time.Time              `json:"updateTime"`
	UpdateUser               string                 `json:"updateUser"`
	ProcessDefineName        string                 `json:"processDefineName"`
	ProcessDefineDisplayName string                 `json:"processDefineDisplayName"`
	DefineVersion            int                    `json:"defineVersion"`
	InstanceVariable         string                 `json:"instanceVariable"`
	InstanceCreateTime       time.Time              `json:"instanceCreateTime"`
}
