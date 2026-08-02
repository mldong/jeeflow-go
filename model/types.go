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
	PerformTypeNormal     PerformType = 0
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
