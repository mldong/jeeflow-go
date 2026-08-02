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
	InstanceStateDoing  InstanceState = 10
	InstanceStateDone   InstanceState = 20
	InstanceStateReject InstanceState = 45
)

type TaskState int

const (
	TaskStateDoing     TaskState = 10
	TaskStateDone      TaskState = 20
	TaskStateAbandoned TaskState = 99
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
