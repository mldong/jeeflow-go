// Package model 域类型定义——对标 Java 版 domain + model 包
package model

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// ─── Flow Model (LogicFlow JSON) ──────────────────────────────────────────────

// FlowModel 流程定义 JSON 的**根对象**（LogicFlow 图）。
//
// ExpireTime 是 spec 02 §根对象（jeeflow-doc `docs/spec/02-flow-definition.md:21/55`）里的
// 「流程期望完成时间」——根上的 `expireTime` 键，与任务节点 `properties.expireTime`（issues/126
// 那一族四处建单写点）**不同一层**：这一项喂的是实例那一列 `wf_process_instance.expire_time`
// （issues/137 A · 批二 §3-4，A 案＝列值＝本表达式的**求值结果**，写点在 engine.StartProcessInstanceByID）。
// 本栈没有独立的模型解析阶段（java 是 LfModel→ProcessModel 那一跳），根键直接由 json.Unmarshal 收进来。
type FlowModel struct {
	Name        string     `json:"name"`
	DisplayName string     `json:"displayName"`
	Type        string     `json:"type"`
	ExpireTime  string     `json:"expireTime"`
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
	TypeStart      = "snaker:start"
	TypeEnd        = "snaker:end"
	TypeTask       = "snaker:task"
	TypeDecision   = "snaker:decision"
	TypeFork       = "snaker:fork"
	TypeJoin       = "snaker:join"
	TypeCustom     = "snaker:custom"
	TypeSubProcess = "snaker:subProcess"
)

// IsSubProcessType 子流程节点的历史类型别名。设计器存量定义使用驼峰与裸小写，
// 三者归一到同一执行档；id/name 仍只作图内标识与回父定位键。
func IsSubProcessType(t string) bool {
	switch t {
	case TypeSubProcess, "subProcess", "subprocess", "wfSubProcess", "snaker:subprocess", "snaker:wfSubProcess":
		return true
	default:
		return false
	}
}

// SubProcessModel 子流程节点契约（issues/257）。Go 的 FlowModel 没有独立 parser 阶段，
// 该模型在执行入口按 FlowNode 解析，字段与 Java WfSubProcessParser 对齐。
type SubProcessModel struct {
	ID          string
	ProcessName string
	Version     *int
	Form        string
}

func ParseSubProcessModel(node *FlowNode) *SubProcessModel {
	if node == nil || !IsSubProcessType(node.Type) {
		return nil
	}
	props := node.Properties
	if props == nil {
		return &SubProcessModel{ID: node.ID, ProcessName: node.ID}
	}
	processName := subProcessStringProp(props, "processName")
	if strings.TrimSpace(processName) == "" {
		processName = subProcessStringProp(props, "name")
	}
	if strings.TrimSpace(processName) == "" {
		processName = node.ID
	}
	model := &SubProcessModel{ID: node.ID, ProcessName: processName, Form: subProcessStringProp(props, "form")}
	if v, ok := props["version"]; ok && v != nil {
		model.Version = subProcessVersion(v)
	}
	return model
}

// subProcessVersion 保留显式版本意图；非法、非整数或超出本机 int 范围的值统一为 -1，
// 绝不能静默截断/回落成 0 而误启另一版定义。nil/缺省才表示取最新版。
func subProcessVersion(v interface{}) *int {
	version := -1
	switch n := v.(type) {
	case int:
		version = n
	case int8:
		version = subProcessInt64Version(int64(n))
	case int16:
		version = subProcessInt64Version(int64(n))
	case int32:
		version = subProcessInt64Version(int64(n))
	case int64:
		version = subProcessInt64Version(n)
	case uint:
		if uint64(n) <= uint64(^uint(0)>>1) {
			version = int(n)
		}
	case uint8:
		version = int(n)
	case uint16:
		version = int(n)
	case uint32:
		if uint64(n) <= uint64(^uint(0)>>1) {
			version = int(n)
		}
	case uint64:
		if n <= uint64(^uint(0)>>1) {
			version = int(n)
		}
	case float32:
		version = subProcessFloatVersion(float64(n))
	case float64:
		version = subProcessFloatVersion(n)
	case json.Number:
		if parsed, err := n.Int64(); err == nil {
			version = subProcessInt64Version(parsed)
		} else if parsed, err := n.Float64(); err == nil {
			version = subProcessFloatVersion(parsed)
		}
	case string:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(n), 10, strconv.IntSize); err == nil {
			version = subProcessInt64Version(parsed)
		}
	}
	return &version
}

func subProcessInt64Version(n int64) int {
	converted := int(n)
	if int64(converted) != n {
		return -1
	}
	return converted
}

func subProcessFloatVersion(n float64) int {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return -1
	}
	// 先按十进制原值解析，避开 float64→int 的越界实现定义与 2^63 舍入边界。
	parsed, err := strconv.ParseInt(strconv.FormatFloat(n, 'f', -1, 64), 10, strconv.IntSize)
	if err != nil {
		return -1
	}
	return subProcessInt64Version(parsed)
}

func subProcessStringProp(props map[string]interface{}, key string) string {
	v, ok := props[key].(string)
	if !ok {
		return ""
	}
	return v
}

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
//
// 7=转办留痕（issues/115）：processTask/transfer 写审批记录用，不走 execute。
type SubmitType int

const (
	SubmitTypeApply               SubmitType = 0
	SubmitTypeAgree               SubmitType = 1
	SubmitTypeReject              SubmitType = 2
	SubmitTypeRollback            SubmitType = 3
	SubmitTypeJump                SubmitType = 4
	SubmitTypeReApply             SubmitType = 5
	SubmitTypeRollbackToOperator  SubmitType = 6
	SubmitTypeTransfer            SubmitType = 7
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
// BusinessNo/Operator 为可空列、DefineName/DefineDisplayName 走 LEFT JOIN pd：
// *string 让 SQL NULL 投成 JSON null 而非 ""（spec 06 §2.4 · issues/141 G3，与 InstanceRow 同口径）。
type CcInstanceRow struct {
	ID                int64
	ParentID          *int64
	DefineID          int64
	State             InstanceState
	ParentNodeName    string
	BusinessNo        *string
	Operator          *string
	ExpireTime        *time.Time
	Variables         map[string]interface{}
	CreateTime        time.Time
	CreateUser        string
	UpdateTime        time.Time
	UpdateUser        string
	DefineName        *string
	DefineDisplayName *string
	DefineVersion     int
}

// ─── 核心表分页行数据（v1.5.0，对齐 Java DefineRow/InstanceRow/TaskRow） ─────

// DefineRow 流程定义行数据（pageDefines 分页）
// Type 可空（wf_process_define.type NULL）：用 *string 让 SQL NULL 投成 JSON null，
// 而非空串 ""（spec 06 §2.4 · issues/141 G3——可空列合法出口为 null/缺键，"" 违规）。
type DefineRow struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	Type        *string   `json:"type"`
	State       int       `json:"state"`
	Version     int       `json:"version"`
	CreateTime  time.Time `json:"createTime"`
	CreateUser  string    `json:"createUser"`
	UpdateTime  time.Time `json:"updateTime"`
	UpdateUser  string    `json:"updateUser"`
}

// InstanceRow 流程实例行数据（pageInstances 分页）
// BusinessNo/Operator 为可空列、DefineName/DefineDisplayName 走 LEFT JOIN pd（定义缺失即 NULL）：
// 用 *string 让 SQL NULL 投成 JSON null 而非空串 ""（spec 06 §2.4 · issues/141 G3）。
type InstanceRow struct {
	ID                int64                  `json:"id"`
	ParentID          *int64                 `json:"parentId"`
	DefineID          int64                  `json:"processDefineId"`
	State             InstanceState          `json:"state"`
	ParentNodeName    string                 `json:"parentNodeName"`
	BusinessNo        *string                `json:"businessNo"`
	Operator          *string                `json:"operator"`
	ExpireTime        *time.Time             `json:"expireTime"`
	Variables         map[string]interface{} `json:"variables"`
	CreateTime        time.Time              `json:"createTime"`
	CreateUser        string                 `json:"createUser"`
	UpdateTime        time.Time              `json:"updateTime"`
	UpdateUser        string                 `json:"updateUser"`
	DefineName        *string                `json:"defineName"`
	DefineDisplayName *string                `json:"defineDisplayName"`
	DefineVersion     int                    `json:"defineVersion"`
}

// TaskRow 任务行数据（pageTodoTasks / pageDoneTasks 分页）
// Operator/FormKey 为可空列、ProcessDefineName/ProcessDefineDisplayName 走 LEFT JOIN pd（定义缺失即 NULL）：
// 用 *string 让 SQL NULL 投成 JSON null 而非空串 ""（issues/141 G3 主案：pageTasks 裸 string 扫 NULL 直接报错）。
type TaskRow struct {
	ID                       int64                  `json:"id"`
	ProcessInstanceID        int64                  `json:"processInstanceId"`
	TaskName                 string                 `json:"taskName"`
	DisplayName              string                 `json:"displayName"`
	TaskType                 int                    `json:"taskType"`
	PerformType              int                    `json:"performType"`
	TaskState                TaskState              `json:"taskState"`
	Operator                 *string                `json:"operator"`
	FinishTime               *time.Time             `json:"finishTime"`
	ExpireTime               *time.Time             `json:"expireTime"`
	FormKey                  *string                `json:"formKey"`
	TaskParentID             *int64                 `json:"taskParentId"`
	Variables                map[string]interface{} `json:"variables"`
	CreateTime               time.Time              `json:"createTime"`
	CreateUser               string                 `json:"createUser"`
	UpdateTime               time.Time              `json:"updateTime"`
	UpdateUser               string                 `json:"updateUser"`
	ProcessDefineName        *string                `json:"processDefineName"`
	ProcessDefineDisplayName *string                `json:"processDefineDisplayName"`
	DefineVersion            int                    `json:"defineVersion"`
	InstanceVariable         string                 `json:"instanceVariable"`
	InstanceCreateTime       time.Time              `json:"instanceCreateTime"`
	// InstanceState 所属流程实例的状态（issues/250，10/20/30/40/45/50/99）——前端据此把
	// 「实例已非进行中」的已办行撤回按钮置灰。任务行自身的 TaskState 照不出这一格。
	// 用指针让「实例不在场」投 JSON null，不硬造 0（0 不是合法状态）。
	InstanceState *int `json:"instanceState"`
}

// ─── 统计 DTO（v1.8.25，issues/103，对齐 Java InstanceStatsRow/TaskStatsRow） ─

// InstanceStatsRow 实例统计行（轻量级，不加载关联任务）
type InstanceStatsRow struct {
	ID         int64
	State      int
	CreateTime time.Time
	DefineID   int64
	Operator   string
}

// TaskStatsRow 任务统计行
type TaskStatsRow struct {
	ID                int64
	ProcessInstanceID int64
	TaskState         int
	PerformType       int
	Operator          string
	DisplayName       string
	CreateTime        *time.Time
	FinishTime        *time.Time
	ExpireTime        *time.Time
}
