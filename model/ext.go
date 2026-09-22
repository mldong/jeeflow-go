// 管理扩展领域对象（v1.1.0）——流程设计 / 设计历史 / 委托代理
package model

import (
	"strings"
	"time"
)

// ProcessDesign 流程设计（wf_process_design）——设计器保存的设计稿元信息
type ProcessDesign struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	Type        string    `json:"type"`
	Icon        string    `json:"icon"`
	IsDeployed  int       `json:"isDeployed"`
	Remark      string    `json:"remark"`
	CreateTime  time.Time `json:"createTime"`
	CreateUser  string    `json:"createUser"`
	UpdateTime  time.Time `json:"updateTime"`
	UpdateUser  string    `json:"updateUser"`
}

// ProcessDesignHis 流程设计历史（wf_process_design_his）——每次保存的 content 快照
type ProcessDesignHis struct {
	ID              int64     `json:"id"`
	ProcessDesignID int64     `json:"processDesignId"`
	Content         []byte    `json:"content"`
	CreateTime      time.Time `json:"createTime"`
	CreateUser      string    `json:"createUser"`
}

// ProcessSurrogate 流程委托代理（wf_process_surrogate）——授权人把待办委托给代理人
//
// 生效规则见 [ProcessSurrogate.IsEffective]：enabled 严格等于 1、时间窗覆盖判定时刻
// （起止为空 = 该侧不限）、自委托（surrogate = operator）不生效；ProcessName 为空 = 全部流程。
// 多条并存时由仓储按主键 id **取最新一条再交本方法裁决**（规范 06 §4.5 条款 1.4；
// 不得"先滤生效再取最新"，见 issues/123）。
type ProcessSurrogate struct {
	ID          int64      `json:"id"`
	ProcessName string     `json:"processName"`
	Operator    string     `json:"operator"`
	Surrogate   string     `json:"surrogate"`
	StartTime   *time.Time `json:"startTime"`
	EndTime     *time.Time `json:"endTime"`
	Enabled     int        `json:"enabled"`
	CreateTime  time.Time  `json:"createTime"`
	CreateUser  string     `json:"createUser"`
	UpdateTime  time.Time  `json:"updateTime"`
	UpdateUser  string     `json:"updateUser"`
}

// IsEffective 四判据（规范 06 §4.5 条款 5 + issues/123 §1）：本条委托此刻对该授权人是否生效。
//
// 调用方必须先按 id 选出「该授权人在该流程作用域内的最新一条」再问本方法——本方法只裁决
// 单条，不做多条择优。SQL 仓与内存仓必须走同一份判据（08-compliance 用例 27 要求双仓同答案）。
//
// operator 为授权人（判自委托：被委托人等于授权人 ⇒ 不新增、不重复）；
// at 为零值表示不做窗口比较（对齐各栈"调用方没给时刻就整窗不限"的入参语义，
// 引擎建单路径恒有值）。
//
// enabled 只认严格等于 1：0 / 2 / 负数 / 库里 NULL（扫描时已折成 0）一律不生效。
func (s *ProcessSurrogate) IsEffective(operator string, at time.Time) bool {
	if s == nil || s.Enabled != 1 {
		return false
	}
	agent := strings.TrimSpace(s.Surrogate)
	if agent == "" {
		return false
	}
	if agent == operator {
		return false
	}
	if at.IsZero() {
		return true
	}
	if s.StartTime != nil && s.StartTime.After(at) {
		return false
	}
	return s.EndTime == nil || !s.EndTime.Before(at)
}
