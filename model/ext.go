// 管理扩展领域对象（v1.1.0）——流程设计 / 设计历史 / 委托代理
package model

import "time"

// ProcessDesign 流程设计（wf_process_design）——设计器保存的设计稿元信息
type ProcessDesign struct {
	ID          int64
	Name        string
	DisplayName string
	Type        string
	Icon        string
	IsDeployed  int
	Remark      string
	CreateTime  time.Time
	CreateUser  string
	UpdateTime  time.Time
	UpdateUser  string
}

// ProcessDesignHis 流程设计历史（wf_process_design_his）——每次保存的 content 快照
type ProcessDesignHis struct {
	ID              int64
	ProcessDesignID int64
	Content         []byte
	CreateTime      time.Time
	CreateUser      string
}

// ProcessSurrogate 流程委托代理（wf_process_surrogate）——授权人把待办委托给代理人
type ProcessSurrogate struct {
	ID          int64
	ProcessName string
	Operator    string
	Surrogate   string
	StartTime   *time.Time
	EndTime     *time.Time
	Enabled     int
	CreateTime  time.Time
	CreateUser  string
	UpdateTime  time.Time
	UpdateUser  string
}
