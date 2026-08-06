// 管理扩展领域对象（v1.1.0）——流程设计 / 设计历史 / 委托代理
package model

import "time"

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
