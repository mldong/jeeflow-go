package spi

import (
	"context"
	"time"
)

// 任务时限动作（spec 04 · 任务时限动作）：引擎写侧把到期动作登记到框架执行队列，
// 由集成层的队列消费者到点执行。引擎核心不依赖 sys 表——落库/删除经本 SPI 交给集成层，
// 未注入 sink 时全部时限动作静默跳过（存量使用方的行为零变化）。

const (
	// TimeActionRemind 到期提醒
	TimeActionRemind = "REMIND"
	// TimeActionAutoComplete 到期自动同意
	TimeActionAutoComplete = "AUTO_COMPLETE"
	// TimeActionCallback 任务完成回调
	TimeActionCallback = "CALLBACK"
)

// TimeAction 一条待执行时限动作的语义载荷（落库形见 spec 04 `variable.__wfAction`）。
type TimeAction struct {
	Type           string
	DueAt          time.Time
	InstanceID     int64
	TaskID         int64
	NodeKey        string
	OccurrenceNo   int
	IdempotencyKey string
}

// TimeActionSink 时限动作凭证的仓储侧实现（由集成层按框架执行队列实现）。
//
// 三个方法都会在引擎的业务事务内被调用（ctx 携带事务），实现方必须复用同一连接，
// 保证动作行与任务行同事务提交/回滚。
type TimeActionSink interface {
	// InsertTimeAction 插入一条 pending 动作行；同一 taskID+type+occurrenceNo 已存在时跳过（单写者＋插前查）。
	InsertTimeAction(ctx context.Context, action *TimeAction) error
	// CancelTaskTimeActions 删除该任务 pending 的 REMIND/AUTO_COMPLETE 行（任务办结/废弃时调用；
	// 漏删由执行前复核兜底）。CALLBACK 行不随失效取消——它表达"任务已完成"的既成事实。
	CancelTaskTimeActions(ctx context.Context, taskID int64) error
	// CancelInstanceTimeActions 删除该实例 pending 的 REMIND/AUTO_COMPLETE 行（实例终态时调用）。
	CancelInstanceTimeActions(ctx context.Context, instanceID int64) error
}

// InstanceLocker 仓储可选的实例级事务排他保护（spec 04 · 并发保护）。
//
// 数据库仓储以实例行锁实现（MySQL/PostgreSQL `SELECT … FOR UPDATE`），锁随最外层
// COMMIT/ROLLBACK 释放；必须在事务内调用。不实现本接口的仓储退化为引擎进程内互斥，
// 此时不得宣称跨进程自动/人工互斥。
type InstanceLocker interface {
	LockInstance(ctx context.Context, instanceID int64) error
}
