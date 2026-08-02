package spi

import (
	"context"

	"github.com/mldong/jeeflow-go/model"
)

// ProcessRepository 流程仓储接口（方法名对齐 Java/Node camelCase）
type ProcessRepository interface {
	FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error)
	// FindDefineByName 按流程编码查最新一条定义（v1.1.0，Facade deploy 版本管理用）
	FindDefineByName(ctx context.Context, name string) (*model.ProcessDefine, error)
	// 定义写操作（v1.0.1，集成反馈①）：保存/更新/启停/删除流程定义
	SaveDefine(ctx context.Context, def *model.ProcessDefine) error
	UpdateDefine(ctx context.Context, def *model.ProcessDefine) error
	UpdateDefineState(ctx context.Context, defineID int64, state int) error
	RemoveDefine(ctx context.Context, defineID int64) error

	FindInstanceByID(ctx context.Context, id int64) (*model.ProcessInstance, error)
	SaveInstance(ctx context.Context, inst *model.ProcessInstance) error
	UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error

	FindTaskByID(ctx context.Context, taskID int64) (*model.ProcessTask, error)
	SaveTask(ctx context.Context, task *model.ProcessTask) error
	UpdateTask(ctx context.Context, task *model.ProcessTask) error
	FindDoingTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindDoneTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindHistoryTasks(ctx context.Context, instanceID int64) ([]*model.ProcessTask, error)

	FindTaskActors(ctx context.Context, taskID int64) ([]string, error)
	AddTaskActor(ctx context.Context, taskID int64, actors []string) error
	RemoveTaskActor(ctx context.Context, taskID int64, actors []string) error

	CreateCcInstance(ctx context.Context, instanceID int64, creator string, actorIDs ...string) error
	UpdateCcStatus(ctx context.Context, instanceID int64, actorID string) error

	// PageCcInstances 我的抄送分页（v1.3.0，对齐 Java pageCcInstances）：
	// 按抄送人 actorID 过滤实例列表，返回行数据（含关联定义名/版本）+ 总数
	PageCcInstances(ctx context.Context, query PageQuery, actorID string) ([]*model.CcInstanceRow, int, error)
}

// UserProvider 用户信息提供者
type UserProvider interface {
	GetUser(userID string) (*model.UserInfo, error)
}

// IDGenerator ID 生成器
type IDGenerator interface {
	NextID() int64
}

// ExpressionEvaluator 表达式求值器
type ExpressionEvaluator interface {
	Eval(expr string, vars map[string]interface{}) (interface{}, error)
}
