// Package spi 引擎 SPI 接口——对标 SPEC.md §6
package spi

import "github.com/mldong/jeeflow-go/model"

type ProcessRepository interface {
	FindDefineByID(id int64) (*model.ProcessDefine, error)

	FindInstanceByID(id int64) (*model.ProcessInstance, error)
	SaveInstance(inst *model.ProcessInstance) error
	UpdateInstance(inst *model.ProcessInstance) error

	FindTaskByID(taskID int64) (*model.ProcessTask, error)
	SaveTask(task *model.ProcessTask) error
	UpdateTask(task *model.ProcessTask) error
	FindDoingTasks(instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindDoneTasks(instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindHistoryTasks(instanceID int64) ([]*model.ProcessTask, error)

	FindTaskActors(taskID int64) ([]string, error)
	AddTaskActor(taskID int64, actors []string) error
	RemoveTaskActor(taskID int64, actors []string) error

	CreateCcInstance(instanceID int64, creator string, actorIDs ...string) error
	UpdateCcStatus(instanceID int64, actorID string) error
}

type UserProvider interface {
	GetUser(userID string) (*model.UserInfo, error)
}

type IDGenerator interface {
	NextID() int64
}

type ExpressionEvaluator interface {
	Eval(expr string, vars map[string]interface{}) (interface{}, error)
}
