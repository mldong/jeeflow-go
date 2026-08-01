// Package engine 引擎接口 + 常量
package engine

import (
	"context"

	"github.com/mldong/jeeflow-go/model"
)

type Engine interface {
	StartProcessInstanceByID(ctx context.Context, defineID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
	ExecuteProcessTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
	ExecuteAndJumpToEnd(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
	ExecuteAndJumpTask(ctx context.Context, taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error)
	ExecuteAndJumpToFirstTaskNode(ctx context.Context, taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
}

const (
	KeySubmitType   = "submitType"
	KeyBusinessNo   = "BUSINESS_NO"
	KeyUserID       = "u_userId"
	KeyRealName     = "u_realName"
	KeyDeptID       = "u_deptId"
	KeyDeptName     = "u_deptName"
	KeyPostID       = "u_postId"
	KeyPostName     = "u_postName"
	KeyAutoGenTitle = "autoGenTitle"
)
