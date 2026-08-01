# 引擎 API

## Engine 接口

```go
type Engine interface {
    StartProcessInstanceByID(defineID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
    ExecuteProcessTask(taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
    ExecuteAndJumpToEnd(taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
    ExecuteAndJumpTask(taskID int64, operator string, args map[string]interface{}, targetTaskName string) (*model.ProcessInstance, error)
    ExecuteAndJumpToFirstTaskNode(taskID int64, operator string, args map[string]interface{}) (*model.ProcessInstance, error)
}
```

## 核心操作

### StartProcessInstanceByID

启动流程，执行 start 节点并创建第一批任务。

```go
inst, err := eng.StartProcessInstanceByID(defineID, "张三", map[string]interface{}{
    "amount": 5000,
    "BUSINESS_NO": "BIZ-001",
})
```

**流程**：加载定义 → 解析 JSON → 创建实例 → 执行 start → 创建第一批任务

### ExecuteProcessTask

完成任务并驱动流程前进。

```go
inst, err := eng.ExecuteProcessTask(taskID, "张三", map[string]interface{}{
    "submitType": 1,   // 1=同意
    "comment":    "同意报销",
})
```

**流程**：校验权限 → 完成任务 → 执行当前节点输出边 → 创建下一批任务 / 结束

### ExecuteAndJumpToEnd

驳回——完成任务后将实例标记为已拒绝。

```go
inst, err := eng.ExecuteAndJumpToEnd(taskID, "张三", nil)
```

**效果**：实例状态 → 45（已拒绝），其他进行中任务 → 99（已废弃）

### ExecuteAndJumpTask

跳转到指定任务节点（回退 / 跳过）。

```go
inst, err := eng.ExecuteAndJumpTask(taskID, "张三", nil, "task1")
```

**效果**：当前任务完成 → 其他任务废弃 → 重新创建 `task1` 的任务

## 流程变量

引擎自动注入以下变量：

| 变量 | 说明 |
|------|------|
| `u_userId` | 操作人 ID |
| `u_realName` | 操作人姓名 |
| `u_deptId` | 部门 ID |
| `u_deptName` | 部门名称 |
| `u_postId` | 岗位 ID |
| `u_postName` | 岗位名称 |
| `submitType` | 0=发起 1=同意 2=拒绝 3=退回上一步 4=跳转 5=重新提交 6=退回发起人 20=会签拒绝 |
| `BUSINESS_NO` | 业务流水号 |

## 状态码

| 常量 | 值 | 含义 |
|------|-----|------|
| `InstanceStateDoing` | 10 | 进行中 |
| `InstanceStateDone` | 20 | 已完成 |
| `InstanceStateReject` | 45 | 已拒绝 |
| `TaskStateDoing` | 10 | 进行中 |
| `TaskStateDone` | 20 | 已完成 |
| `TaskStateAbandoned` | 99 | 已废弃 |
