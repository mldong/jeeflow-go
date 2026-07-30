# SPI 扩展指南

jeeflow 通过 SPI 接口解耦引擎与存储、用户等外部依赖。实现接口即可对接任意数据库或用户系统。

## 必须实现

### ProcessRepository

```go
type ProcessRepository interface {
    FindDefineByID(id int64) (*model.ProcessDefine, error)
    FindInstanceByID(id int64) (*model.ProcessInstance, error)
    SaveInstance(inst *model.ProcessInstance) error
    UpdateInstance(inst *model.ProcessInstance) error
    FindTaskByID(taskID int64) (*model.ProcessTask, error)
    SaveTask(task *model.ProcessTask) error
    UpdateTask(task *model.ProcessTask) error
    FindDoingTasks(instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
    FindHistoryTasks(instanceID int64) ([]*model.ProcessTask, error)
    FindTaskActors(taskID int64) ([]string, error)
    AddTaskActor(taskID int64, actors []string) error
    RemoveTaskActor(taskID int64, actors []string) error
}
```

**内置实现**：
- `memory.New()` — 内存仓储，测试用

**MySQL 实现示例**：

```go
type MySQLRepository struct {
    db *sql.DB
}

func (r *MySQLRepository) FindDefineByID(id int64) (*model.ProcessDefine, error) {
    row := r.db.QueryRow("SELECT id,name,display_name,type,state,content FROM wf_process_define WHERE id=?", id)
    // ... scan ...
}
```

## 可选 SPI

### UserProvider

```go
type UserProvider interface {
    GetUser(userID string) (*model.UserInfo, error)
}
```

实现后引擎自动填充 `u_userId` / `u_realName` 等流程变量。

```go
type MyUserProvider struct {
    userSvc *UserService
}

func (p *MyUserProvider) GetUser(userID string) (*model.UserInfo, error) {
    u, err := p.userSvc.FindByID(userID)
    if err != nil { return nil, err }
    return &model.UserInfo{
        UserID: u.ID, RealName: u.Name,
        DeptID: u.DeptID, DeptName: u.DeptName,
    }, nil
}
```

### IDGenerator

```go
type IDGenerator interface {
    NextID() int64
}
```

不实现则使用 `time.Now().UnixNano()` 作为 ID。

### ExpressionEvaluator

```go
type ExpressionEvaluator interface {
    Eval(expr string, vars map[string]interface{}) (interface{}, error)
}
```

决策节点（`snaker:decision`）依赖此接口求值 `amount > 1000` 等表达式。

## 装配引擎

```go
repo := &MySQLRepository{db: db}
userProv := &MyUserProvider{userSvc: svc}
idGen := &SnowflakeGenerator{}
exprEval := &SpelEvaluator{}

eng := engine.New(repo, userProv, idGen, exprEval)
// 不需要的传 nil
// eng := engine.New(repo, nil, nil, nil)
```
