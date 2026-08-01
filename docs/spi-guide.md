# SPI 扩展指南

jeeflow 通过 SPI 接口解耦引擎与存储、用户等外部依赖。实现接口即可对接任意数据库或用户系统。

## 必须实现

### ProcessRepository

> 接口方法均以 `context.Context` 为第一参数（spec §7.4 事务约定）：事务上下文内仓储调用走绑定连接，否则走连接池。

```go
type ProcessRepository interface {
    FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error)
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
}
```

**内置实现**：

- `memory.New()` — 内存仓储，测试/演示用
- `jdbc.New(db)` — **JDBC 参考实现**（`repository/jdbc`，仅依赖 stdlib `database/sql` + 驱动）。
  `database/sql` 是标准抽象（同 Java `javax.sql.DataSource`），**换数据库 = 换驱动 import + DSN**，
  占位符统一 `?` 由驱动转换：

```go
// MySQL
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "github.com/mldong/jeeflow-go/repository/jdbc"
)
db, _ := sql.Open("mysql", "root:pwd@tcp(127.0.0.1:3306)/jeeflow?parseTime=true")

// PostgreSQL（换驱动即可，代码零改动）
// import _ "github.com/jackc/pgx/v5/stdlib"
// db, _ := sql.Open("pgx", "postgres://root:pwd@127.0.0.1/jeeflow")

repo := jdbc.New(db)                // 关系表主键用内置时间戳 ID 生成器
// repo := jdbc.NewWithIDGen(db, mySnowflake)  // 自定义 ID 生成器
```

> **新增数据库** = 驱动 + 建表 SQL（`repository/jdbc/schema/<db>.sql`，与各语言统一）。
> 核心零驱动依赖：`_ import` 驱动只出现在调用方/测试。

仓储方法自动映射 `wf_*` 5 张表（spec §2）。`content` 为流程定义 JSON，`variable` 为变量 JSON。

**事务（spec §7.4）**：`WithTx` 开启事务并把连接绑定到 ctx，回调内所有仓储调用走同一连接；异常自动回滚：

```go
err := repo.WithTx(ctx, func(ctx context.Context) error {
    if err := repo.SaveInstance(ctx, inst); err != nil {
        return err
    }
    return repo.CreateCcInstance(ctx, inst.ID, creator, "lisi", "wangwu")
})
```

> 约定：**业务层是事务 owner**——先 `WithTx` 再调引擎方法，引擎核心不感知事务。

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
repo := jdbc.New(db)               // 或 memory.New() / 自实现
userProv := &MyUserProvider{userSvc: svc}
idGen := &SnowflakeGenerator{}
exprEval := &SpelEvaluator{}

eng := engine.New(repo, userProv, idGen, exprEval)
// 不需要的传 nil
// eng := engine.New(repo, nil, nil, nil)
```

## 集成测试

`repository/jdbc` 附带连真实 MySQL 的集成测试（`jdbc_test.go`）：端到端启动→完成任务→验证持久化、权限负向、`WithTx` 提交/回滚。前置：开发服务器 MySQL 且 5 张 `wf_*` 表已建。
