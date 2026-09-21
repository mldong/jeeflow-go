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

> **新增数据库** = 驱动 + 建表 SQL（各语言自带：本包 `schema/schema-<db>.sql`，
> 使用者单语言下载即用；维护者改 jeeflow-java 仓 resources 后跑
> `jeeflow-hub/scripts/sync-schema.sh` 分发）。
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

### OrgUserProvider（v1.6.0）

组织维度取人——内置组织 handler（部门领导/分管领导/角色）的数据源。
**业务方只实现数据接口，不写 handler**：

```go
type OrgUserProvider interface {
    FindDeptLeaders(deptID string) ([]string, error)
    FindDeptMainLeaders(deptID string) ([]string, error)
    FindByRole(roleCode string) ([]string, error)
}
```

```go
type MyOrgUserProvider struct {
    orgSvc *OrgService
}

func (p *MyOrgUserProvider) FindDeptLeaders(deptID string) ([]string, error) {
    return p.orgSvc.LeaderIDs(deptID)
}

func (p *MyOrgUserProvider) FindByRole(roleCode string) ([]string, error) {
    return p.orgSvc.UserIDsByRole(roleCode)
}
```

注册内置 handler（`engine.RegisterBuiltinAssignments`，注册名与 Java 类全限定名一致，
流程 JSON 四语言通用）：

```go
reg := engine.NewHandlerRegistry()
engine.RegisterBuiltinAssignments(reg, userProv, orgProv)   // 组织维度依赖注入
eng.SetRegistry(reg)
```

> 内置 handler 的**场景/配置/注意事项**见 [用户指南 07 · 参与者解析](../../guides/07-assignment-handlers.md)。

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

---

## 管理扩展与统一门面（v1.1.0）

设计稿 / 历史 / 委托由扩展仓储 SPI 提供读写（文档站 spec §10），统一门面
`flow(action, map)` 按 action 路由（spec §11.2），返回 `{code, msg, data}`，
deploy 自动版本管理，execute 按 submitType 全分发，操作人由 `args.operator` 显式传入。

扩展仓储实现（JDBC + 内存）与门面均在本仓库：
- 扩展仓储：`<repository>/jdbc/ext.*`（JDBC）、memory 内存实现
- 门面：`facade.*` / `jeeflow/facade.py` / `src/facade.ts`

三张扩展表（wf_process_design / design_his / surrogate）SQL 已随 schema 分发
（`schema-<db>.sql`，维护源 jeeflow-java resources）。

> 分页说明（v1.1.0）：核心表分页 SPI（pageDefines/pageTodoTasks 等）目前 Java 提供，
> 本语言对应分页 action 返回明确错误，计划 1.2.0 补齐；设计/委托分页全支持。

## 委托代理自动生效（引擎内置、默认开启）

`processSurrogate/*` 五个 action 只是台账 CRUD。真正的能力在引擎侧：
**任务参与者落库前**，引擎对每个参与者查一次生效委托（`GetSurrogate`），
命中的被委托人**并入该任务的参与者集合**后随任务一起落 `wf_process_task_actor`
——授权人保留，任一可办（委托不是转办，不摘原人）。

装配（三条路，任选其一）：

```go
// ① 构造期 Option
eng := engine.New(repo, userProv, idGen, exprEval,
    engine.WithSurrogateRepository(ext))            // ext: spi.ProcessExtRepository

// ② 门面装配路：facade.New 传了 ext 就自动接到引擎（集成方零额外配置）
f := facade.New(eng, repo, ext)

// ③ 链式 setter（对齐 SetRegistry / SetExtensions 风格）
eng.SetSurrogateRepository(ext)
```

显式关闭（回退"仅台账"）：

```go
engine.New(repo, userProv, idGen, exprEval,
    engine.WithSurrogateRepository(ext), engine.WithSurrogateAutoApply(false))
eng.SetSurrogateAutoApply(false) // 运行期也可关/再开
```

- **未配置扩展仓储（不注入 / 注入 nil）＝ 静默跳过**，不报错、不打断建单；
  委托查询本身报错时也只记日志跳过该参与者。
- 查询判据（内存仓与 JDBC 仓逐条等价）：空 `processName` 全流程兜底（先精确再兜底）、
  时间窗 `start<=now<=end`（任一侧 NULL＝该侧不限）、`surrogate <> operator` 自委托过滤、
  `enabled` 只认 1；多行命中取 ID 最大者。对拍用例见 `internal/surrparity`
  （`memory/ext_parity_test.go` 与 `repository/jdbc/surrogate_test.go` 跑同一份数据期望）。
- 只对"解析出的那批参与者"各查一次，被委托人不再级联委托（A→B、B→C 只并入 B）。
- 委托在待办列表的合并展示属集成层视图职责，引擎不改 todoList。
