# 业务数据入库（persist 组件）

> issues/18 · 1.6.2 起随 module 发布（go get 同 module，无需新 tag）

`persist/` 子包：**引擎无关的动态表写入组件 + 工作流入库适配拦截器**。
规范契约见文档站《09 · 业务数据通用入库》；本页是 Go 语言视角。

## 引入

```go
import "github.com/mldong/jeeflow-go/persist"
```

## 动态表写入（引擎无关）

```go
writer := persist.NewJdbcDynamicTableWriter(db) // *sql.DB；sqlite 走 PRAGMA，MySQL/PG 走 information_schema

// ① 列过滤
kept, _ := writer.FilterColumns("biz_leave", []string{"title", "ghost_col"})
// ② 幂等检查
ok, _ := writer.Exists("biz_leave", "process_instance_id", instID)
// ③ 系统字段填充 + 参数化插入
data := map[string]interface{}{"title": "年假申请"}
writer.FillSystemFields(data, true)
writer.Insert("biz_leave", data)
```

安全：`sys_` 前缀表拒绝写入；非法字符表名拒绝；值走参数化占位符。

## 流程入库拦截器（流程结束同意自动落表）

```go
writer := persist.NewJdbcDynamicTableWriter(db)
ic := persist.NewPersistPostInterceptor(writer, repo.FindDefineByID) // loader 透传 findDefineById
eng.SetExtensions(&engine.Extensions{Interceptors: []engine.FlowInterceptor{ic}})
```

- 拦截器挂在引擎全局 Extensions；内部按「结束节点 + 实例 Done + submitType=AGREE」过滤，
  仅对流程定义顶层声明了 `relTableName`（缺省回落流程 name）的流程生效
- 语义：实例 `f_` 字段（去前缀）+ 流程上下文（`process_instance_id`/`apply_user_id`/`apply_dept_id`）
  + 系统字段写入业务表；`process_instance_id` 幂等（先查后插）+ 同链内存标记（1.6.3，共享 inst.Variables，不落库）；用户列默认取 operator（1.6.3）；表不存在 panic（配置错误快速失败）；
  不同意/退回不入库
- 引擎对齐（1.6.2）：任务完成后结束节点统一走 `executeNode`，拦截器在流程结束时完整触发

## 测试

```bash
go test ./persist/   # 13 用例：writer 8 + 拦截器集成 5（SQLite 内存库全链路）
```

> 测试依赖 `modernc.org/sqlite`（纯 Go，无 CGO）。
