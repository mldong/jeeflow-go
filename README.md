# jeeflow-go · Go 版工作流引擎

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache-2.0-orange)](./LICENSE)

[jeeflow](https://github.com/mldong/jeeflow-doc) 引擎规范的 **Go 语言实现**，零外部依赖，纯 stdlib。

---

## 快速开始

```go
package main

import (
    "github.com/mldong/jeeflow-go/engine"
    "github.com/mldong/jeeflow-go/memory"
    "github.com/mldong/jeeflow-go/model"
)

func main() {
    repo := memory.New()
    eng := engine.New(repo, nil, nil, nil)

    // 注册流程定义
    def := &model.ProcessDefine{
        Name: "leave", DisplayName: "请假审批",
        Content: []byte(`{"nodes":[...],"edges":[...]}`),
    }
    repo.AddDefine(def)

    // 发起流程
    inst, _ := eng.StartProcessInstanceByID(def.ID, "张三", nil)

    // 查询待办
    tasks, _ := repo.FindDoingTasks(inst.ID, nil)

    // 审批
    eng.ExecuteProcessTask(tasks[0].ID, "张三", nil)
}
```

## 安装

```bash
go get github.com/mldong/jeeflow-go
```

导入引擎核心（**零第三方依赖**）：

```go
import "github.com/mldong/jeeflow-go/engine"
```

## 目录结构

```
jeeflow-go/
├── engine/          ← 引擎核心（对标 Java com.mldong.jeeflow.core）
├── model/           ← 域类型 + LogicFlow 模型
├── spi/             ← SPI 接口（对标 SPEC.md §6）
├── memory/          ← 内存仓储（测试用）
├── repository/jdbc/ ← MySQL 参考实现（含集成测试）
├── demo/            ← GoFrame REST 演示
├── cmd/demo/        ← 演示入口
└── engine_test.go   ← 合规测试
```

## 节点支持

| 节点 | 状态 |
|------|------|
| start / end | ✅ |
| task（线性审批） | ✅ |
| decision（条件分支） | ✅ |
| fork / join（并行分支） | ✅ |
| countersign（并行会签） | ✅ |
| countersign（串行会签） | ✅ |
| countersign（按比例） | ✅ |
| reject（驳回） | ✅ |
| jump（跳转） | ✅ |

## 规范

对齐 [jeeflow-doc](https://github.com/mldong/jeeflow-doc) v1.0，与 Java 版共享同一套流程 JSON 文件驱动测试。

## License

Apache-2.0

## 文档

| 文档 | 说明 |
|------|------|
| [快速开始](docs/getting-started.md) | 安装 + 5 分钟上手 |
| [引擎 API](docs/engine-api.md) | 4 个核心操作 + 状态码 |
| [流程定义](docs/flow-definition.md) | LogicFlow JSON 格式 |
| [SPI 扩展](docs/spi-guide.md) | 仓储/用户/ID 生成器 |
| [GoFrame 集成](docs/goframe-demo.md) | REST 演示站 |
