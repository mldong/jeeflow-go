# GoFrame 集成

`jeeflow-go` 内置 GoFrame v2 演示模块，可作为集成参考。

## 启动

```bash
cd jeeflow-go
go run ./cmd/demo/
# http://localhost:8081
```

## 目录

```
demo/
├── controller.go      ← REST 控制器（7 个端点）
├── web/index.html     ← 前端页面
cmd/demo/main.go       ← 入口
```

## REST API

对齐 Java 版路径：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/stats` | 统计（待办数 + 发起数） |
| POST | `/wf/processDefine/page` | 流程定义列表 |
| POST | `/wf/processInstance/startAndExecute` | 发起流程 |
| POST | `/wf/processInstance/page` | 我的实例 |
| POST | `/wf/processInstance/detail` | 实例详情 + 审批记录 |
| POST | `/wf/processTask/todoList` | 我的待办 |
| POST | `/wf/processTask/execute` | 同意/拒绝 |

## 集成到现有 GoFrame 项目

```go
package main

import (
    "github.com/gogf/gf/v2/frame/g"
    "github.com/mldong/jeeflow-go/demo"
)

func main() {
    s := g.Server()
    ctl := demo.New()         // 使用内存仓储
    ctl.RegisterRoutes(s)      // 注册 /wf/* 和 /api/*
    s.Run()
}
```

生产环境替换 `demo.New()` 中的内存仓储为 MySQL 实现：

```go
repo := &MySQLRepository{db: g.DB()}
eng := engine.New(repo, &MyUserProvider{}, nil, nil)
ctl := &CustomController{engine: eng, repo: repo}
```
