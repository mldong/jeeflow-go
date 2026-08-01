# 快速开始

## 安装

```bash
go get github.com/mldong/jeeflow-go
```

## 5 分钟上手

```go
package main

import (
    "fmt"
    "github.com/mldong/jeeflow-go/engine"
    "github.com/mldong/jeeflow-go/memory"
    "github.com/mldong/jeeflow-go/model"
)

func main() {
    // 1. 创建引擎（内存仓储，测试用）
    repo := memory.New()
    eng := engine.New(repo, nil, nil, nil)

    // 2. 注册流程定义（LogicFlow JSON）
    flowJSON := `{
      "name":"leave","displayName":"请假审批",
      "nodes":[
        {"id":"start","type":"snaker:start","text":{"value":"开始"}},
        {"id":"t1","type":"snaker:task","properties":{"assignee":"leader"},"text":{"value":"组长审批"}},
        {"id":"end","type":"snaker:end","text":{"value":"结束"}}
      ],
      "edges":[
        {"id":"e1","sourceNodeId":"start","targetNodeId":"t1"},
        {"id":"e2","sourceNodeId":"t1","targetNodeId":"end"}
      ]
    }`
    def := &model.ProcessDefine{
        Name: "leave", DisplayName: "请假审批",
        Content: []byte(flowJSON),
    }
    repo.AddDefine(def)

    // 3. 发起流程
    inst, _ := eng.StartProcessInstanceByID(def.ID, "张三", nil)
    fmt.Printf("实例 #%d 已创建\n", inst.ID)

    // 4. 查询待办
    tasks, _ := repo.FindDoingTasks(inst.ID, nil)
    fmt.Printf("%d 个待办任务\n", len(tasks))

    // 5. 审批
    repo.AddTaskActor(tasks[0].ID, []string{"leader"})
    inst, _ = eng.ExecuteProcessTask(tasks[0].ID, "leader", nil)
    fmt.Printf("流程状态: %d (20=完成)\n", inst.State)
}
```

## 运行演示站

```bash
git clone https://github.com/mldong/jeeflow-go
cd jeeflow-go
go run ./cmd/demo/
# 打开 http://localhost:8081
```

从 `jeeflow-java` 仓库的共享流程 JSON 加载 10 条示例流程（含简单/多级/决策/会签/驳回/混合模式）。
