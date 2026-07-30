# 流程定义格式

jeeflow 使用 LogicFlow JSON 作为流程定义格式，与 Java 版完全一致。

## 顶层结构

```json
{
  "name": "leave",
  "displayName": "请假审批",
  "type": "approval",
  "nodes": [...],
  "edges": [...]
}
```

## 节点类型

| type | 说明 | 必填属性 |
|------|------|----------|
| `snaker:start` | 开始 | - |
| `snaker:end` | 结束 | - |
| `snaker:task` | 任务 | `assignee` |
| `snaker:decision` | 条件分支 | `expr`（节点级） |
| `snaker:fork` | 并行分支 | - |
| `snaker:join` | 并行合并 | - |
| `snaker:custom` | 自定义 | `customClass` |

## 任务节点属性

```json
{
  "id": "t1",
  "type": "snaker:task",
  "properties": {
    "assignee": "leader",        // 固定参与者（逗号分隔多人）
    "form": "leave-form",        // 表单标识
    "taskType": 0,               // 0=普通 1=会签
    "performType": 0,            // 0=普通 1=会签参与
    "countersignType": "PARALLEL" // PARALLEL / SEQUENTIAL / RATIO(0.5)
  },
  "text": { "value": "组长审批" }
}
```

参与者优先级：`assignee` → `assignmentHandler`。`candidateUsers` 仅供前端设计器展示，不生成 actor 记录。

## 条件分支

```json
{
  "nodes": [
    {"id":"decision","type":"snaker:decision","properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000?"}},
    {"id":"manager","type":"snaker:task","properties":{"assignee":"manager"},"text":{"value":"经理审批"}},
    {"id":"director","type":"snaker:task","properties":{"assignee":"director"},"text":{"value":"总监审批"}}
  ],
  "edges": [
    {"id":"e3","sourceNodeId":"decision","targetNodeId":"manager",
     "properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000"}},
    {"id":"e4","sourceNodeId":"decision","targetNodeId":"director",
     "properties":{"expr":"amount <= 1000"},"text":{"value":"金额≤1000"}}
  ]
}
```

边的 `text.value` 用于钉钉模式分支标签展示，`properties.expr` 用于引擎求值。

## Go 代码加载

```go
import (
    "encoding/json"
    "github.com/mldong/jeeflow-go/model"
)

var flow model.FlowModel
json.Unmarshal([]byte(flowJSON), &flow)

def := &model.ProcessDefine{
    Name:    flow.Name,
    Content: []byte(flowJSON),
}
repo.AddDefine(def)
```
