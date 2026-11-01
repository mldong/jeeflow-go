package jeeflow_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// issues/241：highLight 重放腿在决策节点必须按任务行佐证选边，不得按重提后的实例变量重算
// （直返契约：来源节点之前的条件分支不重算）。
//
// 夹具：start → apply → dept → decision1{amount>1000 → taskHigh(直返档) | amount<=1000 → taskLow} → end。
// 走单：6666 走 taskHigh → 退回发起人 → 改为 666 重提 → 新待办仍落 taskHigh。
// 高亮必须只点亮实际走过的 e0/e1/e2/e3，不得把 taskLow/e4/e6 算进历史。
const i241ReturnHighlightFlow = `{
  "name": "i241-return-highlight", "displayName": "直返重提高亮", "type": "approval",
  "nodes": [
    {"id": "start", "type": "snaker:start"},
    {"id": "apply", "type": "snaker:task",
     "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}},
    {"id": "dept", "type": "snaker:task",
     "properties": {"assignee": "leader", "taskType": 0, "performType": 0}},
    {"id": "decision1", "type": "snaker:decision", "properties": {"expr": "f_amount > 1000"}},
    {"id": "taskHigh", "type": "snaker:task",
     "properties": {"assignee": "director", "taskType": 0, "performType": 0, "reapplyMode": "RETURN_TO_SOURCE"}},
    {"id": "taskLow", "type": "snaker:task",
     "properties": {"assignee": "manager", "taskType": 0, "performType": 0}},
    {"id": "end", "type": "snaker:end"}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "dept", "properties": {}},
    {"id": "e2", "sourceNodeId": "dept", "targetNodeId": "decision1", "properties": {}},
    {"id": "e3", "sourceNodeId": "decision1", "targetNodeId": "taskHigh", "properties": {"expr": "f_amount > 1000"}},
    {"id": "e4", "sourceNodeId": "decision1", "targetNodeId": "taskLow", "properties": {"expr": "f_amount <= 1000"}},
    {"id": "e5", "sourceNodeId": "taskHigh", "targetNodeId": "end", "properties": {}},
    {"id": "e6", "sourceNodeId": "taskLow", "targetNodeId": "end", "properties": {}}
  ]
}`

func registerFlowContent(repo *memory.Repository, name, content string) *model.ProcessDefine {
	def := &model.ProcessDefine{Name: name, DisplayName: name, Type: "test", State: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return def
}

func i241Exec(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, task *model.ProcessTask,
	op string, args map[string]interface{}) {
	t.Helper()
	rAct(t, repo, task, op)
	if _, err := eng.ExecuteProcessTask(context.Background(), task.ID, op, args); err != nil {
		t.Fatalf("execute %s: %v", task.TaskName, err)
	}
}

func TestReapplyHighLightNotLightUnvisitedBranch241(t *testing.T) {
	eng, repo := setup()
	def := registerFlowContent(repo, "i241-return-highlight", i241ReturnHighlightFlow)
	inst := startAndExecute(eng, repo, def.ID, "zhangsan", map[string]interface{}{"f_amount": 6666})
	iid := inst.ID

	dept := rDoing(t, repo, iid, "dept")
	if dept == nil {
		t.Fatal("夹具自证：应走到 dept")
	}
	rExec(t, eng, repo, dept, "leader", 1)
	high := rDoing(t, repo, iid, "taskHigh")
	if high == nil {
		t.Fatal("夹具自证：6666 应走 taskHigh")
	}
	if rDoing(t, repo, iid, "taskLow") != nil {
		t.Fatal("6666 不得走 taskLow")
	}

	rReturn(t, eng, repo, high, "director")
	app := rDoing(t, repo, iid, "apply")
	if app == nil {
		t.Fatal("退回后应有申请待办")
	}
	i241Exec(t, eng, repo, app, "zhangsan",
		map[string]interface{}{"submitType": 5, "f_amount": 666})
	if rDoing(t, repo, iid, "taskHigh") == nil {
		t.Fatal("重提必须回原来源节点")
	}
	if rDoing(t, repo, iid, "taskLow") != nil {
		t.Fatal("不得按改后的条件改走 taskLow")
	}

	f := facade.New(eng, repo, memory.NewExt())
	r := f.Flow("processInstance/highLight", map[string]interface{}{"id": iid})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	edges := toStrings241(data["historyEdgeNames"])
	nodes := toStrings241(data["historyNodeNames"])

	for _, e := range []string{"e0", "e1", "e2", "e3"} {
		if !contains(edges, e) {
			t.Fatalf("实际走过的边 %s 必须点亮（实际 %v）", e, edges)
		}
	}
	for _, e := range []string{"e4", "e6"} {
		if contains(edges, e) {
			t.Fatalf("未走分支 %s 不得点亮（实际 %v）", e, edges)
		}
	}
	if contains(nodes, "taskLow") {
		t.Fatalf("未走节点 taskLow 不得进历史（实际 %v）", nodes)
	}
	if contains(nodes, "end") {
		t.Fatalf("未走节点 end 不得进历史（实际 %v）", nodes)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// 门面 Flow 的出口经 JSON 归一，字符串数组落地形态是 []interface{}。
func toStrings241(v interface{}) []string {
	out := []string{}
	switch arr := v.(type) {
	case []string:
		out = append(out, arr...)
	case []interface{}:
		for _, x := range arr {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// 通用红样：决策节点的入边来源改成 join（网关不产任务行）。
// 任务行快照实例变量的栈（本栈即如此）在案卷夹具上会被「最老一条前置任务行」偶然救活；
// join 入边让那条通道失效，八栈同夹具都能红。
const i241JoinFlow = `{
  "name": "i241-return-highlight-join", "displayName": "直返重提高亮（汇合后分支）", "type": "approval",
  "nodes": [
    {"id": "start", "type": "snaker:start"},
    {"id": "apply", "type": "snaker:task",
     "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}},
    {"id": "fork1", "type": "snaker:fork", "properties": {}},
    {"id": "taskA", "type": "snaker:task",
     "properties": {"assignee": "userA", "taskType": 0, "performType": 0}},
    {"id": "taskB", "type": "snaker:task",
     "properties": {"assignee": "userB", "taskType": 0, "performType": 0}},
    {"id": "join1", "type": "snaker:join", "properties": {}},
    {"id": "decision1", "type": "snaker:decision", "properties": {"expr": "f_amount > 1000"}},
    {"id": "taskHigh", "type": "snaker:task",
     "properties": {"assignee": "director", "taskType": 0, "performType": 0, "reapplyMode": "RETURN_TO_SOURCE"}},
    {"id": "taskLow", "type": "snaker:task",
     "properties": {"assignee": "manager", "taskType": 0, "performType": 0}},
    {"id": "end", "type": "snaker:end"}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "fork1", "properties": {}},
    {"id": "e2", "sourceNodeId": "fork1", "targetNodeId": "taskA", "properties": {}},
    {"id": "e3", "sourceNodeId": "fork1", "targetNodeId": "taskB", "properties": {}},
    {"id": "e4", "sourceNodeId": "taskA", "targetNodeId": "join1", "properties": {}},
    {"id": "e5", "sourceNodeId": "taskB", "targetNodeId": "join1", "properties": {}},
    {"id": "e6", "sourceNodeId": "join1", "targetNodeId": "decision1", "properties": {}},
    {"id": "e7", "sourceNodeId": "decision1", "targetNodeId": "taskHigh", "properties": {"expr": "f_amount > 1000"}},
    {"id": "e8", "sourceNodeId": "decision1", "targetNodeId": "taskLow", "properties": {"expr": "f_amount <= 1000"}},
    {"id": "e9", "sourceNodeId": "taskHigh", "targetNodeId": "end", "properties": {}},
    {"id": "e10", "sourceNodeId": "taskLow", "targetNodeId": "end", "properties": {}}
  ]
}`

func TestReapplyHighLightNotLightUnvisitedBranchBehindJoin241(t *testing.T) {
	eng, repo := setup()
	def := registerFlowContent(repo, "i241-return-highlight-join", i241JoinFlow)
	inst := startAndExecute(eng, repo, def.ID, "zhangsan", map[string]interface{}{"f_amount": 6666})
	iid := inst.ID
	for _, name := range []string{"taskA", "taskB"} {
		task := rDoing(t, repo, iid, name)
		if task == nil {
			t.Fatalf("夹具自证：并行分支 %s 应就绪", name)
		}
		actor := "userA"
		if name == "taskB" {
			actor = "userB"
		}
		rExec(t, eng, repo, task, actor, 1)
	}
	high := rDoing(t, repo, iid, "taskHigh")
	if high == nil {
		t.Fatal("夹具自证：汇合后 6666 应走 taskHigh")
	}
	rReturn(t, eng, repo, high, "director")
	app := rDoing(t, repo, iid, "apply")
	if app == nil {
		t.Fatal("退回后应有申请待办")
	}
	i241Exec(t, eng, repo, app, "zhangsan", map[string]interface{}{"submitType": 5, "f_amount": 666})
	if rDoing(t, repo, iid, "taskHigh") == nil {
		t.Fatal("重提必须回原来源节点")
	}

	f := facade.New(eng, repo, memory.NewExt())
	r := f.Flow("processInstance/highLight", map[string]interface{}{"id": iid})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	edges := toStrings241(data["historyEdgeNames"])
	nodes := toStrings241(data["historyNodeNames"])
	if !contains(edges, "e7") {
		t.Fatalf("汇合后的实际分支 e7 必须点亮（实际 %v）", edges)
	}
	for _, e := range []string{"e8", "e10"} {
		if contains(edges, e) {
			t.Fatalf("未走分支 %s 不得点亮（实际 %v）", e, edges)
		}
	}
	if contains(nodes, "taskLow") || contains(nodes, "end") {
		t.Fatalf("未走节点不得进历史（实际 %v）", nodes)
	}
}
