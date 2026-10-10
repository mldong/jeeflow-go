// issues/164 · go 决策节点「零命中 ⇒ 显式报错（NOT_FOUND_NEXT_NODE 同档）」——对齐 java。
//
// 改前形状：evaluateDecision 走完表达式臂全不命中后 `return nil`——令牌不前进也不报错，
// 办理人看到 code=0"成功"、实例停在 state=10 且没有任何人的待办（僵尸实例；issues/166 的
// 种子三行就是被这个静默停住骗过去的）。本案 A（2026-10-11 拍，python issues/161 已走同一条
// 路）＝零命中抛 `decision节点无法确定下一步执行路线`——文案逐字取 java
// `WfErrEnum.NOT_FOUND_NEXT_NODE`（DecisionModel.exec 尾部 !found 那支）。
//
// 判据档：
// ① amount=500（对 `amount>1000`／`amount<0` 两 expr 全 false）⇒ execute 返回错误且文案与
//    java 逐字同文；实例停在 DOING、零后续待办。
// ② 对照档 amount=5000 ⇒ e3 为真照常走 task2——证明抛错只在"零命中"档，没把正常分支打断。
// ③ 缺 amount（两 expr 同样全 false）⇒ 同档报错（"变量没填"是现实里最主要的坏定义形状）。
//
// 夹具＝**内联**决策流程（start → apply → task1 → decision 两 expr 边 → task2/task3 → end，
// 与活栈探针/共享夹具 03-decision-expr 同形）；不新增共享 flows/ 夹具（编辑源在 jeeflow-java）。
package jeeflow_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

const i164NotFoundNextNodeMsg = "decision节点无法确定下一步执行路线"

func i164Flow(name string) string {
	return `{"name":"` + name + `","displayName":"决策无命中(164夹具)","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","performType":0},"text":{"value":"发起申请"}},` +
		`{"id":"task1","type":"snaker:task","properties":{"assignee":"leader","performType":0},"text":{"value":"填写报销单"}},` +
		`{"id":"decision1","type":"snaker:decision","properties":{"expr":"","handleClass":""},"text":{"value":"金额判断"}},` +
		`{"id":"task2","type":"snaker:task","properties":{"assignee":"manager","performType":0},"text":{"value":"经理审批"}},` +
		`{"id":"task3","type":"snaker:task","properties":{"assignee":"director","performType":0},"text":{"value":"总监审批"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],` +
		`"edges":[{"id":"e0","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e1","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},` +
		`{"id":"e2","sourceNodeId":"task1","targetNodeId":"decision1","properties":{}},` +
		`{"id":"e3","sourceNodeId":"decision1","targetNodeId":"task2","properties":{"expr":"amount > 1000"}},` +
		`{"id":"e4","sourceNodeId":"decision1","targetNodeId":"task3","properties":{"expr":"amount < 0"}},` +
		`{"id":"e5","sourceNodeId":"task2","targetNodeId":"end","properties":{}},` +
		`{"id":"e6","sourceNodeId":"task3","targetNodeId":"end","properties":{}}]}`
}

func i164Setup(t *testing.T, name string) (*engine.EngineImpl, *memory.Repository, *model.ProcessInstance) {
	t.Helper()
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &i163ExprEval{})
	def := &model.ProcessDefine{Name: name, DisplayName: name, Type: "approval",
		State: 1, Version: 1, Content: []byte(i164Flow(name))}
	repo.AddDefine(def)
	inst := startAndExecute(eng, repo, def.ID, "user1", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("夹具：发起后应停在 task1，实得 %v", doing)
	}
	return eng, repo, inst
}

func i164SubmitTask1(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, instID int64, args map[string]interface{}) error {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instID, nil)
	if len(doing) != 1 {
		t.Fatalf("task1 应恰有 1 条待办，实得 %d", len(doing))
	}
	task := doing[0]
	repo.AddTaskActor(context.Background(), task.ID, []string{"leader"})
	task.ActorIDs = append(task.ActorIDs, "leader")
	_, err := eng.ExecuteProcessTask(context.Background(), task.ID, "leader", args)
	return err
}

// TestI164NoMatchErrors ① 核心格：两 expr 全 false ⇒ 显式报错（改前此格红：code=0 静默停住）。
func TestI164NoMatchErrors(t *testing.T) {
	eng, repo, inst := i164Setup(t, "i164-nomatch")

	err := i164SubmitTask1(t, eng, repo, inst.ID, map[string]interface{}{"amount": 500})
	if err == nil {
		t.Fatal("两 expr 边全 false 必须报错（改前此格红：静默停住不报错）")
	}
	if !strings.Contains(err.Error(), i164NotFoundNextNodeMsg) {
		t.Fatalf("文案须与 java NOT_FOUND_NEXT_NODE 逐字同文 %q，实得 %q", i164NotFoundNextNodeMsg, err.Error())
	}
	if st := i163State(t, repo, inst.ID); st != model.InstanceStateDoing {
		t.Fatalf("任务已消耗，实例应仍停在 DOING: state=%d", st)
	}
	if d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil); len(d) != 0 {
		t.Fatalf("零命中后不得再产生任何待办（僵尸实例判据），实得 %d", len(d))
	}
}

// TestI164MatchStillRoutes ② 对照档：amount=5000 ⇒ e3 为真照常走 task2。
func TestI164MatchStillRoutes(t *testing.T) {
	eng, repo, inst := i164Setup(t, "i164-match")

	if err := i164SubmitTask1(t, eng, repo, inst.ID, map[string]interface{}{"amount": 5000}); err != nil {
		t.Fatalf("有命中档不得报错: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatalf("amount=5000 应走 task2，实得 %v", doing)
	}
}

// TestI164MissingVarErrors ③ 缺 amount ⇒ 同档报错（变量没填是最主要的坏定义形状）。
func TestI164MissingVarErrors(t *testing.T) {
	eng, repo, inst := i164Setup(t, "i164-missing")

	err := i164SubmitTask1(t, eng, repo, inst.ID, nil)
	if err == nil || !strings.Contains(err.Error(), i164NotFoundNextNodeMsg) {
		t.Fatalf("缺判定变量也必须同档报错，实得 %v", err)
	}
}
