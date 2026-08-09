package jeeflow_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

func setup() (*engine.EngineImpl, *memory.Repository) {
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	return eng, repo
}

func registerFlow(repo *memory.Repository, filename string) *model.ProcessDefine {
	data, err := os.ReadFile("../jeeflow-java/jeeflow-core/src/test/resources/flows/" + filename)
	if err != nil {
		panic(err.Error())
	}
	def := &model.ProcessDefine{Name: filename, DisplayName: filename, Type: "test", State: 1, Content: data}
	repo.AddDefine(def)
	return def
}

type testUserProv struct{}

func (p *testUserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID, DeptID: "D01", DeptName: "测试部门", PostID: "P01", PostName: "测试岗位"}, nil
}

type testIDGen struct{ n int64 }

func (g *testIDGen) NextID() int64 { g.n++; return g.n }

type testExprEval struct{}

func (e *testExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	if v, ok := vars["amount"]; ok {
		amt := toFloat(v)
		if expr == "amount > 1000" {
			return amt > 1000, nil
		}
		if expr == "amount <= 1000" {
			return amt <= 1000, nil
		}
	}
	return false, nil
}
func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	}
	return 0
}

var _ spi.ProcessRepository // ensure import

// 模拟 boot2 startAndExecute 契约：启动后自动完成申请节点
func startAndExecute(eng *engine.EngineImpl, repo *memory.Repository, defineID int64, operator string, args map[string]interface{}) *model.ProcessInstance {
	inst, _ := eng.StartProcessInstanceByID(context.Background(), defineID, operator, args)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	for _, task := range doing {
		if task.TaskName == "apply" {
			repo.AddTaskActor(context.Background(), task.ID, []string{operator})
			eng.ExecuteProcessTask(context.Background(), task.ID, operator, nil)
		}
	}
	return inst
}

func Test01SimpleFlow(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "01-simple.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	// issue 29：autoGenTitle 自动生成验证
	if title, ok := inst.Variables[engine.KeyAutoGenTitle].(string); !ok || title == "" {
		t.Fatalf("autoGenTitle should be set in instance variables, got: %v", inst.Variables[engine.KeyAutoGenTitle])
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatal("expected task1")
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	inst, _ = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "applicant", nil)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test02MultiTask(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatal("expected task1")
	}
	// t1→t2
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userA"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userA")
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userA", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatal("expected task2")
	}
	// t2→t3
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userB"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userB")
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userB", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task3" {
		t.Fatal("expected task3")
	}
	// t3→end
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userC"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userC")
	inst, _ = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userC", nil)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test03DecisionExpr(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "03-decision-expr.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", map[string]interface{}{"amount": float64(3000)})
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "applicant", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatalf("expected task2, got %s", doing[0].TaskName)
	}
}

func Test04ForkJoin(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "04-fork-join.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 2 {
		t.Fatalf("expected 2, got %d", len(doing))
	}
	var tA, tB *model.ProcessTask
	for _, d := range doing {
		if d.TaskName == "taskA" {
			tA = d
		} else {
			tB = d
		}
	}
	repo.AddTaskActor(context.Background(), tA.ID, []string{"userA"})
	tA.ActorIDs = append(tA.ActorIDs, "userA")
	eng.ExecuteProcessTask(context.Background(), tA.ID, "userA", nil)
	inst, _ = repo.FindInstanceByID(context.Background(), inst.ID)
	if inst.State != model.InstanceStateDoing {
		t.Fatal("should still be doing")
	}
	repo.AddTaskActor(context.Background(), tB.ID, []string{"userB"})
	tB.ActorIDs = append(tB.ActorIDs, "userB")
	inst, _ = eng.ExecuteProcessTask(context.Background(), tB.ID, "userB", nil)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test05CountersignParallel(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "05-countersign-parallel.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 3 {
		t.Fatalf("expected 3, got %d", len(doing))
	}
	for _, a := range []string{"userA", "userB", "userC"} {
		d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		task := d[0]
		repo.AddTaskActor(context.Background(), task.ID, []string{a})
		task.ActorIDs = append(task.ActorIDs, a)
		eng.ExecuteProcessTask(context.Background(), task.ID, a, nil)
	}
	inst, _ = repo.FindInstanceByID(context.Background(), inst.ID)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test06CountersignSequential(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "06-countersign-sequential.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 {
		t.Fatalf("expected 1, got %d", len(doing))
	}
	task := doing[0]
	repo.AddTaskActor(context.Background(), task.ID, []string{"userA"})
	task.ActorIDs = append(task.ActorIDs, "userA")
	eng.ExecuteProcessTask(context.Background(), task.ID, "userA", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 {
		t.Fatalf("step2 expected 1, got %d", len(doing))
	}
	task = doing[0]
	repo.AddTaskActor(context.Background(), task.ID, []string{"userB"})
	task.ActorIDs = append(task.ActorIDs, "userB")
	inst, _ = eng.ExecuteProcessTask(context.Background(), task.ID, "userB", nil)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test07CountersignRatio(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "07-countersign-ratio.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 4 {
		t.Fatalf("expected 4, got %d", len(doing))
	}
	for _, a := range []string{"userA", "userB", "userC", "userD"} {
		d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		task := d[0]
		repo.AddTaskActor(context.Background(), task.ID, []string{a})
		task.ActorIDs = append(task.ActorIDs, a)
		eng.ExecuteProcessTask(context.Background(), task.ID, a, nil)
	}
	inst, _ = repo.FindInstanceByID(context.Background(), inst.ID)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

func Test08Reject(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	inst, _ = eng.ExecuteAndJumpToEnd(context.Background(), doing[0].ID, "applicant", nil)
	if inst.State != model.InstanceStateReject {
		t.Fatalf("expected reject, got %d", inst.State)
	}
}

func Test09ActorNotAllowed(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = []string{"leader"}
	_, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "intruder", nil)
	if err == nil {
		t.Fatal("expected permission error")
	}
	_ = inst
}

func Test10InterceptorAndEvents(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "01-simple.json")

	var preCalled, postCalled bool
	var events []string

	eng.SetExtensions(&engine.Extensions{
		Interceptors: []engine.FlowInterceptor{
			&testInterceptor{pre: func(node *model.FlowNode, inst *model.ProcessInstance) bool {
				preCalled = true
				return true
			}, post: func(node *model.FlowNode, inst *model.ProcessInstance) {
				postCalled = true
			}, order: 1},
		},
		Listeners: []engine.ProcessEventListener{
			func(evt engine.ProcessEvent) {
				switch evt.Type {
				case engine.EventProcessStart:
					events = append(events, "start")
				case engine.EventTaskComplete:
					events = append(events, "taskDone")
				case engine.EventProcessFinish:
					events = append(events, "finish")
				}
			},
		},
	})

	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader", nil)

	if !preCalled {
		t.Error("preHandle not called")
	}
	if !postCalled {
		t.Error("postHandle not called")
	}
	if len(events) != 4 {
		t.Errorf("expected 4 events (start+apply+task+finish), got %d: %v", len(events), events)
	}
}

type testInterceptor struct {
	pre   func(node *model.FlowNode, inst *model.ProcessInstance) bool
	post  func(node *model.FlowNode, inst *model.ProcessInstance)
	order int
}

func (ic *testInterceptor) PreHandle(node *model.FlowNode, inst *model.ProcessInstance) bool {
	return ic.pre(node, inst)
}
func (ic *testInterceptor) PostHandle(node *model.FlowNode, inst *model.ProcessInstance) {
	ic.post(node, inst)
}
func (ic *testInterceptor) Order() int { return ic.order }

// ─── assignee 变量解析（v1.0.1，集成反馈③） ──────────────────────────────────

func Test11AssigneeVariableResolution(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "11-assignee-vars.json")

	// ① deptLeader 变量命中 → 参与者 = 变量值
	inst := startAndExecute(eng, repo, def.ID, "applicant", map[string]interface{}{"deptLeader": "L001"})
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("want task1, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 1 || doing[0].ActorIDs[0] != "L001" {
		t.Fatalf("assignee var not resolved: %v", doing[0].ActorIDs)
	}

	// ② 静态字面量 userA,userB（变量未命中）
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "L001", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatalf("want task2, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "userA" || doing[0].ActorIDs[1] != "userB" {
		t.Fatalf("literal actors wrong: %v", doing[0].ActorIDs)
	}

	// ③ 变量未传入 → token 字面量回退（对齐 boot3 args.get(token, token)）
	def = registerFlow(repo, "11-assignee-vars.json")
	inst = startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if doing[0].ActorIDs[0] != "deptLeader" {
		t.Fatalf("want literal deptLeader, got %v", doing[0].ActorIDs)
	}

	// ④ tf_nextNodeOperator 优先于 assignee
	def = registerFlow(repo, "11-assignee-vars.json")
	inst, _ = eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if _, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "applicant",
		map[string]interface{}{engine.KeyNextNodeOperator: "BOSS1,BOSS2"}); err != nil {
		t.Fatalf("execute apply: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "BOSS1" || doing[0].ActorIDs[1] != "BOSS2" {
		t.Fatalf("nextNodeOperator not prioritized: %v", doing[0].ActorIDs)
	}
}

// ─── 系统代执行 flow.auto / flow.admin（v1.0.1，集成反馈④） ───────────────────

func Test12SystemExecuteFlowAuto(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "11-assignee-vars.json")
	inst, _ := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant",
		map[string]interface{}{"deptLeader": "L001"})
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)

	// ① flow.auto 非参与者身份放行（startAndExecute 契约）
	inst, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, engine.KeyAutoExecute, nil)
	if err != nil {
		t.Fatalf("flow.auto should pass: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if doing[0].TaskName != "task1" {
		t.Fatalf("want task1, got %v", doing[0].TaskName)
	}

	// ② 跳过 UserProvider 注入：u_userId 保持发起人
	reloaded, _ := repo.FindInstanceByID(context.Background(), inst.ID)
	if uid, _ := reloaded.Variables[engine.KeyUserID].(string); uid != "applicant" {
		t.Fatalf("user info should not be injected for flow.auto: %v", reloaded.Variables[engine.KeyUserID])
	}

	// ③ flow.admin 放行
	inst, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, engine.KeyAdminID, nil)
	if err != nil {
		t.Fatalf("flow.admin should pass: %v", err)
	}
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if doing[0].TaskName != "task2" {
		t.Fatalf("want task2, got %v", doing[0].TaskName)
	}
}

// ─── issues/16 内置通用 handler（11-assignment-handler.json 全链路）────────────

type testOrgUserProv struct{}

func (p *testOrgUserProv) FindDeptLeaders(deptID string) ([]string, error) {
	if deptID == "D01" {
		return []string{"leader1", "leader2"}, nil
	}
	return nil, nil
}

func (p *testOrgUserProv) FindDeptMainLeaders(deptID string) ([]string, error) {
	if deptID == "D01" {
		return []string{"boss1"}, nil
	}
	return nil, nil
}

func (p *testOrgUserProv) FindByRole(roleCode string) ([]string, error) {
	if roleCode == "task4" {
		return []string{"roleA", "roleB"}, nil
	}
	return nil, nil
}

func Test12BuiltinAssignmentHandlers(t *testing.T) {
	repo := memory.New()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, &testUserProv{}, &testOrgUserProv{})
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetRegistry(reg)

	def := registerFlow(repo, "11-assignment-handler.json")

	// ① FormFieldAssigneeHandler：节点 task1 → args.task1 = userA,userB
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1",
		map[string]interface{}{"task1": "userA,userB"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("want task1, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "userA" || doing[0].ActorIDs[1] != "userB" {
		t.Fatalf("formField actors wrong: %v", doing[0].ActorIDs)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, doing[0].ActorIDs)
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userA", nil)

	// ② OperatorAssignmentHandler：task2 → 发起人 user1
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatalf("want task2, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 1 || doing[0].ActorIDs[0] != "user1" {
		t.Fatalf("operator actors wrong: %v", doing[0].ActorIDs)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, doing[0].ActorIDs)
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1", nil)

	// ③ DeptLeaderAssignmentHandler：task3 → user1 部门 D01 领导 = leader1,leader2
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task3" {
		t.Fatalf("want task3, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "leader1" || doing[0].ActorIDs[1] != "leader2" {
		t.Fatalf("deptLeader actors wrong: %v", doing[0].ActorIDs)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, doing[0].ActorIDs)
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leader1", nil)

	// ④ TaskRoleAssigneeHandler：task4 → roleCode=task4 → roleA,roleB
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task4" {
		t.Fatalf("want task4, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "roleA" || doing[0].ActorIDs[1] != "roleB" {
		t.Fatalf("taskRole actors wrong: %v", doing[0].ActorIDs)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, doing[0].ActorIDs)
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "roleA", nil)

	// 结束
	reloaded, _ := repo.FindInstanceByID(context.Background(), inst.ID)
	if reloaded.State != model.InstanceStateDone {
		t.Fatalf("want finished, got %v", reloaded.State)
	}
}
