package jeeflow_test

import (
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
	if err != nil { panic(err.Error()) }
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
		if expr == "amount > 1000" { return amt > 1000, nil }
		if expr == "amount <= 1000" { return amt <= 1000, nil }
	}
	return false, nil
}
func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64: return val
	case int: return float64(val)
	case int64: return float64(val)
	case json.Number: f, _ := val.Float64(); return f
	}
	return 0
}

var _ spi.ProcessRepository // ensure import

// 模拟 boot2 startAndExecute 契约：启动后自动完成申请节点
func startAndExecute(eng *engine.EngineImpl, repo *memory.Repository, defineID int64, operator string, args map[string]interface{}) *model.ProcessInstance {
	inst, _ := eng.StartProcessInstanceByID(defineID, operator, args)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	for _, task := range doing {
		if task.TaskName == "apply" {
			repo.AddTaskActor(task.ID, []string{operator})
			eng.ExecuteProcessTask(task.ID, operator, nil)
		}
	}
	return inst
}

func Test01SimpleFlow(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "01-simple.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" { t.Fatal("expected task1") }
	repo.AddTaskActor(doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	inst, _ = eng.ExecuteProcessTask(doing[0].ID, "applicant", nil)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test02MultiTask(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" { t.Fatal("expected task1") }
	// t1→t2
	repo.AddTaskActor(doing[0].ID, []string{"userA"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userA")
	eng.ExecuteProcessTask(doing[0].ID, "userA", nil)
	doing, _ = repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" { t.Fatal("expected task2") }
	// t2→t3
	repo.AddTaskActor(doing[0].ID, []string{"userB"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userB")
	eng.ExecuteProcessTask(doing[0].ID, "userB", nil)
	doing, _ = repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task3" { t.Fatal("expected task3") }
	// t3→end
	repo.AddTaskActor(doing[0].ID, []string{"userC"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userC")
	inst, _ = eng.ExecuteProcessTask(doing[0].ID, "userC", nil)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test03DecisionExpr(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "03-decision-expr.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", map[string]interface{}{"amount": float64(3000)})
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	repo.AddTaskActor(doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	eng.ExecuteProcessTask(doing[0].ID, "applicant", nil)
	doing, _ = repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" { t.Fatalf("expected task2, got %s", doing[0].TaskName) }
}

func Test04ForkJoin(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "04-fork-join.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 2 { t.Fatalf("expected 2, got %d", len(doing)) }
	var tA, tB *model.ProcessTask
	for _, d := range doing {
		if d.TaskName == "taskA" { tA = d } else { tB = d }
	}
	repo.AddTaskActor(tA.ID, []string{"userA"})
	tA.ActorIDs = append(tA.ActorIDs, "userA")
	eng.ExecuteProcessTask(tA.ID, "userA", nil)
	inst, _ = repo.FindInstanceByID(inst.ID)
	if inst.State != model.InstanceStateDoing { t.Fatal("should still be doing") }
	repo.AddTaskActor(tB.ID, []string{"userB"})
	tB.ActorIDs = append(tB.ActorIDs, "userB")
	inst, _ = eng.ExecuteProcessTask(tB.ID, "userB", nil)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test05CountersignParallel(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "05-countersign-parallel.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 3 { t.Fatalf("expected 3, got %d", len(doing)) }
	for _, a := range []string{"userA", "userB", "userC"} {
		d, _ := repo.FindDoingTasks(inst.ID, nil)
		task := d[0]
		repo.AddTaskActor(task.ID, []string{a})
		task.ActorIDs = append(task.ActorIDs, a)
		eng.ExecuteProcessTask(task.ID, a, nil)
	}
	inst, _ = repo.FindInstanceByID(inst.ID)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test06CountersignSequential(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "06-countersign-sequential.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 { t.Fatalf("expected 1, got %d", len(doing)) }
	task := doing[0]
	repo.AddTaskActor(task.ID, []string{"userA"})
	task.ActorIDs = append(task.ActorIDs, "userA")
	eng.ExecuteProcessTask(task.ID, "userA", nil)
	doing, _ = repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 1 { t.Fatalf("step2 expected 1, got %d", len(doing)) }
	task = doing[0]
	repo.AddTaskActor(task.ID, []string{"userB"})
	task.ActorIDs = append(task.ActorIDs, "userB")
	inst, _ = eng.ExecuteProcessTask(task.ID, "userB", nil)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test07CountersignRatio(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "07-countersign-ratio.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	if len(doing) != 4 { t.Fatalf("expected 4, got %d", len(doing)) }
	for _, a := range []string{"userA", "userB", "userC", "userD"} {
		d, _ := repo.FindDoingTasks(inst.ID, nil)
		task := d[0]
		repo.AddTaskActor(task.ID, []string{a})
		task.ActorIDs = append(task.ActorIDs, a)
		eng.ExecuteProcessTask(task.ID, a, nil)
	}
	inst, _ = repo.FindInstanceByID(inst.ID)
	if inst.State != model.InstanceStateDone { t.Fatalf("expected done, got %d", inst.State) }
}

func Test08Reject(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	repo.AddTaskActor(doing[0].ID, []string{"applicant"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "applicant")
	inst, _ = eng.ExecuteAndJumpToEnd(doing[0].ID, "applicant", nil)
	if inst.State != model.InstanceStateReject { t.Fatalf("expected reject, got %d", inst.State) }
}

func Test09ActorNotAllowed(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	repo.AddTaskActor(doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = []string{"leader"}
	_, err := eng.ExecuteProcessTask(doing[0].ID, "intruder", nil)
	if err == nil { t.Fatal("expected permission error") }
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
				case engine.EventProcessStart: events = append(events, "start")
				case engine.EventTaskComplete: events = append(events, "taskDone")
				case engine.EventProcessFinish: events = append(events, "finish")
				}
			},
		},
	})

	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	doing, _ := repo.FindDoingTasks(inst.ID, nil)
	repo.AddTaskActor(doing[0].ID, []string{"leader"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "leader")
	eng.ExecuteProcessTask(doing[0].ID, "leader", nil)

	if !preCalled { t.Error("preHandle not called") }
	if !postCalled { t.Error("postHandle not called") }
	if len(events) != 4 { t.Errorf("expected 4 events (start+apply+task+finish), got %d: %v", len(events), events) }
}

type testInterceptor struct {
	pre     func(node *model.FlowNode, inst *model.ProcessInstance) bool
	post    func(node *model.FlowNode, inst *model.ProcessInstance)
	order int
}
func (ic *testInterceptor) PreHandle(node *model.FlowNode, inst *model.ProcessInstance) bool { return ic.pre(node, inst) }
func (ic *testInterceptor) PostHandle(node *model.FlowNode, inst *model.ProcessInstance) { ic.post(node, inst) }
func (ic *testInterceptor) Order() int { return ic.order }
