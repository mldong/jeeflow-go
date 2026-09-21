package jeeflow_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/internal/flowsutil"
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
	data, err := os.ReadFile(filepath.Join(flowsutil.Dir(), filename))
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

// Test02B_InitiatorURealNameStable issues/97 对齐 Java：execute 只把表单字段并入实例，
// 不覆盖实例级 u_*——实例 u_realName 恒为发起人（start 注入），操作人 u_* 只留在任务行 ext。
func Test02B_InitiatorURealNameStable(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	// 发起人 alice 发起并自动完成 apply
	inst := startAndExecute(eng, repo, def.ID, "alice", nil)
	if inst.Variables[engine.KeyRealName] != "用户alice" {
		t.Fatalf("start: 实例 u_realName 应为发起人 alice，got %v", inst.Variables[engine.KeyRealName])
	}
	// 第一审批节点 bob 办理
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatal("expected task1")
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"bob"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "bob")
	doneTaskID := doing[0].ID
	inst, _ = eng.ExecuteProcessTask(context.Background(), doneTaskID, "bob", nil)

	// 核心断言：实例 u_realName 恒为发起人 alice，不被操作人 bob 覆盖
	if got := inst.Variables[engine.KeyRealName]; got != "用户alice" {
		t.Fatalf("issues/97: 实例 u_realName 应恒为发起人 alice，实际漂移为 %v", got)
	}
	if got := inst.Variables[engine.KeyUserID]; got != "alice" {
		t.Fatalf("issues/97: 实例 u_userId 应恒为发起人 alice，实际为 %v", got)
	}
	// autoGenTitle 前缀（发起人）与实例 u_realName 一致
	if title, _ := inst.Variables[engine.KeyAutoGenTitle].(string); !strings.HasPrefix(title, "用户alice的") {
		t.Fatalf("issues/97: autoGenTitle 前缀应为发起人，got %v", title)
	}
	// 任务行 ext（facade 操作人来源）保留操作人 bob 的 u_*
	doneTask, _ := repo.FindTaskByID(context.Background(), doneTaskID)
	if doneTask == nil {
		t.Fatal("completed task1 not found")
	}
	if got := doneTask.Variables[engine.KeyRealName]; got != "用户bob" {
		t.Fatalf("issues/97: 任务行 u_realName 应为操作人 bob，got %v", got)
	}
	// 深层节点再办一次（userB 办 task2），实例 u_realName 仍为 alice
	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		t.Fatalf("expected task2, got %v", len(doing))
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userB"})
	doing[0].ActorIDs = append(doing[0].ActorIDs, "userB")
	inst, _ = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userB", nil)
	if got := inst.Variables[engine.KeyRealName]; got != "用户alice" {
		t.Fatalf("issues/97: 深层节点后实例 u_realName 应仍为 alice，实际 %v", got)
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

// Test14TaskCreateEvent TASK_CREATE fire 契约（对齐 Java CreateTaskHandler / Rust engine.rs）：
// 任务落库**之后**逐个 fire，事件带 TaskID/NodeID/Operator，监听器可按 TaskID 反查任务 actor。
// 覆盖：① 普通任务 ② 并行会签多任务逐个 ③ fire 时机（落库后，反查非空）。
func Test14TaskCreateEvent(t *testing.T) {
	ctx := context.Background()

	// ① 普通任务：01-simple.json startAndExecute → apply 完成 → task1 创建
	t.Run("normal", func(t *testing.T) {
		eng, repo := setup()
		def := registerFlow(repo, "01-simple.json")
		var creates []engine.ProcessEvent
		eng.SetExtensions(&engine.Extensions{
			Listeners: []engine.ProcessEventListener{
				func(evt engine.ProcessEvent) {
					if evt.Type == engine.EventTaskCreate {
						creates = append(creates, evt)
					}
				},
			},
		})
		inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
		// start(apply) + task1 = 2 个任务创建事件
		if len(creates) != 2 {
			t.Fatalf("want 2 TASK_CREATE (apply+task1), got %d", len(creates))
		}
		// 落库后反查：每个事件 TaskID 必须可查到（fire 在 SaveTask 之后）、
		// NodeID/Operator 非空、任务 actor 非空（监听器按 TaskID 反查 actor 的前提）
		for _, c := range creates {
			task, _ := repo.FindTaskByID(ctx, c.TaskID)
			if task == nil {
				t.Fatalf("TASK_CREATE TaskID=%d 落库后反查为空（fire 时机过早）", c.TaskID)
			}
			if c.InstanceID != inst.ID || c.NodeID == "" || c.Operator == "" {
				t.Fatalf("event 字段不全: %+v", c)
			}
			if len(task.ActorIDs) == 0 || task.ActorIDs[0] == "" {
				t.Fatalf("task actor 应非空: task=%+v", task.ActorIDs)
			}
		}
		_ = inst
	})

	// ② 并行会签：userA/userB/userC 三人逐任务 fire（会签多任务逐个）
	t.Run("countersign-parallel", func(t *testing.T) {
		eng, repo := setup()
		def := registerFlow(repo, "05-countersign-parallel.json")
		var creates []engine.ProcessEvent
		eng.SetExtensions(&engine.Extensions{
			Listeners: []engine.ProcessEventListener{
				func(evt engine.ProcessEvent) {
					if evt.Type == engine.EventTaskCreate {
						creates = append(creates, evt)
					}
				},
			},
		})
		startAndExecute(eng, repo, def.ID, "applicant", nil)
		// apply(1) + 会签 task1 三人(3) = 4 个任务创建事件
		if len(creates) != 4 {
			t.Fatalf("want 4 TASK_CREATE (apply+3会签), got %d", len(creates))
		}
		// 会签三任务 NodeID 相同、TaskID 互不相同、逐个落库可反查
		var csIDs []int64
		for _, c := range creates[1:] {
			if c.NodeID == "" {
				t.Fatalf("会签事件 NodeID 空: %+v", c)
			}
			task, _ := repo.FindTaskByID(ctx, c.TaskID)
			if task == nil {
				t.Fatalf("会签 TASK_CREATE TaskID=%d 反查为空", c.TaskID)
			}
			csIDs = append(csIDs, c.TaskID)
		}
		seen := map[int64]bool{}
		for _, id := range csIDs {
			if seen[id] {
				t.Fatalf("会签 TaskID 重复: %v", csIDs)
			}
			seen[id] = true
		}
	})
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

func Test13FormFieldAssigneeFPrefix(t *testing.T) {
	repo := memory.New()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, &testUserProv{}, &testOrgUserProv{})
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetRegistry(reg)

	def := registerFlow(repo, "11-assignment-handler.json")

	// ① f_ 前缀变量（前端表单提交格式）
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1",
		map[string]interface{}{"f_task1": "userA,userB"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("want task1, got %+v", doing)
	}
	if len(doing[0].ActorIDs) != 2 || doing[0].ActorIDs[0] != "userA" || doing[0].ActorIDs[1] != "userB" {
		t.Fatalf("① f_ prefix actors: %v", doing[0].ActorIDs)
	}
	repo.AddTaskActor(context.Background(), doing[0].ID, doing[0].ActorIDs)
	eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userA", nil)

	// ② f_ 前缀优先于裸名（两者同时存在时 f_ 命中）
	inst2, _ := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1",
		map[string]interface{}{"f_task1": "userX", "task1": "userY"})
	doing2, _ := repo.FindDoingTasks(context.Background(), inst2.ID, nil)
	if len(doing2[0].ActorIDs) != 1 || doing2[0].ActorIDs[0] != "userX" {
		t.Fatalf("② f_ priority actors: %v", doing2[0].ActorIDs)
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// issues/121 P1 · 建单不变量：引擎每次新建任务行必写 task_parent_id（发起那条＝0）
// 与行变量 isFirstTaskNode。夹具一律 ≥3 个任务节点——两步流里"上一节点"与"首任务节点"同格，
// 断言恒真、抓不到东西（规范「引擎操作 04」测试纪律）。
// ═════════════════════════════════════════════════════════════════════════════

// doingByName 取实例下名为 name 的**进行中**任务（一律从仓储读回，不用聚合根内存对象）
func doingByName(t *testing.T, repo *memory.Repository, instanceID int64, name string) *model.ProcessTask {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName == name {
			return tk
		}
	}
	t.Fatalf("应有进行中的 %s 任务（doing 列表里没有）", name)
	return nil
}

// rowOf 读回落库行（历史行/存量行都从这里取，保证断言打在持久值上）
func rowOf(t *testing.T, repo *memory.Repository, taskID int64) *model.ProcessTask {
	t.Helper()
	tk, err := repo.FindTaskByID(context.Background(), taskID)
	if err != nil || tk == nil {
		t.Fatalf("任务行读不回: id=%d err=%v", taskID, err)
	}
	return tk
}

// wantParent 断言落库的血缘指针：nil＝死列没写（本案要消灭的形状），0＝发起那条的正确值
func wantParent(t *testing.T, tk *model.ProcessTask, want int64) {
	t.Helper()
	if tk.ParentTaskID == nil {
		t.Fatalf("%s 行 task_parent_id 未写（nil）——建单不变量 ① 破了", tk.TaskName)
	}
	if *tk.ParentTaskID != want {
		t.Fatalf("%s 行 parent 应为 %d，实际 %d", tk.TaskName, want, *tk.ParentTaskID)
	}
}

// wantFirstFlag 断言行变量里的首任务节点标记（**缺键即红**——落库是本案的产物，不只是值对不对）
func wantFirstFlag(t *testing.T, tk *model.ProcessTask, want bool) {
	t.Helper()
	v, ok := tk.Variables[model.IsFirstTaskNodeKey]
	if !ok {
		t.Fatalf("%s 行变量缺 %s 键——建单不变量 ② 破了（variables=%v）",
			tk.TaskName, model.IsFirstTaskNodeKey, tk.Variables)
	}
	b, ok := v.(bool)
	if !ok || b != want {
		t.Fatalf("%s 行 isFirstTaskNode 应为 %v，实际 %v(%T)", tk.TaskName, want, v, v)
	}
}

// approve 以 actor 办结该任务（参与者先补齐，对齐其他用例姿势）
func approve(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, tk *model.ProcessTask, actor string) {
	t.Helper()
	repo.AddTaskActor(context.Background(), tk.ID, []string{actor})
	tk.ActorIDs = append(tk.ActorIDs, actor)
	if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, actor, nil); err != nil {
		t.Fatalf("execute %s(%d): %v", tk.TaskName, tk.ID, err)
	}
}

// Test150CreateWritesLineageChain 链式四节点（apply→task1→task2→task3）逐条断言血缘指针相接、
// 首节点标记只有 apply 为 true，并验「标记随已办结历史行存活」——本案必须落库（而非门面现算）的唯一理由。
func Test150CreateWritesLineageChain(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	apply := doingByName(t, repo, inst.ID, "apply")
	// 发起 execution 没有当前任务 ⇒ parent 落 0（不是 nil/NULL）；apply 是 start 直接后继 ⇒ true
	wantParent(t, apply, 0)
	wantFirstFlag(t, apply, true)
	approve(t, eng, repo, apply, "applicant")

	task1 := doingByName(t, repo, inst.ID, "task1")
	wantParent(t, task1, apply.ID) // task1 的 parent ＝刚办结的 apply
	wantFirstFlag(t, task1, false) // 非首节点必须 false（否则"parent=0 当首节点"这类假判据蒙不过去）
	approve(t, eng, repo, task1, "leader")

	task2 := doingByName(t, repo, inst.ID, "task2")
	wantParent(t, task2, task1.ID) // 链式血缘：task2.parent == task1.id
	wantFirstFlag(t, task2, false)
	approve(t, eng, repo, task2, "manager")

	task3 := doingByName(t, repo, inst.ID, "task3")
	wantParent(t, task3, task2.ID)
	wantFirstFlag(t, task3, false)

	// 行级标记不得升格为实例变量（P1 承诺"行为零变化"；Java 侧 task 变量根本不并入实例）
	if instNow, _ := repo.FindInstanceByID(context.Background(), inst.ID); instNow == nil {
		t.Fatalf("实例读不回")
	} else if _, leaked := instNow.Variables[model.IsFirstTaskNodeKey]; leaked {
		t.Fatalf("isFirstTaskNode 漏进实例变量（应只活在任务行上）: %v", instNow.Variables)
	}

	// 本案真正要的那格：血缘版回退读的是**已办结的历史行**，标记必须随行存活
	// （门面出口现算版带 doing 判定，历史行上恒 false ⇒ 首节点回退会被错判成普通回退）
	hisApply := rowOf(t, repo, apply.ID)
	if hisApply.TaskState != model.TaskStateDone {
		t.Fatalf("apply 应已办结，实际 state=%d", hisApply.TaskState)
	}
	wantFirstFlag(t, hisApply, true)
	wantParent(t, hisApply, 0) // 历史行的血缘指针不被后续建单路径覆写
}

// Test151CreateWritesLineageParallelCountersign 会签（PARALLEL）：三人各一行，parent 全部指向刚办结的
// apply（"谁造了它们"），非首节点 ⇒ 三行标记一律 false。
func Test151CreateWritesLineageParallelCountersign(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "05-countersign-parallel.json")
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	apply := doingByName(t, repo, inst.ID, "apply")
	wantParent(t, apply, 0)
	wantFirstFlag(t, apply, true)
	approve(t, eng, repo, apply, "applicant")

	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 3 {
		t.Fatalf("并行会签应有 3 条成员任务，实际 %d", len(doing))
	}
	for _, tk := range doing {
		if tk.TaskName != "task1" {
			t.Fatalf("会签行应在 task1 节点，实际 %s", tk.TaskName)
		}
		wantParent(t, tk, apply.ID)
		wantFirstFlag(t, tk, false)
	}
	for _, a := range []string{"userA", "userB", "userC"} {
		d, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		if len(d) == 0 {
			t.Fatalf("会签成员未办完就没有待办了（%s 之前）", a)
		}
		approve(t, eng, repo, d[0], a)
	}
	if inst, _ = repo.FindInstanceByID(context.Background(), inst.ID); inst.State != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", inst.State)
	}
}

// Test152CreateWritesLineageSequentialCountersign 串行会签（SEQUENTIAL）"下一位成员"那条建单路径
// （ExecuteProcessTask 内联建单，不走 createTask）：第二位的 parent＝刚办结的第一位，
// 簿记变量并入后标记仍在（整体覆写变量的话这格会红）。
func Test152CreateWritesLineageSequentialCountersign(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "08-countersign-sequential-approve.json")
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	apply := doingByName(t, repo, inst.ID, "apply")
	wantParent(t, apply, 0)
	wantFirstFlag(t, apply, true)
	approve(t, eng, repo, apply, "applicant")

	first := doingByName(t, repo, inst.ID, "task1") // 首位成员 userA
	wantParent(t, first, apply.ID)
	wantFirstFlag(t, first, false)
	approve(t, eng, repo, first, "userA")

	next := doingByName(t, repo, inst.ID, "task1") // 串行会签"下一位成员"＝另一条新建行
	if next.ID == first.ID {
		t.Fatalf("串行会签下一位应是新建行，不应复用第一位的行")
	}
	wantParent(t, next, first.ID) // parent＝刚办结的那一位，不是 apply
	wantFirstFlag(t, next, false)
	if _, ok := next.Variables["loopCounter_task1"]; !ok {
		t.Fatalf("串行会签簿记变量丢了: %v", next.Variables)
	}
	approve(t, eng, repo, next, "userB")

	// 会签整体办结后进入 approve 节点：parent＝刚办结的最后一位成员
	after := doingByName(t, repo, inst.ID, "approve")
	wantParent(t, after, next.ID)
	wantFirstFlag(t, after, false)
}

// Test153CreateWritesLineageOnRollback 驳回/回退建新任务那条（ExecuteAndJumpTask 空 target＝ROLLBACK）：
// 仍是拓扑版落点，parent 记"谁造了它"＝被回退的当前任务；新行落在首节点上 ⇒ 标记 true，
// 与 parent!=0 同时成立 ⇒ 证明标记是真判据、不是从 parent==0 推出来的。
func Test153CreateWritesLineageOnRollback(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "09-with-reject.json")
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	apply := doingByName(t, repo, inst.ID, "apply")
	wantParent(t, apply, 0)
	wantFirstFlag(t, apply, true)
	approve(t, eng, repo, apply, "applicant")

	task1 := doingByName(t, repo, inst.ID, "task1")
	wantParent(t, task1, apply.ID)
	wantFirstFlag(t, task1, false)

	// 在 task1 上办"退回上一步"⇒ 复活 apply 那条历史行（血缘版，issues/121 P2）
	if _, err := eng.ExecuteAndJumpTask(context.Background(), task1.ID, "leader",
		map[string]interface{}{"submitType": int(model.SubmitTypeRollback)}, ""); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	back := doingByName(t, repo, inst.ID, "apply")
	if back.ID == apply.ID {
		t.Fatalf("回退应新建行，不应复活原 apply 行")
	}
	// 随行拷贝：复活行的 parent＝apply 原行的 parent（发起 execution 无当前任务 ⇒ 0）
	wantParent(t, back, 0)
	wantFirstFlag(t, back, true) // apply 是 start 直接后继
	// 首任务节点行 ⇒ 参与者取该行 u_userId（发起人），不是执行回退的 leader
	if actors, _ := repo.FindTaskActors(context.Background(), back.ID); len(actors) == 0 || actors[0] != "applicant" {
		t.Fatalf("血缘版回退到首节点，参与者应为发起人 applicant，实得 %v", actors)
	}
}

// customFirstFlow 自定义节点作为 start 直接后继的流（Go 的 TypeCustom 建单复用 createTask，
// 与 Java 的 createHistoryTask 形状不同——见收口说明，这里验的就是 Go 那条自定义节点建单路径）
const customFirstFlow = `{
  "name": "custom-first",
  "displayName": "自定义节点首行建单不变量",
  "type": "approval",
  "nodes": [
    {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
    {"id": "cust1", "type": "snaker:custom", "properties": {"assignee": "ext-sys"}, "text": {"value": "通知外部系统"}},
    {"id": "task1", "type": "snaker:task", "properties": {"assignee": "leader", "performType": 0}, "text": {"value": "审批"}},
    {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "cust1", "properties": {}},
    {"id": "e1", "sourceNodeId": "cust1", "targetNodeId": "task1", "properties": {}},
    {"id": "e2", "sourceNodeId": "task1", "targetNodeId": "end", "properties": {}}
  ]
}`

// Test154CreateWritesLineageCustomNode 自定义节点建单同样兑现两条不变量：它是 start 直接后继 ⇒
// 首节点标记 true（Java 的历史行在 Go 里是这条 DOING 行），且发起路径 parent 落 0。
func Test154CreateWritesLineageCustomNode(t *testing.T) {
	eng, repo := setup()
	def := &model.ProcessDefine{Name: "custom-first", DisplayName: "custom-first", Type: "test",
		State: 1, Content: []byte(customFirstFlow)}
	repo.AddDefine(def)
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	cust := doingByName(t, repo, inst.ID, "cust1")
	wantParent(t, cust, 0)
	wantFirstFlag(t, cust, true)
	approve(t, eng, repo, cust, "ext-sys")

	task1 := doingByName(t, repo, inst.ID, "task1")
	wantParent(t, task1, cust.ID)
	wantFirstFlag(t, task1, false)
}

// Test154RollbackLineageNegatives issues/121 P2 两格负向：
// ① 无血缘（parent 为 0，以及 P1 之前老行的 NULL 形状）⇒ 20010007，不得静默不建单；
// ② 血缘前驱跨不过 fork（boot2 canRejected 遇 fork/join/start 是跳过该入边、不再深入）
//
//	⇒ 20010008。夹具 04-fork-join：分支行的 parent 是 fork 之前的 apply。
func Test154RollbackLineageNegatives(t *testing.T) {
	ctx := context.Background()

	// ① 无血缘
	eng, repo := setup()
	def := registerFlow(repo, "02-multi-task.json")
	inst, err := eng.StartProcessInstanceByID(ctx, def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	apply := firstDoing(t, repo, ctx, inst.ID, "apply")
	if apply.ParentTaskID == nil || *apply.ParentTaskID != 0 {
		t.Fatalf("前置条件：发起那条 parent 应为 0，实得 %v", apply.ParentTaskID)
	}
	errRollback(ctx, t, eng, apply.ID, "applicant", "20010007")

	// 老行形状：parent 为 NULL
	nullParent := apply
	nullParent.ParentTaskID = nil
	if err := repo.UpdateTask(ctx, &nullParent); err != nil {
		t.Fatalf("置 NULL 被打断: %v", err)
	}
	errRollback(ctx, t, eng, apply.ID, "applicant", "20010007")

	// ② 守卫：fork 分支行退到 fork 之前的节点
	eng2, repo2 := setup()
	def2 := registerFlow(repo2, "04-fork-join.json")
	inst2, err := eng2.StartProcessInstanceByID(ctx, def2.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start fork: %v", err)
	}
	// 推进到 fork 之后（apply 办结 → 两条并行 DOING）
	apply2 := firstDoing(t, repo2, ctx, inst2.ID, "apply")
	if err := repo2.AddTaskActor(ctx, apply2.ID, []string{"applicant"}); err != nil {
		t.Fatalf("加参与者: %v", err)
	}
	apply2.ActorIDs = append(apply2.ActorIDs, "applicant")
	if _, err := eng2.ExecuteProcessTask(ctx, apply2.ID, "applicant", nil); err != nil {
		t.Fatalf("推进过 fork 被打断: %v", err)
	}
	branch := firstDoing(t, repo2, ctx, inst2.ID, "taskA")
	if branch.ParentTaskID == nil || *branch.ParentTaskID == 0 {
		t.Fatalf("前置条件：分支行的 parent 应已由 P1 写入，实得 %v", branch.ParentTaskID)
	}
	if len(branch.ActorIDs) == 0 {
		t.Fatalf("前置条件：分支行应有参与者，否则测不到守卫（会被权限校验先挡下）")
	}
	errRollback(ctx, t, eng2, branch.ID, branch.ActorIDs[0], "20010008")
}

// firstDoing 取该实例某节点的进行中任务（不存在即 fail，避免"节点名写错→空集合→假绿"）
func firstDoing(t *testing.T, repo *memory.Repository, ctx context.Context, instID int64, node string) model.ProcessTask {
	t.Helper()
	tasks, err := repo.FindDoingTasks(ctx, instID, []string{node})
	if err != nil || len(tasks) == 0 {
		t.Fatalf("节点 %s 应有进行中任务（len=%d err=%v）", node, len(tasks), err)
	}
	return *tasks[0]
}

// errRollback 在 taskID 上办"退回上一步"（targetTaskName 传空），断言报错且 msg 含指定期望码
func errRollback(ctx context.Context, t *testing.T, eng *engine.EngineImpl, taskID int64, operator, wantCode string) {
	t.Helper()
	_, err := eng.ExecuteAndJumpTask(ctx, taskID, operator,
		map[string]interface{}{"submitType": int(model.SubmitTypeRollback)}, "")
	if err == nil {
		t.Fatalf("退回上一步在 %s 上必须报错（期望码 %s），不得静默不建单", wantCode, wantCode)
	}
	if !strings.Contains(err.Error(), wantCode) {
		t.Fatalf("错码应体现在 msg（期望 %s），实得 %v", wantCode, err)
	}
}
