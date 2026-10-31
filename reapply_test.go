package jeeflow_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// 「退回修改、重提直返」验收（Go 栈）：基本直返、条件不重算、未汇合并行等待、
// 多分支分别退回、重复退回、并发重提、回归与非法配置。

func rDoing(t *testing.T, repo *memory.Repository, iid int64, name string) *model.ProcessTask {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), iid, nil)
	for _, x := range doing {
		if x.TaskName == name {
			return x
		}
	}
	return nil
}

func rAll(t *testing.T, repo *memory.Repository, iid int64) []*model.ProcessTask {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), iid, nil)
	return doing
}

func rAct(t *testing.T, repo *memory.Repository, task *model.ProcessTask, op string) {
	t.Helper()
	_ = repo.AddTaskActor(context.Background(), task.ID, []string{op})
	task.ActorIDs = append(task.ActorIDs, op)
}

func rExec(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, task *model.ProcessTask, op string, submit int) {
	t.Helper()
	rAct(t, repo, task, op)
	if _, err := eng.ExecuteProcessTask(context.Background(), task.ID, op,
		map[string]interface{}{engine.KeySubmitType: submit}); err != nil {
		t.Fatalf("execute %s submit=%d: %v", task.TaskName, submit, err)
	}
}

func rReturn(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, task *model.ProcessTask, op string) {
	t.Helper()
	rAct(t, repo, task, op)
	if _, err := eng.ExecuteAndJumpToFirstTaskNode(context.Background(), task.ID, op,
		map[string]interface{}{engine.KeySubmitType: 6}); err != nil {
		t.Fatalf("return to applicant from %s: %v", task.TaskName, err)
	}
}

func rInstance(t *testing.T, repo *memory.Repository, iid int64) *model.ProcessInstance {
	t.Helper()
	inst, err := repo.FindInstanceByID(context.Background(), iid)
	if err != nil || inst == nil {
		t.Fatalf("instance %d: %v", iid, err)
	}
	return inst
}

// TestReapply01Simple 基本直返：只生成新的来源待办，不生成来源之后的节点。
func TestReapply01Simple(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "14-return-to-source.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	a := rDoing(t, repo, iid, "taskA")
	if a == nil {
		t.Fatal("taskA missing")
	}
	rReturn(t, eng, repo, a, "userA")
	if rDoing(t, repo, iid, "apply") == nil {
		t.Fatal("applicant task not created")
	}
	app := rDoing(t, repo, iid, "apply")
	if _, ok := app.Variables[engine.VarReapplyCtx]; !ok {
		t.Fatalf("applicant task missing reapply context: %v", app.Variables)
	}
	if rDoing(t, repo, iid, "taskB") != nil {
		t.Fatal("return must not create downstream node")
	}
	// 重提：直接回 taskA
	rExec(t, eng, repo, app, "applicant", 5)
	newA := rDoing(t, repo, iid, "taskA")
	if newA == nil {
		t.Fatal("reapply did not create source task")
	}
	if newA.ParentTaskID == nil || *newA.ParentTaskID != app.ID {
		t.Fatalf("new source parent should be applicant task, got %v", newA.ParentTaskID)
	}
	if rDoing(t, repo, iid, "taskB") != nil {
		t.Fatal("reapply must not create downstream node")
	}
	// 完成新 A → taskB；完成 B → 结束
	rExec(t, eng, repo, newA, "userA", 1)
	if rDoing(t, repo, iid, "taskB") == nil {
		t.Fatal("taskB should appear after source approved")
	}
	rExec(t, eng, repo, rDoing(t, repo, iid, "taskB"), "userB", 1)
	if got := rInstance(t, repo, iid).State; got != model.InstanceStateDone {
		t.Fatalf("expected done, got %d", got)
	}
}

// TestReapply02RejectOtherSubmitTypes 直返申请待办不接受 submitType=1。
func TestReapply02RejectOtherSubmitTypes(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "14-return-to-source.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	app := rDoing(t, repo, iid, "apply")
	rAct(t, repo, app, "applicant")
	if _, err := eng.ExecuteProcessTask(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 1}); err == nil {
		t.Fatal("agree on direct-return applicant task must fail")
	}
	if rDoing(t, repo, iid, "taskA") != nil {
		t.Fatal("rejected path must not create source task")
	}
}

// TestReapply03ForkWaits 未汇合并行：申请人在改时汇合不得放行；重提本身不放行；新来源办完才放行一次。
func TestReapply03ForkWaits(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "15-return-to-source-fork.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	a := rDoing(t, repo, iid, "taskA")
	b := rDoing(t, repo, iid, "taskB")
	if a == nil || b == nil {
		t.Fatalf("fork should create two tasks, got %v", rAll(t, repo, iid))
	}
	rReturn(t, eng, repo, a, "userA")
	if rDoing(t, repo, iid, "taskB") == nil {
		t.Fatal("other branch must remain actionable")
	}
	rExec(t, eng, repo, b, "userB", 1)
	if rDoing(t, repo, iid, "taskC") != nil {
		t.Fatal("join must wait while applicant is editing")
	}
	app := rDoing(t, repo, iid, "apply")
	rExec(t, eng, repo, app, "applicant", 5)
	newA := rDoing(t, repo, iid, "taskA")
	if newA == nil {
		t.Fatal("reapply did not create source task")
	}
	if rDoing(t, repo, iid, "taskB") != nil {
		t.Fatal("branch B must not be recreated")
	}
	if rDoing(t, repo, iid, "taskC") != nil {
		t.Fatal("reapply alone must not release join")
	}
	rExec(t, eng, repo, newA, "userA", 1)
	c := rDoing(t, repo, iid, "taskC")
	if c == nil {
		t.Fatal("join should release after source completes")
	}
	if len(rAll(t, repo, iid)) != 1 {
		t.Fatalf("join should release once, doing=%v", rAll(t, repo, iid))
	}
}

// TestReapply04TwoBranches 两条分支分别退回：恢复一条不影响另一条。
func TestReapply04TwoBranches(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "15-return-to-source-fork.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskB"), "userB")
	apps, _ := repo.FindDoingTasks(context.Background(), iid, []string{"apply"})
	if len(apps) != 2 {
		t.Fatalf("two branches should create two applicant tasks, got %d", len(apps))
	}
	var appA, appB *model.ProcessTask
	for _, x := range apps {
		rc, _ := x.Variables[engine.VarReapplyCtx].(map[string]interface{})
		if rc != nil && rc["sourceNode"] == "taskA" {
			appA = x
		}
		if rc != nil && rc["sourceNode"] == "taskB" {
			appB = x
		}
	}
	if appA == nil || appB == nil {
		t.Fatal("applicant tasks should bind their own source")
	}
	rExec(t, eng, repo, appA, "applicant", 5)
	rest, _ := repo.FindDoingTasks(context.Background(), iid, []string{"apply"})
	if len(rest) != 1 {
		t.Fatalf("restoring A must leave B's applicant task, got %d", len(rest))
	}
	rExec(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA", 1)
	if rDoing(t, repo, iid, "taskC") != nil {
		t.Fatal("B still editing, join must wait")
	}
	rExec(t, eng, repo, appB, "applicant", 5)
	rExec(t, eng, repo, rDoing(t, repo, iid, "taskB"), "userB", 1)
	if rDoing(t, repo, iid, "taskC") == nil {
		t.Fatal("join should release after both branches finish")
	}
}

// TestReapply05ConditionNotReevaluated 改条件字段后仍回原来源节点。
func TestReapply05ConditionNotReevaluated(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "16-return-to-source-decision.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", map[string]interface{}{"amount": 5000.0})
	iid := inst.ID
	high := rDoing(t, repo, iid, "taskHigh")
	if high == nil {
		t.Fatalf("amount>1000 should route to taskHigh, doing=%v", rAll(t, repo, iid))
	}
	rReturn(t, eng, repo, high, "userH")
	app := rDoing(t, repo, iid, "apply")
	rAct(t, repo, app, "applicant")
	if _, err := eng.ExecuteProcessTask(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 5, "amount": 100.0}); err != nil {
		t.Fatalf("reapply: %v", err)
	}
	if rDoing(t, repo, iid, "taskHigh") == nil {
		t.Fatal("reapply must return to the original source node")
	}
	if rDoing(t, repo, iid, "taskLow") != nil {
		t.Fatal("condition before source must not be re-evaluated")
	}
}

// TestReapply06RepeatedReturn 第二轮退回绑定本轮来源。
func TestReapply06RepeatedReturn(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "14-return-to-source.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	rExec(t, eng, repo, rDoing(t, repo, iid, "apply"), "applicant", 5)
	round2 := rDoing(t, repo, iid, "taskA")
	rReturn(t, eng, repo, round2, "userA")
	app2 := rDoing(t, repo, iid, "apply")
	rc, _ := app2.Variables[engine.VarReapplyCtx].(map[string]interface{})
	if rc == nil || rc["sourceTaskId"] == nil {
		t.Fatalf("second round context missing: %v", app2.Variables)
	}
	want := fmt.Sprintf("%d", round2.ID)
	if got := fmt.Sprintf("%v", rc["sourceTaskId"]); got != want {
		t.Fatalf("second round must bind its own source, got %v want %s", got, want)
	}
}

// TestReapply07ConcurrentReapply 同一申请任务并发重提至多一个成功。
func TestReapply07ConcurrentReapply(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "14-return-to-source.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	app := rDoing(t, repo, iid, "apply")
	rAct(t, repo, app, "applicant")

	var wg sync.WaitGroup
	var mu sync.Mutex
	okCount, failCount := 0, 0
	start := make(chan struct{})
	run := func() {
		defer wg.Done()
		<-start
		_, err := eng.ExecuteProcessTask(context.Background(), app.ID, "applicant",
			map[string]interface{}{engine.KeySubmitType: 5})
		mu.Lock()
		if err != nil {
			failCount++
		} else {
			okCount++
		}
		mu.Unlock()
	}
	wg.Add(2)
	go run()
	go run()
	close(start)
	wg.Wait()
	if okCount != 1 || failCount != 1 {
		t.Fatalf("concurrent reapply should have exactly one success, ok=%d fail=%d", okCount, failCount)
	}
	a2, _ := repo.FindDoingTasks(context.Background(), iid, []string{"taskA"})
	if len(a2) != 1 {
		t.Fatalf("exactly one new source task expected, got %d", len(a2))
	}
}

// TestReapply08LegacyUnchanged 未配置 reapplyMode 的节点仍走原路。
func TestReapply08LegacyUnchanged(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "04-fork-join.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	app := rDoing(t, repo, iid, "apply")
	if _, ok := app.Variables[engine.VarReapplyCtx]; ok {
		t.Fatal("legacy node must not create reapply context")
	}
	rExec(t, eng, repo, app, "applicant", 5)
	// 原 taskB 仍在办，普通重提重新分叉再建 A/B ⇒ 3 条进行中
	if got := len(rAll(t, repo, iid)); got != 3 {
		t.Fatalf("legacy reapply should re-fork, doing=%d", got)
	}
}

// TestReapply09MarkedApplicantRejectsJump 带直返标记的申请待办走真实跳转入口必须失败且无写入。
func TestReapply09MarkedApplicantRejectsJump(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "14-return-to-source.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	rReturn(t, eng, repo, rDoing(t, repo, iid, "taskA"), "userA")
	app := rDoing(t, repo, iid, "apply")
	if app == nil {
		t.Fatal("applicant task missing")
	}
	rAct(t, repo, app, "applicant")
	before, _ := repo.FindHistoryTasks(context.Background(), iid)
	beforeDoing, _ := repo.FindDoingTasks(context.Background(), iid, nil)

	expectRejected := func(label string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s should be rejected", label)
		}
		if !strings.Contains(err.Error(), "只接受重新提交") {
			t.Fatalf("%s unexpected error: %v", label, err)
		}
	}
	_, err := eng.ExecuteAndJumpTask(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 4}, "taskB")
	expectRejected("named jump", err)
	_, err = eng.ExecuteAndJumpTask(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 3}, "")
	expectRejected("rollback", err)
	_, err = eng.ExecuteAndJumpToEnd(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 2})
	expectRejected("reject", err)
	_, err = eng.ExecuteAndJumpToFirstTaskNode(context.Background(), app.ID, "applicant",
		map[string]interface{}{engine.KeySubmitType: 6})
	expectRejected("return again", err)

	after, _ := repo.FindHistoryTasks(context.Background(), iid)
	afterDoing, _ := repo.FindDoingTasks(context.Background(), iid, nil)
	if len(after) != len(before) || len(afterDoing) != len(beforeDoing) {
		t.Fatalf("rejected actions must not write rows: hist %d->%d doing %d->%d",
			len(before), len(after), len(beforeDoing), len(afterDoing))
	}
	again, _ := repo.FindTaskByID(context.Background(), app.ID)
	if again == nil || again.TaskState != model.TaskStateDoing || again.Variables[engine.VarReapplyCtx] == nil {
		t.Fatal("marked applicant task must stay DOING with context")
	}
	rExec(t, eng, repo, again, "applicant", 5)
	if rDoing(t, repo, iid, "taskA") == nil {
		t.Fatal("normal reapply should still work")
	}
}

func rHistCount(t *testing.T, repo *memory.Repository, iid int64, name string) int {
	t.Helper()
	hist, _ := repo.FindHistoryTasks(context.Background(), iid)
	n := 0
	for _, x := range hist {
		if x.TaskName == name {
			n++
		}
	}
	return n
}

// TestReapply10NestedForkKeepsOwnership 嵌套并行：内层来源分支退回修改时内层汇合等待；
// 重提只重建内层来源；外层汇合等待外层另一支；两级汇合各只放行一次。
func TestReapply10NestedForkKeepsOwnership(t *testing.T) {
	eng, repo := setup()
	def := registerFlow(repo, "18-return-to-source-nested.json")
	inst := startAndExecute(eng, repo, def.ID, "applicant", nil)
	iid := inst.ID
	a := rDoing(t, repo, iid, "taskA")
	b := rDoing(t, repo, iid, "taskB")
	if a == nil || b == nil {
		t.Fatalf("outer fork should create two tasks, got %v", rAll(t, repo, iid))
	}
	// 外层分支 A 进入内层并行组
	rExec(t, eng, repo, a, "userA", 1)
	x := rDoing(t, repo, iid, "taskX")
	y := rDoing(t, repo, iid, "taskY")
	if x == nil || y == nil {
		t.Fatalf("inner fork should create two tasks, got %v", rAll(t, repo, iid))
	}
	// 内层分支 X 退回发起人
	rReturn(t, eng, repo, x, "userX")
	if rDoing(t, repo, iid, "apply") == nil {
		t.Fatal("applicant task not created")
	}
	// 内层另一支 Y 办完：内层汇合必须等 X 分支
	rExec(t, eng, repo, y, "userY", 1)
	if rDoing(t, repo, iid, "taskA2") != nil {
		t.Fatal("inner join must wait while applicant edits inner source branch")
	}
	// 重提只重建 X，不重建 Y，也不重放内层分叉
	app := rDoing(t, repo, iid, "apply")
	rExec(t, eng, repo, app, "applicant", 5)
	newX := rDoing(t, repo, iid, "taskX")
	if newX == nil {
		t.Fatal("reapply did not recreate inner source task")
	}
	if rDoing(t, repo, iid, "taskY") != nil {
		t.Fatal("inner branch Y must not be recreated")
	}
	if rDoing(t, repo, iid, "taskA2") != nil {
		t.Fatal("reapply alone must not release inner join")
	}
	rExec(t, eng, repo, newX, "userX", 1)
	if got := rHistCount(t, repo, iid, "taskA2"); got != 1 {
		t.Fatalf("inner join should release once, taskA2 rows=%d", got)
	}
	// 外层：A 分支已到外层汇合，但 B 分支仍在办
	rExec(t, eng, repo, rDoing(t, repo, iid, "taskA2"), "userA", 1)
	if rDoing(t, repo, iid, "taskC") != nil {
		t.Fatal("outer join must wait for outer branch B")
	}
	rExec(t, eng, repo, b, "userB", 1)
	if got := rHistCount(t, repo, iid, "taskC"); got != 1 {
		t.Fatalf("outer join should release once, taskC rows=%d", got)
	}
}
