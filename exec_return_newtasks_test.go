package jeeflow_test

// audit8 R92（spec04 §完成任务 :37/:77/:98/:111）：执行族公开方法返回**本次动作产出的任务行**。
// 基准＝java `JeeflowEngineImpl.executeProcessTask` 返回 `exec.getProcessTaskList()`；
// `exec == null` 时 java 返回 `Collections.emptyList()` ⇒ 空档必须是**非 nil 空切片**（对外 []，不是 null）。
//
// 两条判据各打一件事，避免"改签名但没人证明语义"：
//   正向＝办理 task1 后，返回值逐行等于动作后新落的待办行（同 id、同实例、且已在仓储里）；
//   终止＝办理最后一环使实例完成时，返回空数组而非 nil，且实例状态确为完成。

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/model"
)

func TestExecFamilyReturnsNewTasksI178(t *testing.T) {
	ctx := context.Background()

	t.Run("正向：办理产生的下一批任务必须逐行回传", func(t *testing.T) {
		eng, repo := setup()
		def := registerFlow(repo, "02-multi-task.json")
		inst := startAndExecute(eng, repo, def.ID, "alice", nil)

		doing, err := repo.FindDoingTasks(ctx, inst.ID, nil)
		if err != nil || len(doing) != 1 {
			t.Fatalf("前置：apply 后应有一条待办，实得 %d 行 err=%v", len(doing), err)
		}
		target := doing[0]
		// 让发起人本人可办理该节点（与 engine_test 既有夹具同法：补参与者再办理）
		_ = repo.AddTaskActor(ctx, target.ID, []string{"alice"})
		target.ActorIDs = append(target.ActorIDs, "alice")
		_ = repo.UpdateTask(ctx, target)

		nt, err := eng.ExecuteProcessTask(ctx, target.ID, "alice", map[string]interface{}{"comment": "ok"})
		if err != nil {
			t.Fatalf("ExecuteProcessTask 失败: %v", err)
		}
		if nt == nil {
			t.Fatal("执行族返回值不得为 nil（java emptyList 同档，对外应是 []）")
		}
		after, err := repo.FindDoingTasks(ctx, inst.ID, nil)
		if err != nil {
			t.Fatalf("FindDoingTasks: %v", err)
		}
		if len(nt) == 0 {
			t.Fatalf("本次办理创建了下一级任务，返回却是空列表（新行 %d 条）——摘掉建单口的追加即在此红", len(after))
		}
		if len(nt) != len(after) {
			t.Fatalf("返回行数 %d 应等于动作后的待办行数 %d", len(nt), len(after))
		}
		want := map[int64]bool{}
		for _, tb := range after {
			want[tb.ID] = true
		}
		for i, tb := range nt {
			if tb == nil {
				t.Fatalf("返回第 %d 行为 nil", i)
			}
			if !want[tb.ID] {
				t.Fatalf("返回第 %d 行 id=%d 不在动作后的待办集合里（%v）", i, tb.ID, want)
			}
			if tb.ProcessInstanceID != inst.ID {
				t.Fatalf("返回第 %d 行实例号=%d 应等于 %d", i, tb.ProcessInstanceID, inst.ID)
			}
			back, ferr := repo.FindTaskByID(ctx, tb.ID)
			if ferr != nil || back == nil {
				t.Fatalf("返回行必须已在仓储里（id=%d err=%v）", tb.ID, ferr)
			}
		}
	})

	t.Run("终止档：实例完成时返回空数组而不是 nil", func(t *testing.T) {
		eng, repo := setup()
		def := registerFlow(repo, "01-simple.json")
		inst := startAndExecute(eng, repo, def.ID, "alice", nil)
		if inst.State == model.InstanceStateDone {
			t.Fatal("前置：01-simple 办理审批节点前实例不应已完成")
		}
		doing, _ := repo.FindDoingTasks(ctx, inst.ID, nil)
		if len(doing) != 1 {
			t.Fatalf("前置：应剩 1 条待办，实得 %d", len(doing))
		}
		op := doing[0].ActorID
		if op == "" && len(doing[0].ActorIDs) > 0 {
			op = doing[0].ActorIDs[0]
		}
		if op == "" {
			t.Fatalf("前置：末环待办没有可办理的人（ActorID/ActorIDs 均空）：%+v", doing[0])
		}
		nt, err := eng.ExecuteProcessTask(ctx, doing[0].ID, op, nil)
		if err != nil {
			t.Fatalf("办理末环失败: %v", err)
		}
		if nt == nil {
			t.Fatal("末环返回必须是空数组（非 nil）——java exec.getProcessTaskList() 空档同形")
		}
		if len(nt) != 0 {
			t.Fatalf("实例已到 end，不应还有产出行，实得 %d", len(nt))
		}
		after, _ := repo.FindInstanceByID(ctx, inst.ID)
		if after == nil || after.State != model.InstanceStateDone {
			t.Fatalf("实例应为完成态，实得 %+v", after)
		}
	})
}
