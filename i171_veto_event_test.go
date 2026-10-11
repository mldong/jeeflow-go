// issues/171 · 并行会签 ONE_VOTE_VETO 下 submitType=20 的**否决者本人那张任务**事件腿——
// 到底 fire 了几条码、TaskID 是谁（引擎级笔头判据，对齐契约门禁 L2-12 第三档同一断言：
// 否决者 taskId 含 6 不含 5；基准＝spec 11 §11.4-6／java REJECT_SUBMIT_TYPES 含 20）。
// 本用例只钉事实（先判据后修法），不预设修在哪一层。
package jeeflow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
)

func TestI171VetoTaskFiresRejectNotComplete(t *testing.T) {
	eng, repo, inst := i163Start(t, "i171-veto-evt", "ONE_VOTE_VETO")
	ctx := context.Background()
	doing, _ := repo.FindDoingTasks(ctx, inst.ID, nil)
	if len(doing) == 0 {
		t.Fatal("并行会签应有成员待办")
	}
	vetoTask := doing[0]
	repo.AddTaskActor(ctx, vetoTask.ID, []string{"userA"})
	vetoTask.ActorIDs = append(vetoTask.ActorIDs, "userA")

	var evts []engine.ProcessEvent
	recordEvents(eng, &evts)
	if _, err := eng.ExecuteProcessTask(ctx, vetoTask.ID, "userA",
		map[string]interface{}{engine.KeySubmitType: 20}); err != nil {
		t.Fatalf("veto execute: %v", err)
	}

	var mine []int
	has5, has6 := false, false
	for _, e := range evts {
		if e.TaskID != vetoTask.ID {
			continue
		}
		mine = append(mine, int(e.Type))
		switch e.Type {
		case engine.EventTaskComplete:
			has5 = true
		case engine.EventTaskReject:
			has6 = true
		}
	}
	t.Logf("判据读数：veto taskId=%d codes=%v（本次 execute 全事件=%s）", vetoTask.ID, mine, eventNames(evts))
	if has5 {
		t.Fatalf("否决者任务 %d 发了 TASK_COMPLETE(5)，违反 5/6 互斥（§11.3 码 6 括注）：codes=%v", vetoTask.ID, mine)
	}
	if !has6 {
		t.Fatalf("否决者任务 %d 没发 TASK_REJECT(6)（§11.4-6／L2-12 第三档同判据）：codes=%v 全事件=%s",
			vetoTask.ID, mine, eventNames(evts))
	}
	if fmt.Sprint(mine) != "[6]" {
		t.Fatalf("否决者任务 %d 事件码应为恰一条 6（TaskID 必须指向它本人），实得 %v", vetoTask.ID, mine)
	}
}
