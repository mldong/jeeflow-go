// 归属值写侧归一（issues/142 B 批 · Go 栈，**写侧兜底层** · 内存仓一路）。
//
// spec 06-facade.md §2.11 把 §2.10 的四点实现要求逐字搬到任务侧，写点表最后一行就是
// 「两仓 addTaskActor …仓储写侧自己再挡一次——只修门面腿则绕过门面直连仓储的调用方照样灌空值」。
// 普查实读本栈改前是"两仓只判重"（memory/repository.go:277-292、repository/jdbc/jdbc.go:615-639）
// ⇒ ""/空白全放行。
//
// 本文件与 repository/jdbc/actor_norm_142_sqlite_test.go（＋同目录真库那格）是同一组格子的
// 两仓镜像：**同一份数据两仓必须同答案**（issues/117 场景 27 那把尺子）。
// 判据的单点＝[spi.NormalizeActors]（内存仓与 JDBC 仓调同一支，不抄两份）。
package memory_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// a142Task 造一条 DOING 任务行；actors 用 SaveTask 直投（**绕过 AddTaskActor**），
// 专门用来摆"修复前落下的历史脏行"（未 trim 的原值），写侧兜底档才照得出来。
func a142Task(t *testing.T, repo *memory.Repository, id int64, actors []string) int64 {
	t.Helper()
	task := &model.ProcessTask{
		ID: id, ProcessInstanceID: id * 10, TaskName: "task1", DisplayName: "审批",
		TaskState: model.TaskStateDoing, ActorIDs: actors, Variables: map[string]interface{}{},
	}
	if err := repo.SaveTask(context.Background(), task); err != nil {
		t.Fatalf("建任务行: %v", err)
	}
	return task.ID
}

func actorsOf142(t *testing.T, repo *memory.Repository, taskID int64) []string {
	t.Helper()
	got, err := repo.FindTaskActors(context.Background(), taskID)
	if err != nil {
		t.Fatalf("读参与者: %v", err)
	}
	return got
}

func mustActors142(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
		t.Fatalf("%s：实得 %q want %q", label, got, want)
	}
}

// TestIssue142MemoryAddTaskActorDropsBlankKeepsValid 空串/纯空白不入库，有效的人照旧一行。
//
// 改前红：内存仓写侧 `for _, a := range actors` 只判重不判空 ⇒ 实得 ["", "   ", "8801"] 三行。
func TestIssue142MemoryAddTaskActorDropsBlankKeepsValid(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214201, nil)

	if err := repo.AddTaskActor(context.Background(), taskID, []string{"", "   ", "8801"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142(t, "§2.11：仓储写侧空串/纯空白都不落行", actorsOf142(t, repo, taskID), []string{"8801"})
}

// TestIssue142MemoryAddTaskActorStoresTrimmedValue 落库值取 trim 后的串（硬要求②）。
//
// 改前红：actor_id 原样落 " 8901 "（未 trim）。
func TestIssue142MemoryAddTaskActorStoresTrimmedValue(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214202, nil)

	if err := repo.AddTaskActor(context.Background(), taskID, []string{" 8901 "}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142(t, "§2.11：入库值是 trim 后的串", actorsOf142(t, repo, taskID), []string{"8901"})
}

// TestIssue142MemoryAddTaskActorCollapsesWithinOneCall 同一次调用内的重复折叠成一行
// （" 9001 " 与 "9001" 是同一个人；不 trim 就是两个人，判重被架空）。
//
// 改前红：["9001"," 9001 "] 判不出重复 ⇒ 实得两行。
func TestIssue142MemoryAddTaskActorCollapsesWithinOneCall(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214203, nil)

	if err := repo.AddTaskActor(context.Background(), taskID,
		[]string{"9001", " 9001 ", "9001"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142(t, "§2.11：同次调用折叠成一行", actorsOf142(t, repo, taskID), []string{"9001"})
}

// TestIssue142MemoryAddTaskActorDedupHitsLegacyUntrimmedRow 判重比较也取 trim 后的值：
// 行上是修复前落下的 " 9101 "（脏行，写侧不清理，同 §2.10/G2 的"历史重复行不清理"立场），
// 再给 "9101" 必须判成同一个人 ⇒ 不落第二行。
//
// 改前红：seen 按原串比 ⇒ "9101" 判不出与 " 9101 " 重复 ⇒ 实得两行。
func TestIssue142MemoryAddTaskActorDedupHitsLegacyUntrimmedRow(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214204, []string{" 9101 "})

	if err := repo.AddTaskActor(context.Background(), taskID, []string{"9101"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	got := actorsOf142(t, repo, taskID)
	if len(got) != 1 {
		t.Fatalf("§2.11：trim 后同值命中判重 ⇒ 不得落第二行，实得 %q", got)
	}
}

// TestIssue142MemoryAddTaskActorKeepsSentinelIds 反向哨兵（写侧）："0"/"00"/"null"/"a"
// 都是正常归属值且互不相同；只有纯空白那一个被丢。
//
// 改前红：纯空白 " " 也照落一行。
func TestIssue142MemoryAddTaskActorKeepsSentinelIds(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214205, nil)

	if err := repo.AddTaskActor(context.Background(), taskID,
		[]string{"0", "00", " ", "null", "a"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142(t, "§2.11 只丢空串/纯空白：哨兵 id 不得被吃掉或折叠",
		actorsOf142(t, repo, taskID), []string{"0", "00", "null", "a"})
}

// TestIssue142MemoryAddTaskActorAllBlankIsNoop 全空白 ⇒ 一行都不落（不是落一条 actor_id 为空串 的行）。
// 与 issues/142 A 批"零参与者行不插 actor 行"同一条立场。
//
// 改前红：["","  "] 落两行空归属值——正是 issues/129 那族"空 actor_id 读全库"的进水口。
func TestIssue142MemoryAddTaskActorAllBlankIsNoop(t *testing.T) {
	repo := memory.New()
	taskID := a142Task(t, repo, 9214206, nil)

	if err := repo.AddTaskActor(context.Background(), taskID, []string{"", "  "}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	if got := actorsOf142(t, repo, taskID); len(got) != 0 {
		t.Fatalf("§2.11：全空白必须零行，实得 %q", got)
	}
}

// TestIssue142MemoryUpdateCcStatusComparesTrimmedValue updateCCStatus 的 operator
// 「入参归一后再比」（§2.11 写点表第 4 行）：入参带两端空格仍打中 trim 后落库的那一行。
//
// 改前红：`row.ActorID == actorID` 拿未 trim 原值比 ⇒ 一条都不更新（门面报 code=0 的静默 no-op）。
func TestIssue142MemoryUpdateCcStatusComparesTrimmedValue(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	inst := &model.ProcessInstance{DefineID: 1, State: 10, BusinessNo: "A142-CC-TRIM",
		Operator: "zhangsan", Variables: map[string]interface{}{}}
	if err := repo.SaveInstance(ctx, inst); err != nil {
		t.Fatalf("建实例: %v", err)
	}
	if err := repo.CreateCcInstance(ctx, inst.ID, "zhangsan", "4201"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if err := repo.UpdateCcStatus(ctx, inst.ID, " 4201 "); err != nil {
		t.Fatalf("UpdateCcStatus: %v", err)
	}
	rows := repo.CcRowsForTest(inst.ID)
	if len(rows) != 1 {
		t.Fatalf("夹具：cc 应一行，实得 %d", len(rows))
	}
	if rows[0].State != 1 {
		t.Fatalf("§2.11：入参归一后再比 ⇒ 该行应被置已读，实得 state=%d", rows[0].State)
	}
}

// TestIssue142MemoryUpdateCcStatusBlankActorTouchesModuleRows 归属为空 ⇒ 一条不动
// （严禁退化成"这条条件不加"把 state=1 打到历史 actor_id 为空串 的脏行上）。
// 本格钉的是"空值不放宽"，改前后都是绿的（回归钉，不是红样）。
func TestIssue142MemoryUpdateCcStatusBlankActorTouchesModuleRows(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	inst := &model.ProcessInstance{DefineID: 1, State: 10, BusinessNo: "A142-CC-BLANK",
		Operator: "zhangsan", Variables: map[string]interface{}{}}
	if err := repo.SaveInstance(ctx, inst); err != nil {
		t.Fatalf("建实例: %v", err)
	}
	if err := repo.CreateCcInstance(ctx, inst.ID, "zhangsan", "4301"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if err := repo.UpdateCcStatus(ctx, inst.ID, "   "); err != nil {
		t.Fatalf("UpdateCcStatus: %v", err)
	}
	for _, row := range repo.CcRowsForTest(inst.ID) {
		if row.State != 0 {
			t.Fatalf("§2.11：空归属不得放宽成\"这条条件不加\"，实得 state=%d", row.State)
		}
	}
}
