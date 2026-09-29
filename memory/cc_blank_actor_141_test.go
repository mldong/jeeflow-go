// 空抄送人不建 cc 行（issues/141 G10 · Go 栈，**写侧兜底层** · 内存仓一路）。
//
// spec 06-facade.md §2.10 实现要求①「两层都挡」：漏斗层（引擎 HandleCcActors／门面手动
// createCCInstance 解析集合时归一）之外，`CreateCcInstance` 自己也必须丢空值——只修漏斗，
// 绕过引擎/门面**直连仓储**的调用方（集成层、第三方仓储消费者）照样能往 actor_id 里灌空值，
// 那正是 issues/129 那族"空 operator 读全库"的病根。
//
// 本文件与 repository/jdbc/cc_blank_actor_141_sqlite_test.go 是同一组格子的两仓镜像：
// **同一份数据两仓必须同答案**（issues/117 场景 27 那把尺子，G1/G2 两案都立过法）。
// 判据的单点＝[spi.NormalizeCcActors]（内存仓与 JDBC 仓调同一支，不抄两份）。
package memory_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// inst141G10 直连仓储建一条实例，返回 id（写侧兜底档的前置：不走引擎也不走门面）。
func inst141G10(t *testing.T, repo *memory.Repository, businessNo string) int64 {
	t.Helper()
	ctx := context.Background()
	inst := &model.ProcessInstance{
		DefineID: 1, State: 10, BusinessNo: businessNo, Operator: "zhangsan",
		Variables: map[string]interface{}{},
	}
	if err := repo.SaveInstance(ctx, inst); err != nil {
		t.Fatalf("建实例失败: %v", err)
	}
	if inst.ID == 0 {
		t.Fatalf("夹具失效：实例没拿到 id")
	}
	return inst.ID
}

func eq141G10(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustEq141G10(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !eq141G10(got, want) {
		t.Fatalf("%s：实得 %q want %q", label, got, want)
	}
}

// TestIssue141G10MemoryRepoWriteDropsBlankActors 直连 CreateCcInstance：空串/纯空白都不建行，
// 有效的人照旧落一行（本格钉两层里的第二层——漏斗修了、写侧没修时这一档照样灌进空值）。
//
// 改前红：内存仓写侧 `for _, actorID := range actorIDs` 无判据 ⇒ "" 与 "   " 各落一行。
func TestIssue141G10MemoryRepoWriteDropsBlankActors(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	instanceID := inst141G10(t, repo, "CC141-G10-REPO")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "", "   ", "8501"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	mustEq141G10(t, "G10：仓储写侧空串/纯空白都不建行", repo.CcActorsForTest(instanceID), []string{"8501"})
	if n := len(repo.CcRowsForTest(instanceID)); n != 1 {
		t.Fatalf("G10：写侧只落那一行，实得 %d 行", n)
	}
}

// TestIssue141G10MemoryRepoIfAbsentSubsetExcludesBlanks 返回的**实际新建子集**也不得含空值——
// 子集是直接拿去逐人 fire 码 4 的入参，漏一个空值＝为"没发生的事件"发了一次提醒。
//
// 改前红：子集原样回 ["", "8601", "  ", " 8602 "]（既含空值也含未 trim 值）。
func TestIssue141G10MemoryRepoIfAbsentSubsetExcludesBlanks(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	instanceID := inst141G10(t, repo, "CC141-G10-SUBSET")

	created, err := repo.CreateCcInstanceIfAbsent(ctx, instanceID, "zhangsan", "", "8601", "  ", " 8602 ")
	if err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	mustEq141G10(t, "G10：实际新建子集只含有效且 trim 后的人", created, []string{"8601", "8602"})
	mustEq141G10(t, "G10：子集与落库行一致", repo.CcActorsForTest(instanceID), []string{"8601", "8602"})
	if n := len(repo.CcRowsForTest(instanceID)); n != 2 {
		t.Fatalf("G10：应恰好 2 行，实得 %d", n)
	}
}

// TestIssue141G10MemoryRepoStoresTrimmedValueAndDedups 落库值取 trim 后的串，且与 G2 写侧判重
// 同一条尺子：" 8701 " 与 "8701" 是同一个人 ⇒ 仍 1 行、子集为空（没有"创建"发生）。
//
// 改前红：actor_id 原样落 " 8701 "，第二次的 "8701" 判不出重复 ⇒ 落两行。
func TestIssue141G10MemoryRepoStoresTrimmedValueAndDedups(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	instanceID := inst141G10(t, repo, "CC141-G10-TRIM")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", " 8701 "); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	mustEq141G10(t, "G10：入库值应是 trim 后的串", repo.CcActorsForTest(instanceID), []string{"8701"})

	created, err := repo.CreateCcInstanceIfAbsent(ctx, instanceID, "zhangsan", "8701")
	if err != nil {
		t.Fatalf("重复建 cc: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("G10：trim 后同值命中 G2 判重 ⇒ 子集必须为空，实得 %q", created)
	}
	if n := len(repo.CcRowsForTest(instanceID)); n != 1 {
		t.Fatalf("G10：同一人不得落两行（不 trim 就把 G2 判据打穿），实得 %d 行", n)
	}
}

// TestIssue141G10MemoryRepoKeepsZeroLikeIds 反向哨兵（写侧）：判据只吃空值，不吃 "0"
// 这类"看起来像空"的正常 id，也不吃 "null"/"false" 字面串。
func TestIssue141G10MemoryRepoKeepsZeroLikeIds(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	instanceID := inst141G10(t, repo, "CC141-G10-SENTINEL")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "0", "null", "user-1"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	mustEq141G10(t, "G10 只丢空串/纯空白：正常 id 不得被吃掉",
		repo.CcActorsForTest(instanceID), []string{"0", "null", "user-1"})
}
