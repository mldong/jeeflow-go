// 抄送分页归属条件必填（issues/141 G1 · Go 栈，内存仓一路）。
//
// 立法逐字依据＝spec 06-facade.md §2.5「抄送分页同一条尺子（owner 2026-09-29 拍）」：
// PageCcInstances 这类"抄送我"取数入口，归属列 cc.actor_id 的条件**必填**——条件缺失或为
// 空值时**返回空页**（total=0、rows 为空集），不得退化成"这条条件不加"而返回全部实例。
//
// 本栈归属有两个载体：显式 actorID 入参（主通道，门面 ccList 恒挂 operator）＋ m_ 条件通道
// 里的 cc.actor_id 条件。第一层（门面把空串归一化成缺省 user1）与本档无关，这里打的是
// **直连仓储**那一档：绕过门面的调用方、或下一版门面漏挂条件时，仓储必须自己顶住。
//
// ⚠️ 本仓改前的病灶：内存仓 PageCcInstances **完全不读** query.Conditions（JDBC 仓把它们落进
// WHERE），于是"cc.actor_id 条件是空值 / 与归属入参不一致"这类查询两仓各说各话——正是
// issues/117 场景 27 立过法的那一类"同一栈两个仓储两个答案"。逐格镜像见
// repository/jdbc/cc_ownership_dedup_141_sqlite_test.go（SQL 仓一路），两仓读数必须相等。
package memory_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// q141 分页参数（可选带 m_ 条件），与 JDBC 仓同名测试用同一份条件形状。
func q141(conds ...spi.Condition) spi.PageQuery {
	out := spi.PageQuery{PageNum: 1, PageSize: 100}
	if len(conds) > 0 {
		out.Conditions = conds
	}
	return out
}

// instanceCcTo141 直连仓储写侧：建一个实例并抄送给给定的人，返回实例 id。
// business_no 给非空值：空值列在 SQL 三值逻辑里是"恒不命中"档，会让下面那格
// "非归属条件空值仍被忽略"的对照在两仓各说各话（既有形状，非本案范围）。
func instanceCcTo141(t *testing.T, repo *memory.Repository, actorID string) int64 {
	t.Helper()
	ctx := context.Background()
	inst := &model.ProcessInstance{
		DefineID:   1,
		State:      10,
		BusinessNo: "CC141-" + actorID,
		Operator:   "zhangsan",
		Variables:  map[string]interface{}{},
	}
	if err := repo.SaveInstance(ctx, inst); err != nil {
		t.Fatalf("建实例失败: %v", err)
	}
	if inst.ID == 0 {
		t.Fatalf("夹具失效：实例没拿到 id")
	}
	if err := repo.CreateCcInstance(ctx, inst.ID, "zhangsan", actorID); err != nil {
		t.Fatalf("建 cc 行失败: %v", err)
	}
	return inst.ID
}

// TestIssue141MemoryCcPageWithOwnershipReturnsOnlyMine 正向对照：带有效归属时照旧只出"我的"那一页。
func TestIssue141MemoryCcPageWithOwnershipReturnsOnlyMine(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	mine := instanceCcTo141(t, repo, "user1")
	theirs := instanceCcTo141(t, repo, "user2")

	rows, total, err := repo.PageCcInstances(ctx, q141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user1"}), "user1")
	if err != nil {
		t.Fatalf("带归属条件查询失败: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("带条件应命中我的那 1 条, got total=%d rows=%d", total, len(rows))
	}
	if rows[0].ID != mine {
		t.Fatalf("命中的应是我的实例 %d，实得 %d", mine, rows[0].ID)
	}
	if rows[0].ID == theirs {
		t.Fatalf("别人的实例不该串进来")
	}
}

// TestIssue141MemoryCcPageWithoutOwnershipIsEmptyPage 缺陷档：归属整条没给 ⇒ **空页**。
// 本栈归属主通道是显式 actorID 入参，三形态空值（空串/全空白/制表符）都算"没给"，
// 绝不折叠成"这条不加"而返回全部实例（改前形状：内存仓把空串读成"不过滤"，demo 里
// {"operator":""} 直接把所有人的 cc 实例都返出去）。
func TestIssue141MemoryCcPageWithoutOwnershipIsEmptyPage(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	instanceCcTo141(t, repo, "user1")
	instanceCcTo141(t, repo, "user2")

	// 夹具守卫：带归属时确有行（否则下面的 0==0 是自等假绿）
	if rows, total, _ := repo.PageCcInstances(ctx, q141(), "user1"); len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效：PageCcInstances(user1) 应有 1 行, got rows=%d total=%d", len(rows), total)
	}

	for _, blank := range []struct{ name, val string }{{"空串", ""}, {"全空白", "   "}, {"制表符", "\t"}} {
		rows, total, err := repo.PageCcInstances(ctx, q141(), blank.val)
		if err != nil {
			t.Fatalf("PageCcInstances(%s) 不应报错: %v", blank.name, err)
		}
		if total != 0 || len(rows) != 0 {
			t.Fatalf("缺归属条件必须返回空页，实得 %s ⇒ total=%d rows=%d（退化成「这条不加」返回全部实例）",
				blank.name, total, len(rows))
		}
		if rows == nil {
			t.Fatalf("空页的 rows 必须是空集而不是 nil（契约 rows=[]）")
		}
	}
}

// TestIssue141MemoryCcPageBlankOwnershipConditionIsEmptyPage 空值三形＋空 IN 与"条件整条没给"
// 同档 ⇒ 空页。**本档是内存仓改前的红**：旧实现完全不读 query.Conditions，
// 归属列上的空值条件被整体忽略 ⇒ 照样返回 user1 的行，而 JDBC 仓给 `AND 1=0` ⇒ 两仓两个答案。
func TestIssue141MemoryCcPageBlankOwnershipConditionIsEmptyPage(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	instanceCcTo141(t, repo, "user1")

	// 夹具守卫
	if rows, total, _ := repo.PageCcInstances(ctx, q141(), "user1"); len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效：PageCcInstances(user1) 应有 1 行, got rows=%d total=%d", len(rows), total)
	}

	for _, v := range []interface{}{"", "   ", nil} {
		rows, total, err := repo.PageCcInstances(ctx, q141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: v}), "user1")
		if err != nil {
			t.Fatalf("cc.actor_id EQ %v 查询失败: %v", v, err)
		}
		if len(rows) != 0 || total != 0 {
			t.Fatalf("空串/全空白/nil 归属条件 ⇒ 空页，实得 EQ %v ⇒ rows=%d total=%d", v, len(rows), total)
		}
	}
	// 空集合＝"没有人"：EQ/IN 两个操作符上同一判据（JDBC 仓旧形状是把空 IN 整个丢掉不加条件）
	for _, op := range []string{"EQ", "IN"} {
		for _, empty := range []interface{}{[]interface{}{}, []string{}} {
			rows, total, err := repo.PageCcInstances(ctx, q141(spi.Condition{Column: "cc.actor_id", Operator: op, Value: empty}), "user1")
			if err != nil {
				t.Fatalf("cc.actor_id %s 空集合查询失败: %v", op, err)
			}
			if len(rows) != 0 || total != 0 {
				t.Fatalf("空集合归属条件（%s %T）⇒ 空页，实得 rows=%d total=%d", op, empty, len(rows), total)
			}
		}
	}
}

// TestIssue141MemoryCcPageOwnershipConditionMustMatchActor 有效归属条件与归属入参不一致时
// 取交集（AND 语义）⇒ 0 行。**本档同样是内存仓改前的红**：旧实现忽略条件 ⇒ 返回 user1 的行，
// 而 JDBC 仓 `WHERE cc.actor_id = ? AND cc.actor_id = ?` ⇒ 0 行。
func TestIssue141MemoryCcPageOwnershipConditionMustMatchActor(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	instanceCcTo141(t, repo, "user1")

	rows, total, err := repo.PageCcInstances(ctx,
		q141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user2"}), "user1")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 0 || total != 0 {
		t.Fatalf("条件 cc.actor_id=user2 与归属入参 user1 不一致应 0 行（两仓同判据），实得 rows=%d total=%d",
			len(rows), total)
	}
	// 一致时照常命中（本案没把归属列条件整个禁掉）
	if rows, total, _ := repo.PageCcInstances(ctx,
		q141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user1"}), "user1"); len(rows) != 1 || total != 1 {
		t.Fatalf("条件与归属入参一致应 1 行, got rows=%d total=%d", len(rows), total)
	}
}

// TestIssue141MemoryCcPageBlankNonOwnershipConditionStillIgnored 改动面哨兵：只收归属谓词，
// 非归属列的空值放行必须保持不变（m_LIKE_* 传空串仍按"没填"忽略 ⇒ 可选过滤照旧生效）。
func TestIssue141MemoryCcPageBlankNonOwnershipConditionStillIgnored(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	mine := instanceCcTo141(t, repo, "user1")

	rows, total, err := repo.PageCcInstances(ctx, q141(
		spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user1"},
		spi.Condition{Column: "t.business_no", Operator: "LIKE", Value: ""},
	), "user1")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 || total != 1 {
		t.Fatalf("空值非归属条件应被忽略、归属条件照常生效 ⇒ 1 行, got rows=%d total=%d ⇒ 通用放行被误改了",
			len(rows), total)
	}
	if rows[0].ID != mine {
		t.Fatalf("命中实例应为 %d, 实得 %d", mine, rows[0].ID)
	}
}
