// 参与者**删除腿**归属值判据（issues/137 §3-6 · Go 栈 · **SQL 仓一路**，sqlite :memory:，不连 160 真库）。
//
// 立法逐字依据＝spec 06-facade.md §processTask/removeTaskActor 语义 6 ＋ §2.11 写点表末行
// （owner 2026-10-02 拍「两形并集」）；判据基准＝ jeeflow-java `JdbcTaskActorDeleteFormsTest`
// （java 基准腿 commit c48a6ba，9 格逐格对照）。与 memory/task_actor_delete_forms_137_test.go
// 跑同一条判据：**同一份数据，SQL 仓与内存仓必须给同一个答案**（issues/117 场景 27 那把尺子）。
//
// 断言全部直接查 `wf_process_task_actor.actor_id` 的**真实列值**（不看内存对象、不看返回值）——
// issues/142 §8.2 的教训：内存绿不等于落库绿。
//
// 本文件存在的理由是：语义 6 那两种假成功**只有在真库上才照得出来**。库里的历史脏行是
// "修复前落下的未 trim 原值"（' 9101 '），写侧归一后的规范行是 '9101'；排序规则还会插手——
// MySQL 8.0 默认的 NO PAD 系（utf8mb4_0900_*）连**尾部**空格都算进比较，5.7 默认的 PAD SPACE 系
// 只忽略尾部、**前导空格永远算**。所以脏行夹具一律用**前导空格**（" 9101 "），
// 在任何排序规则下都与规范行不等，判据不会因跑在哪台库上而漂。
//
// 本栈改前现状（1.8.36）：`repository/jdbc/jdbc.go` 的删除腿是**裸传**——`actors` 逐个绑进
// `IN (?)`，既不产出 trim 形（第三方传 " 8601 " 删不掉规范行，issues/142 §9.2 那一路），
// 也不丢空值（空串入参会把历史 actor_id 空串 脏行删掉）。改后走 [spi.ActorDeleteForms] 一枚单点，
// `IN` 占位符数量按展开后的并集长度算。
//
// 跑法：
//
//	go test ./repository/jdbc/ -run 'TestIssue137Jdbc'
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// adf137RowSeq 行 id 序列（关系表主键非自增，直插要显式给）。
var adf137RowSeq = int64(7300137000)

func adf137NextRowID() int64 {
	adf137RowSeq++
	return adf137RowSeq
}

// adf137SeedTask 摆一条 DOING 任务行（删除腿的前置：参与者行要挂在真任务上）。
func adf137SeedTask(t *testing.T, db *sql.DB, taskID int64) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,variable)
		 VALUES (?,?,'task1','上级审批',0,0,10,'{}')`, taskID, taskID*10)
	if err != nil {
		t.Fatalf("seed task %d: %v", taskID, err)
	}
}

// adf137SeedRawActors **绕开写侧归一**直插参与者行＝模拟"修复前落库的历史数据"。
// `AddTaskActor` 会 trim＋丢空，正常路径既建不出未 trim 脏行、也建不出 actor_id 空串 行，
// 而这两档正是删除腿判据要照的。
func adf137SeedRawActors(t *testing.T, db *sql.DB, taskID int64, actorIDs ...string) {
	t.Helper()
	for _, actorID := range actorIDs {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO wf_process_task_actor (id,process_task_id,actor_id,create_user) VALUES (?,?,?,'jeeflow')`,
			adf137NextRowID(), taskID, actorID); err != nil {
			t.Fatalf("seed raw actor %q: %v", actorID, err)
		}
	}
}

// adf137RawActors 取证：库里 `wf_process_task_actor.actor_id` 的**真实列值**（按建行顺序 id ASC）。
func adf137RawActors(t *testing.T, db *sql.DB, taskID int64) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id`, taskID)
	if err != nil {
		t.Fatalf("取证 actor 行: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan actor 行: %v", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate actor 行: %v", err)
	}
	return out
}

// adf137RowCount 库里该任务的参与者行数（"一条 DELETE 都没发"这一档要数行，不只比值）。
func adf137RowCount(t *testing.T, db *sql.DB, taskID int64) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id = ?`, taskID).Scan(&n); err != nil {
		t.Fatalf("数 actor 行: %v", err)
	}
	return n
}

func adf137MustSQL(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s：库里实得 %q（%d 行）want %q", label, got, len(got), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s：库里实得 %q want %q", label, got, want)
		}
	}
}

// adf137Setup 每格一套全新的 :memory: 库＋仓储（openSQLite129 建五张 wf_* 表，列名对齐 schema-mysql.sql）。
func adf137Setup(t *testing.T, taskID int64, seeded ...string) (*sql.DB, *jdbc.Repository) {
	t.Helper()
	db := openSQLite129(t)
	t.Cleanup(func() { db.Close() })
	adf137SeedTask(t, db, taskID)
	adf137SeedRawActors(t, db, taskID, seeded...)
	return db, jdbc.New(db)
}

func adf137Remove(t *testing.T, repo *jdbc.Repository, taskID int64, actors []string) {
	t.Helper()
	if err := repo.RemoveTaskActor(context.Background(), taskID, actors); err != nil {
		t.Fatalf("RemoveTaskActor(%q): %v", actors, err)
	}
}

// ─── N 档：两种形态都必须真删得掉 ───────────────────────────────────────────────

// TestIssue137JdbcRemoveTaskActorDeletesUntrimmedLegacyRowByRawForm
// **假成功修复（语义 6 主场景）**：库里躺着未 trim 的历史脏行 ' 9101 '（直插，绕过写侧归一），
// 门面按语义 6 交出**行上的原值**去删 ⇒ 库里那一行必须真消失。
//
// 只产出 trim 形的实现（1.8.36 之前的 php/csharp/rust/moon 四栈八处，本轮**变异 A**）在这一格
// 会把 " 9101 " 削成 9101，DELETE 命中零行、门面却报成功——**被摘的人待办还在**。
func TestIssue137JdbcRemoveTaskActorDeletesUntrimmedLegacyRowByRawForm(t *testing.T) {
	db, repo := adf137Setup(t, 7310137, " 9101 ", "leader")

	adf137Remove(t, repo, 7310137, []string{" 9101 "})

	adf137MustSQL(t, "未 trim 的历史脏行必须被原值形真删掉（否则是门面报成功的假成功）",
		adf137RawActors(t, db, 7310137), []string{"leader"})
}

// TestIssue137JdbcRemoveTaskActorDeletesNormalizedRowByTrimmedForm
// **issues/142 §9.2 那一路不破**：库里是写侧归一后的规范行 '8601'，第三方绕过门面直连仓储
// 传 " 8601 " ⇒ 靠 trim 形命中，也必须删得掉。
//
// 改前红：本仓改前裸传，" 8601 " 与库里 '8601' 等值比不上 ⇒ 一行都没删（本格红）。
// **变异 B**（单点只产出原值形＝owner 10-02 第 1 问 A 案的字面形状）同样打红本格。
// 并集方案下这一格照绿 ⇒ 本轮**不需要反向改任何 142 的既有测试**（那是红线）。
func TestIssue137JdbcRemoveTaskActorDeletesNormalizedRowByTrimmedForm(t *testing.T) {
	db, repo := adf137Setup(t, 7310138, "8601", "leader")

	adf137Remove(t, repo, 7310138, []string{" 8601 "})

	adf137MustSQL(t, "规范行由 trim 形命中（issues/142 §9.2 既有判据不破）",
		adf137RawActors(t, db, 7310138), []string{"leader"})
}

// TestIssue137JdbcRemoveTaskActorBothFormsRemovedTogether 同一个人的两种写法在库里并存
// （脏行＋规范行）⇒ 两行都要摘掉，其余参与人一行不动（语义 1）。按 §2.11 归一口径它们本就是
// 同一个人，删两行才是"摘掉这个人"的正确结果，**不构成误删**。
//
// 这一格在三组变异（裸传／只 trim／只原值）下**都红**，是真正的判别式那一格。
func TestIssue137JdbcRemoveTaskActorBothFormsRemovedTogether(t *testing.T) {
	db, repo := adf137Setup(t, 7310139, " 9101 ", "9101", "leader", "boss")

	adf137Remove(t, repo, 7310139, []string{" 9101 "})

	adf137MustSQL(t, "两形并集 ⇒ 脏行与规范行一起摘，其余参与人原样保留",
		adf137RawActors(t, db, 7310139), []string{"leader", "boss"})
}

// TestIssue137JdbcRemoveTaskActorBothFormsOfTwoPeopleAreIndependent 两个人各自带脏行 ⇒
// 只摘被指名的那一个（含他的两形），另一个人的两行一行不动。钉"并集不越权扩大删除面"。
func TestIssue137JdbcRemoveTaskActorBothFormsOfTwoPeopleAreIndependent(t *testing.T) {
	db, repo := adf137Setup(t, 7310140, " 9101 ", "9101", " 9202 ", "9202", "leader")

	adf137Remove(t, repo, 7310140, []string{" 9101 "})

	adf137MustSQL(t, "只摘被指名的人（含其两形），另一个人的脏行与规范行都保留",
		adf137RawActors(t, db, 7310140), []string{" 9202 ", "9202", "leader"})
}

// ─── P 档：空值一律不参与匹配，且不得退化成"清空全部参与者" ─────────────────────

// TestIssue137JdbcRemoveTaskActorBlankInputKeepsDirtyRows **脏行保护**：空串／纯空白入参一律
// 不喂 DELETE——历史 actor_id 空串 脏行是待另案清洗的取证痕迹，不得被一次空值入参做掉。
//
// 改前红：裸传把 空串／纯空白 绑进 `IN (?)` ⇒ 库里两行脏行被删（**变异 C** 打红的就是本格与下一格）。
func TestIssue137JdbcRemoveTaskActorBlankInputKeepsDirtyRows(t *testing.T) {
	db, repo := adf137Setup(t, 7310141, "", "   ", "leader")

	adf137Remove(t, repo, 7310141, []string{"", "   ", "\t"})

	adf137MustSQL(t, "空值入参一行都不许删（含历史 actor_id=''/纯空白脏行）",
		adf137RawActors(t, db, 7310141), []string{"", "   ", "leader"})
	if n := adf137RowCount(t, db, 7310141); n != 3 {
		t.Fatalf("库里应仍是 3 行（一条 DELETE 都不该命中脏行），实得 %d 行", n)
	}
}

// TestIssue137JdbcRemoveTaskActorAllBlankInputIsNoOp **不得退化成清空**：展开后为空 ⇒ 早退，
// **一条 DELETE 都不发**。少了这一条，一次误传空串就会把该任务全部参与者清空，
// 留下永远无人可办、也无法撤回重派的死任务（语义 5 的仓储侧对偶）。
func TestIssue137JdbcRemoveTaskActorAllBlankInputIsNoOp(t *testing.T) {
	db, repo := adf137Setup(t, 7310142, "zhangsan", "leader")

	adf137Remove(t, repo, 7310142, []string{"", "  "}) // 纯空白
	adf137Remove(t, repo, 7310142, []string{})         // 空列表
	adf137Remove(t, repo, 7310142, nil)                // nil

	adf137MustSQL(t, "空入参三形（纯空白／空列表／nil）都是零删除",
		adf137RawActors(t, db, 7310142), []string{"zhangsan", "leader"})
	if n := adf137RowCount(t, db, 7310142); n != 2 {
		t.Fatalf("参与者一行不许少，实得 %d 行", n)
	}
}

// TestIssue137JdbcRemoveTaskActorNilElementNeverStringified nil 元素**不得**被串化成
// 'null'/'<nil>' 再去删——那会删掉一个真名叫 'null' 的人（spec §2.11 第 1 行点名的五栈病形；
// java 的反面形状是 String.valueOf(null)→"null"）。
//
// Go 侧仓储签名是 []string，nil 元素到不了删除腿：形态拆解那一层先丢掉它，落到仓储就是空串档。
// 本格钉两件事：① 空串档绝不命中库里字面叫 'null'/'<nil>' 的行；② 真人名 'null' 只在被显式指名时才摘。
func TestIssue137JdbcRemoveTaskActorNilElementNeverStringified(t *testing.T) {
	db, repo := adf137Setup(t, 7310143, "null", "<nil>", "leader")

	adf137Remove(t, repo, 7310143, []string{"", "   "})
	adf137MustSQL(t, "空值／nil 不得被串化成 'null'/'<nil>' 参与匹配",
		adf137RawActors(t, db, 7310143), []string{"null", "<nil>", "leader"})

	adf137Remove(t, repo, 7310143, []string{"", "leader"})
	adf137MustSQL(t, "只摘被显式指名的人，'null'/'<nil>' 字面行不受牵连",
		adf137RawActors(t, db, 7310143), []string{"null", "<nil>"})
}

// TestIssue137JdbcRemoveTaskActorZeroLikeIdsNotCollapsed 反向哨兵（§2.11 硬要求④）：
// '0' 是合法 id，摘 '0' **不得**连带摘掉 '00'——它们是两个人。判空一律"trim 后为空"才算空，
// 严禁借语言自带的假值判据。
func TestIssue137JdbcRemoveTaskActorZeroLikeIdsNotCollapsed(t *testing.T) {
	db, repo := adf137Setup(t, 7310144, "0", "00", "leader")

	adf137Remove(t, repo, 7310144, []string{"0"})

	adf137MustSQL(t, "'0' 与 '00' 是两个人，摘一个不得连带另一个",
		adf137RawActors(t, db, 7310144), []string{"00", "leader"})
}

// ─── 幂等与零操作 ──────────────────────────────────────────────────────────────

// TestIssue137JdbcRemoveTaskActorUnknownActorIsSilentlyIgnored 非参与者静默忽略（语义 7 幂等）：
// 一个都没命中 ⇒ 零删除、不报错。带空格的非参与者两形都进 IN 也一样命中零行。
func TestIssue137JdbcRemoveTaskActorUnknownActorIsSilentlyIgnored(t *testing.T) {
	db, repo := adf137Setup(t, 7310145, "zhangsan", "leader")

	adf137Remove(t, repo, 7310145, []string{"stranger", " 9999 "})

	adf137MustSQL(t, "非参与者静默忽略，既有参与者一行不动",
		adf137RawActors(t, db, 7310145), []string{"zhangsan", "leader"})
}

// TestIssue137JdbcRemoveTaskActorUnknownTaskIsNoOp 任务不存在 ⇒ 零操作、不报错、不 panic
// （删除腿对不存在的 taskID 不响亮报错——主键那一档的报错义务在门面）。
func TestIssue137JdbcRemoveTaskActorUnknownTaskIsNoOp(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)

	adf137Remove(t, repo, 404404, []string{" 9101 "})
	adf137Remove(t, repo, 404404, []string{""})
	adf137Remove(t, repo, 404404, nil)

	if n := adf137RowCount(t, db, 404404); n != 0 {
		t.Fatalf("不存在的任务 ⇒ 库里零行，实得 %d 行", n)
	}
}

// TestIssue137JdbcRemoveTaskActorMatchesMemoryRepoAnswer 两仓同答案（issues/117 场景 27）：
// 这张判据表与 memory/task_actor_delete_forms_137_test.go 的
// `TestIssue137MemoryRemoveTaskActorMatchesSqlRepoAnswer` **逐字同一张表**——
// 任何一档两仓分叉都会在两边之一变红。
func TestIssue137JdbcRemoveTaskActorMatchesMemoryRepoAnswer(t *testing.T) {
	cases := []struct {
		name   string
		seeded []string
		input  []string
		want   []string
	}{
		{"脏行按原值形命中", []string{" 9101 ", "leader"}, []string{" 9101 "}, []string{"leader"}},
		{"规范行按 trim 形命中", []string{"8601", "leader"}, []string{" 8601 "}, []string{"leader"}},
		{"两形并存一起摘", []string{" 9101 ", "9101", "leader", "boss"}, []string{" 9101 "}, []string{"leader", "boss"}},
		{"空值不删脏行", []string{"", "   ", "leader"}, []string{"", "   "}, []string{"", "   ", "leader"}},
		{"全空零删除", []string{"zhangsan", "leader"}, []string{"", "  "}, []string{"zhangsan", "leader"}},
		{"哨兵 0 与 00 是两个人", []string{"0", "00", "leader"}, []string{"0"}, []string{"00", "leader"}},
		{"非参与者静默忽略", []string{"zhangsan", "leader"}, []string{"stranger", " 9999 "}, []string{"zhangsan", "leader"}},
		{"null 字面行不被空值牵连", []string{"null", "<nil>", "leader"}, []string{"", "leader"}, []string{"null", "<nil>"}},
	}
	for i, c := range cases {
		taskID := int64(7310200 + i)
		db, repo := adf137Setup(t, taskID, c.seeded...)
		adf137Remove(t, repo, taskID, c.input)
		adf137MustSQL(t, "两仓同答案·"+c.name, adf137RawActors(t, db, taskID), c.want)
		db.Close()
	}
}
