// 参与者**删除腿**归属值判据（issues/137 §3-6 · Go 栈 · **内存仓一路**）。
//
// 立法逐字依据＝spec 06-facade.md §processTask/removeTaskActor 语义 6 ＋ §2.11 写点表末行
// （owner 2026-10-02 拍「两形并集」）；判据基准＝ jeeflow-java `TaskActorDeleteFormsTest`
// （java 基准腿 commit c48a6ba，14 格逐格对照）。判据本体只有一枚＝[spi.ActorDeleteForms]，
// 本文件钉的是"内存仓 `RemoveTaskActor` 确实走了它"。
//
// 本栈改前现状（1.8.36）：`memory/repository.go` 的删除腿是**裸传**——`for _, a := range actors`
// 逐个塞进 map 做等值命中，既不产出 trim 形（第三方直连仓储传 " 8601 " 删不掉规范行 8601，
// issues/142 §9.2 那一路），也不丢空值（空串入参会把历史 actor_id 空串 脏行删掉，
// 那是替脏数据做掉唯一痕迹）。
//
// 与 repository/jdbc/task_actor_delete_forms_137_sqlite_test.go 是同一组格子的两仓镜像：
// **同一份数据两仓必须同答案**（issues/117 场景 27 那把尺子）。
//
// 脏行夹具一律**直投 SaveTask 的 ActorIDs**（绕过写侧归一）：`AddTaskActor` 会 trim＋丢空，
// 正常路径既建不出未 trim 的 " 9101 "、也建不出 actor_id 空串 行，而这两档正是删除腿要照的。
// 脏行值一律用**前导空格**——MySQL 5.7 PAD SPACE 只忽略尾部空格、8.0 NO PAD 连尾部也算，
// 前导空格在任何排序规则下都与规范行不等，判据不会因跑在哪台库上而漂（SQL 仓那一路同口径）。
package memory_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// adf137Task 造一条 DOING 任务行，actors 用 SaveTask **直投**（绕过 AddTaskActor 的写侧归一），
// 专门用来摆"修复前落下的历史脏行"（未 trim 原值、actor_id 空串）。
func adf137Task(t *testing.T, repo *memory.Repository, id int64, actors []string) int64 {
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

// adf137Actors 取证：该任务参与者行的当前读数（内存仓的"库里的列"）。
func adf137Actors(t *testing.T, repo *memory.Repository, taskID int64) []string {
	t.Helper()
	got, err := repo.FindTaskActors(context.Background(), taskID)
	if err != nil {
		t.Fatalf("读参与者: %v", err)
	}
	return got
}

func adf137Must(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
		t.Fatalf("%s：实得 %q（%d 个）want %q", label, got, len(got), want)
	}
}

func adf137Remove(t *testing.T, repo *memory.Repository, taskID int64, actors []string) {
	t.Helper()
	if err := repo.RemoveTaskActor(context.Background(), taskID, actors); err != nil {
		t.Fatalf("RemoveTaskActor(%q): %v", actors, err)
	}
}

// ─── N 档：两种形态都必须真删得掉 ───────────────────────────────────────────────

// TestIssue137MemoryRemoveTaskActorDeletesUntrimmedLegacyRowByRawForm
// **假成功修复（语义 6 主场景）**：库里躺着修复前落下的未 trim 历史脏行 " 9101 "，
// 门面按语义 6 交出**行上的原值**去删 ⇒ 那一行必须真消失。
//
// 改前红：本仓改前是裸传，这一格**本来就绿**（原值形命中）；打红它的是**变异 A**
// （单点只产出 trim 形＝php/csharp/rust/moon 旧形状）⇒ 脏行留在库里、门面报成功，
// 被摘的人待办还在。本格与下面"两形并存"那格一起把变异 A 咬住。
func TestIssue137MemoryRemoveTaskActorDeletesUntrimmedLegacyRowByRawForm(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301371, []string{" 9101 ", "leader"})

	adf137Remove(t, repo, taskID, []string{" 9101 "})

	adf137Must(t, "未 trim 的历史脏行必须被原值形删掉（否则是门面报成功的假成功）",
		adf137Actors(t, repo, taskID), []string{"leader"})
}

// TestIssue137MemoryRemoveTaskActorDeletesNormalizedRowByTrimmedForm
// **issues/142 §9.2 那一路不破**：库里是写侧归一后的规范行 8601，第三方绕过门面直连仓储
// 传 " 8601 " ⇒ 靠 trim 形命中，也必须删得掉。
//
// 改前红：本仓改前裸传，拿 " 8601 " 去等值匹配规范行 8601 ⇒ **一行都没删**（本格红）。
// 变异 B（单点只产出原值形＝owner 10-02 第 1 问 A 案的字面形状）同样打红本格——
// 这正是"只取原值会破 142 既有判据"的实测证据。
func TestIssue137MemoryRemoveTaskActorDeletesNormalizedRowByTrimmedForm(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301372, []string{"8601", "leader"})

	adf137Remove(t, repo, taskID, []string{" 8601 "})

	adf137Must(t, "规范行由 trim 形命中（issues/142 §9.2 的既有判据不破）",
		adf137Actors(t, repo, taskID), []string{"leader"})
}

// TestIssue137MemoryRemoveTaskActorBothFormsRemovedTogether 同一个人的两种写法在库里并存
// （脏行＋规范行）⇒ 两行都要摘掉，其余参与人一行不动（语义 1「只摘不加、不动其他参与人」）。
// 按 §2.11 归一口径它们本就是同一个人，删两行才是"摘掉这个人"的正确结果，**不构成误删**。
//
// 这一格在三组变异（裸传／只 trim／只原值）下**都红**，是真正的判别式那一格。
func TestIssue137MemoryRemoveTaskActorBothFormsRemovedTogether(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301373, []string{" 9101 ", "9101", "leader", "boss"})

	adf137Remove(t, repo, taskID, []string{" 9101 "})

	adf137Must(t, "两形并集 ⇒ 脏行与规范行一起摘，其余参与人原样保留",
		adf137Actors(t, repo, taskID), []string{"leader", "boss"})
}

// ─── P 档：空值一律不参与匹配，且不得退化成"清空全部参与者" ─────────────────────

// TestIssue137MemoryRemoveTaskActorBlankInputKeepsDirtyRows **脏行保护**：空串／纯空白入参
// 一律不参与匹配——历史 actor_id 空串 脏行是待另案清洗的取证痕迹，不得被一次空值入参做掉
// （issues/129 那族"空归属值读全库"的删除位对偶）。
//
// 改前红：裸传把 ""／"   " 塞进命中集合 ⇒ 两行脏行被删（变异 C 打红的就是本格与下一格）。
func TestIssue137MemoryRemoveTaskActorBlankInputKeepsDirtyRows(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301374, []string{"", "   ", "leader"})

	adf137Remove(t, repo, taskID, []string{"", "   ", "\t"})

	adf137Must(t, "空值入参一行都不许删（含历史 actor_id 空串／纯空白脏行）",
		adf137Actors(t, repo, taskID), []string{"", "   ", "leader"})
}

// TestIssue137MemoryRemoveTaskActorAllBlankInputIsNoOp **不得退化成清空**：展开后为空 ⇒ 早退，
// 一次删除都不发生。少了这一条，一次误传空串就会把该任务全部参与者清空，留下永远无人可办、
// 也无法撤回重派的死任务（语义 5「至少需保留一名参与人」的仓储侧对偶）。
func TestIssue137MemoryRemoveTaskActorAllBlankInputIsNoOp(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301375, []string{"zhangsan", "leader"})

	adf137Remove(t, repo, taskID, []string{"", "  "}) // 纯空白
	adf137Remove(t, repo, taskID, []string{})         // 空列表
	adf137Remove(t, repo, taskID, nil)                // nil

	adf137Must(t, "空入参三形（纯空白／空列表／nil）都是零删除，不得清空参与者",
		adf137Actors(t, repo, taskID), []string{"zhangsan", "leader"})
}

// TestIssue137MemoryRemoveTaskActorNilElementNeverStringified nil 元素**不得**被串化成
// "null"/"<nil>" 再去匹配——那会删掉一个真名叫 "null" 的人（spec §2.11 第 1 行点名的五栈病形）。
//
// Go 侧仓储签名是 []string，nil 元素到不了删除腿：形态拆解那一层（门面 toStringSlice2／
// 引擎 valueToActors）先把它丢掉，落到仓储就是空串档。本格因此钉两件事：
// ① 空串档绝不命中字面叫 "null"/"<nil>" 的行；② 真人名 "null" 只在被显式指名时才摘。
func TestIssue137MemoryRemoveTaskActorNilElementNeverStringified(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301376, []string{"null", "<nil>", "leader"})

	// nil 元素在 []string 里只能以"" 这一档出现（拆解层丢掉后的形状）⇒ 一行都不许删
	adf137Remove(t, repo, taskID, []string{"", "   "})
	adf137Must(t, "nil／空值不得被串化成 \"null\"/\"<nil>\" 参与匹配",
		adf137Actors(t, repo, taskID), []string{"null", "<nil>", "leader"})

	// 显式指名 "leader" ⇒ 只摘 leader，字面叫 "null"/"<nil>" 的两行原样保留
	adf137Remove(t, repo, taskID, []string{"", "leader"})
	adf137Must(t, "只摘被显式指名的人，\"null\"/\"<nil>\" 字面行不受牵连",
		adf137Actors(t, repo, taskID), []string{"null", "<nil>"})
}

// TestIssue137MemoryRemoveTaskActorZeroLikeIdsNotCollapsed 反向哨兵（§2.11 硬要求④）：
// "0" 是合法 id，摘 "0" **不得**连带摘掉 "00"——它们是两个人。
// 判空一律 trim(x) == ""，严禁借语言自带的假值判据（那会把 "0" 当空丢掉）。
func TestIssue137MemoryRemoveTaskActorZeroLikeIdsNotCollapsed(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301377, []string{"0", "00", "leader"})

	adf137Remove(t, repo, taskID, []string{"0"})

	adf137Must(t, "'0' 与 '00' 是两个人，摘一个不得连带另一个",
		adf137Actors(t, repo, taskID), []string{"00", "leader"})
}

// TestIssue137MemoryRemoveTaskActorPaddedInputHitsZeroLikeDirtyRow 哨兵与两形并集的交叉档：
// 库里是未 trim 的 " 0 "（脏行），入参给规范形 "0" ⇒ 门面会交行原值，但第三方直连仓储只给 "0"，
// 此时靠原值形删不掉、靠 trim 形也对不上 " 0 " 那一行 ⇒ **该行留下**（并集只展开入参，
// 不把库里的行反推成两形）。本格钉住"不越权扩大删除面"这条边界。
func TestIssue137MemoryRemoveTaskActorPaddedInputHitsZeroLikeDirtyRow(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301378, []string{" 0 ", "leader"})

	adf137Remove(t, repo, taskID, []string{" 0 "})

	adf137Must(t, "入参带原值形 ⇒ 脏行删掉，其余人不动",
		adf137Actors(t, repo, taskID), []string{"leader"})
}

// ─── 幂等与零操作 ──────────────────────────────────────────────────────────────

// TestIssue137MemoryRemoveTaskActorUnknownActorIsSilentlyIgnored 非参与者静默忽略
// （语义 7 幂等）：一个都没命中 ⇒ 零删除、不报错。
func TestIssue137MemoryRemoveTaskActorUnknownActorIsSilentlyIgnored(t *testing.T) {
	repo := memory.New()
	taskID := adf137Task(t, repo, 7301379, []string{"zhangsan", "leader"})

	adf137Remove(t, repo, taskID, []string{"stranger", " 9999 "})

	adf137Must(t, "非参与者静默忽略，既有参与者一行不动",
		adf137Actors(t, repo, taskID), []string{"zhangsan", "leader"})
}

// TestIssue137MemoryRemoveTaskActorUnknownTaskIsNoOp 任务不存在（无任何参与者行）⇒ 零操作、
// 不 panic、不报错（删除腿对不存在的 taskID 不响亮报错——主键那一档的报错义务在门面）。
func TestIssue137MemoryRemoveTaskActorUnknownTaskIsNoOp(t *testing.T) {
	repo := memory.New()

	adf137Remove(t, repo, 404404, []string{" 9101 "})
	adf137Remove(t, repo, 404404, []string{""})
	adf137Remove(t, repo, 404404, nil)

	adf137Must(t, "任务不存在 ⇒ 零操作", adf137Actors(t, repo, 404404), nil)
}

// TestIssue137MemoryRemoveTaskActorMatchesSqlRepoAnswer 两仓同答案（issues/117 场景 27）：
// 同一份数据、同一条入参，内存仓的读数必须与 SQL 仓那一路
// （repository/jdbc/task_actor_delete_forms_137_sqlite_test.go 的同名格子）逐字一致。
// 本格拉的是"判据表"这一层：表里每一档都过一遍，任何一档与 SQL 仓分叉都会在两边之一变红。
func TestIssue137MemoryRemoveTaskActorMatchesSqlRepoAnswer(t *testing.T) {
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
		repo := memory.New()
		taskID := adf137Task(t, repo, int64(7301390+i), c.seeded)
		adf137Remove(t, repo, taskID, c.input)
		adf137Must(t, "两仓同答案·"+c.name, adf137Actors(t, repo, taskID), c.want)
	}
}
