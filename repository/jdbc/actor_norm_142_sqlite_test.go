// 归属值写侧归一（issues/142 B 批 · Go 栈，**写侧兜底层** · SQL 仓一路，sqlite :memory:）。
//
// spec 06-facade.md §2.11 写点表最后一行「两仓 addTaskActor …仓储写侧自己再挡一次」：
// 改前 JDBC 仓与内存仓同病（普查底稿 issues/142 §2 B 表 go 行：`jdbc.go:615-639`、
// `memory.go:277-292` 两仓只判重 ⇒ ""/空白全放行）。
//
// 本文件是 memory/actor_norm_142_test.go 的 SQL 镜像（同一份数据、两仓必须同答案，
// issues/117 场景 27 那把尺子），判据单点同样是 [spi.NormalizeActors]。
// **取证一律走库里的列而不是返回值**（issues/142 §8.2 的教训：内存绿不等于落库，
// " 8901 " 这类两端空格 VARCHAR 会逐字保留，不 trim 就是两行，把判重打穿）。
//
// 跑法（sqlite :memory:，不连 160 真库）：
//
//	go test ./repository/jdbc/ -run 'TestIssue142'
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// a142SeedTask 摆一条 DOING 任务行（写侧兜底档的前置：AddTaskActor 要有任务可挂）。
func a142SeedTask(t *testing.T, db *sql.DB, taskID, instanceID int64) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,variable)
		 VALUES (?,?,'task1','上级审批',0,0,10,'{}')`, taskID, instanceID)
	if err != nil {
		t.Fatalf("seed task %d: %v", taskID, err)
	}
}

// a142SeedRawActor 直插一条参与者行（**绕过 AddTaskActor**）＝摆"修复前落下的历史脏行"。
func a142SeedRawActor(t *testing.T, db *sql.DB, taskID int64, id int64, actorID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO wf_process_task_actor (id,process_task_id,actor_id) VALUES (?,?,?)`,
		id, taskID, actorID); err != nil {
		t.Fatalf("seed raw actor %q: %v", actorID, err)
	}
}

// a142RawActors 库里该任务的 actor_id 原值（按建行顺序 id ASC）。
func a142RawActors(t *testing.T, db *sql.DB, taskID int64) []string {
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

func mustActors142SQL(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s：实得 %q（%d 个）want %q", label, got, len(got), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s：实得 %q want %q", label, got, want)
		}
	}
}

// TestIssue142JdbcAddTaskActorDropsBlankKeepsValid 空串/纯空白不落库，有效的人照旧一行。
//
// 改前红：SQL 仓写侧只判重不判空 ⇒ 库里实得 ["", "   ", "8801"] 三行。
func TestIssue142JdbcAddTaskActorDropsBlankKeepsValid(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214231, 8214231)

	if err := repo.AddTaskActor(context.Background(), 9214231, []string{"", "   ", "8801"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142SQL(t, "§2.11：SQL 仓写侧空串/纯空白都不落行",
		a142RawActors(t, db, 9214231), []string{"8801"})
}

// TestIssue142JdbcAddTaskActorStoresTrimmedValue 落库列里的值是 trim 后的串（硬要求②）。
//
// 改前红：库里那一列原样存 " 8901 "（带两端空格）。
func TestIssue142JdbcAddTaskActorStoresTrimmedValue(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214232, 8214232)

	if err := repo.AddTaskActor(context.Background(), 9214232, []string{" 8901 "}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142SQL(t, "§2.11：库里的列应是 trim 后的串",
		a142RawActors(t, db, 9214232), []string{"8901"})
}

// TestIssue142JdbcAddTaskActorCollapsesWithinOneCall 同次调用的重复折叠（" 9001 " 与 "9001" 同一个人）。
//
// 改前红：判重按原串比 ⇒ 库里两行。
func TestIssue142JdbcAddTaskActorCollapsesWithinOneCall(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214233, 8214233)

	if err := repo.AddTaskActor(context.Background(), 9214233,
		[]string{"9001", " 9001 ", "9001"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142SQL(t, "§2.11：同次调用折叠成一行",
		a142RawActors(t, db, 9214233), []string{"9001"})
}

// TestIssue142JdbcAddTaskActorDedupHitsLegacyUntrimmedRow 判重比较取 trim 后的值：
// 库里历史脏行是 " 9101 "（直插，绕过写侧），再给 "9101" 必须判成同一个人 ⇒ 不落第二行。
//
// 改前红：seen 按原串比 ⇒ 库里两行（同一人两行＝把 issues/141 G2 那类判重打穿的形状搬到任务侧）。
func TestIssue142JdbcAddTaskActorDedupHitsLegacyUntrimmedRow(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214234, 8214234)
	a142SeedRawActor(t, db, 9214234, 7214234, " 9101 ")

	if err := repo.AddTaskActor(context.Background(), 9214234, []string{"9101"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	got := a142RawActors(t, db, 9214234)
	if len(got) != 1 {
		t.Fatalf("§2.11：trim 后同值命中判重 ⇒ 库里不得两行，实得 %q", got)
	}
}

// TestIssue142JdbcAddTaskActorKeepsSentinelIds 反向哨兵（SQL 侧）："0"/"00"/"null"/"a"
// 都是正常归属值且互不相同（严禁松散比较把 "0" 与 "00" 折成一个），只有纯空白被丢。
//
// 改前红：" " 也落一行空归属值。
func TestIssue142JdbcAddTaskActorKeepsSentinelIds(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214235, 8214235)

	if err := repo.AddTaskActor(context.Background(), 9214235,
		[]string{"0", "00", " ", "null", "a"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	mustActors142SQL(t, "§2.11 只丢空串/纯空白：哨兵 id 不得被吃掉或折叠",
		a142RawActors(t, db, 9214235), []string{"0", "00", "null", "a"})
}

// TestIssue142JdbcAddTaskActorAllBlankIsNoop 全空白 ⇒ 库里零行（不是落一条 actor_id 为空串 的行）。
//
// 改前红：库里两行空归属值——issues/129 那族"空 actor_id 读全库"的进水口。
func TestIssue142JdbcAddTaskActorAllBlankIsNoop(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	a142SeedTask(t, db, 9214236, 8214236)

	if err := repo.AddTaskActor(context.Background(), 9214236, []string{"", "  "}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}
	if got := a142RawActors(t, db, 9214236); len(got) != 0 {
		t.Fatalf("§2.11：全空白必须库里零行，实得 %q", got)
	}
}

// TestIssue142JdbcUpdateCcStatusComparesTrimmedValue updateCCStatus 的 operator 归一后再比
// （§2.11 写点表第 4 行）：入参带两端空格仍打中 trim 后落库的那一行。
//
// 改前红：`WHERE actor_id=?` 拿未 trim 原值比 ⇒ 一行都没更新，SQL 仓静默 no-op。
func TestIssue142JdbcUpdateCcStatusComparesTrimmedValue(t *testing.T) {
	ctx := context.Background()
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	instanceID := newInstance141(t, db, 9214237, "A142-CC-TRIM-SQL")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "4201"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if err := repo.UpdateCcStatus(ctx, instanceID, " 4201 "); err != nil {
		t.Fatalf("UpdateCcStatus: %v", err)
	}
	var state int
	if err := db.QueryRowContext(ctx, ph142(`SELECT state FROM wf_process_cc_instance WHERE process_instance_id = ?`),
		instanceID).Scan(&state); err != nil {
		t.Fatalf("取证 cc state: %v", err)
	}
	if state != 1 {
		t.Fatalf("§2.11：入参归一后再比 ⇒ 库里那行应 state=1，实得 %d", state)
	}
}

// ph142 sqlite 一路用 ? 占位符（本文件不连 MySQL/PG，直译即可）。
func ph142(s string) string { return s }
