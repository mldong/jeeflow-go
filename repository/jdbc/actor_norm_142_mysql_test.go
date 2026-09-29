// 归属值写侧归一（issues/142 B 批 · Go 栈，**写侧兜底层** · SQL 仓真库一路，T1）。
//
// 为什么 sqlite 那组还不够：issues/142 §8.2 的教训——「内存/夹具绿不等于落库，
// 判据必须打在**库里的列**上」。actor_id 是 VARCHAR，MySQL 会把两端空格原样存下来，
// 所以"写侧到底 trim 没 trim"在真库上是能被读出来定性的；sqlite 那组走的是同一套 SQL 文本，
// 本格把它在 160 开发服务器 MySQL 的 jeeflow 库上再验一次（含"历史脏行"那一档：
// 真库上未 trim 的原值确实逐字保留 ⇒ 不 trim 就是同一人两行，判重被架空）。
//
// 跑法（默认 DSN 指 192.168.1.160:3306/jeeflow，可用 JEFFLOW_DB_DRIVER/JEFFLOW_DB_DSN 覆盖）：
//
//	go test ./repository/jdbc/ -run 'TestIssue142RealDB'
//
// 测试自己按固定 id 清场（t.Cleanup），不依赖也不触碰其他格的数据。
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

const (
	a142RealTask  = int64(921426001) // 本组专用 id（其他格用的是雪花/ts id，不会撞）
	a142RealInst  = int64(921426002)
	a142RealDirty = int64(921426003) // 历史脏行的行 id（直插，绕过写侧兜底）
)

// a142RealCleanup 按 id 清场（进也清、出也清，幂等）。
func a142RealCleanup(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`DELETE FROM wf_process_task_actor WHERE process_task_id = ?`,
		`DELETE FROM wf_process_task WHERE id = ?`,
	} {
		if _, err := db.ExecContext(ctx, ph(q), a142RealTask); err != nil {
			t.Fatalf("清场 %q: %v", q, err)
		}
	}
}

// a142RealActors 真库里该任务的 actor_id 原值（按建行顺序）。
func a142RealActors(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		ph(`SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id`),
		a142RealTask)
	if err != nil {
		t.Fatalf("取证 actor 行: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return out
}

// TestIssue142RealDBAddTaskActorColumnsAreNormalized 真库列值定性：
// 空串/纯空白不落库、落库的是 trim 后的串、同次调用折叠、哨兵 id 全部保留，
// 且与"修复前落下的未 trim 脏行"判为同一个人（不再落第二行）。
//
// 改前红：真库里实得 [" 9101 " "" "   " " 9201 " "<nil>" "0" "00" " "] ——
// 空归属值进表、同一人两行、哨兵之外还多两条垃圾形状。
func TestIssue142RealDBAddTaskActorColumnsAreNormalized(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	// 清场注册在 Close 之后 ⇒ t.Cleanup 是 LIFO，先按 id 清数据、再关连接
	// （defer db.Close() 会早于 t.Cleanup 执行，清场就撞上"database is closed"）
	t.Cleanup(func() { db.Close() })
	t.Cleanup(func() { a142RealCleanup(t, db) })
	a142RealCleanup(t, db)

	repo := jdbc.New(db)
	// 任务行走仓储自己的 INSERT 通道；参与者用 SaveTask 直投（**绕过 AddTaskActor**）
	// ⇒ 摆出"修复前落下的历史脏行"，那一列在真库里逐字是 " 9101 "。
	now := time.Now()
	task := &model.ProcessTask{
		ID: a142RealTask, ProcessInstanceID: a142RealInst, TaskName: "task1",
		DisplayName: "上级审批", TaskState: model.TaskStateDoing,
		CreateTime: now, UpdateTime: now, CreateUser: "a142", UpdateUser: "a142",
		ActorIDs: []string{" 9101 "}, Variables: map[string]interface{}{},
	}
	if err := repo.SaveTask(ctx, task); err != nil {
		t.Fatalf("建任务行: %v", err)
	}
	if got := a142RealActors(t, db); len(got) != 1 {
		t.Fatalf("夹具：脏行应先落一条，实得 %q", got)
	}

	if err := repo.AddTaskActor(ctx, a142RealTask,
		[]string{"9101", " 9101 ", "", "   ", " 9201 ", "0", "00", " ", "null"}); err != nil {
		t.Fatalf("AddTaskActor: %v", err)
	}

	want := []string{" 9101 ", "9201", "0", "00", "null"}
	got := a142RealActors(t, db)
	if len(got) != len(want) {
		t.Fatalf("§2.11：真库列值应 %q，实得 %q", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("§2.11：第 %d 行应逐字是 %q，实得 %q（全量 %q）", i, want[i], got[i], got)
		}
	}
}
