// issues/129 案 A 第二层的 SQL 侧取证（T0，sqlite 内存库，不连 MySQL/PG，不碰 160）。
//
// 与同目录其它 *_test.go 的区别：那些是 T1（真库集成测试，默认 DSN 指开发服务器 MySQL），
// 本文件只用 modernc.org/sqlite 的 :memory:，可在任何机器上裸跑：
//
//	go test ./repository/jdbc/ -run 'TestIssue129'
//
// 盖三件事：
//  1. 归属入参（operator/actorID/filter）为空 ⇒ **空页**。本栈 SQL 侧的归属谓词是绑定参数
//     （WHERE t.operator = ?），不像 Java 那样被"空值不加条件"整条丢掉 ⇒ 无"读全库"形态，
//     但夹具刻意放一行 operator/actor_id 为空串的脏行：若按真实值比对会误出 1 行，
//     按 spec 06 §2.5「归属列为空 ⇒ 空页」必须出 0 行。
//  2. m_ 条件落在归属列（t.operator/pi.operator/pta.actor_id/cc.actor_id）且值为空
//     （空串/全空白/nil）⇒ buildWhere 生成 `AND 1=0` ⇒ 空页。
//  3. 哨兵：非归属列（t.business_no）的空值条件仍被当作"没填"忽略 ⇒ 可选过滤照旧生效，
//     本案没把 PageQuery 的通用放行改成"空值即空页"。
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

// openSQLite129 打开 :memory: sqlite 并建本栈五张 wf_* 表（列名对齐 schema-mysql.sql）。
// 必须 SetMaxOpenConns(1)：:memory: 库是连接私有的，建表连接与查询连接得是同一条。
func openSQLite129(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	stmts := []string{
		`CREATE TABLE wf_process_define (
			id INTEGER PRIMARY KEY, name TEXT NOT NULL, display_name TEXT NOT NULL, type TEXT,
			state INTEGER, content BLOB, version INTEGER, create_time TIMESTAMP, create_user TEXT,
			update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_instance (
			id INTEGER PRIMARY KEY, parent_id INTEGER, process_define_id INTEGER, state INTEGER,
			parent_node_name TEXT, business_no TEXT, operator TEXT, expire_time TIMESTAMP, variable TEXT,
			create_time TIMESTAMP, create_user TEXT, update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_task (
			id INTEGER PRIMARY KEY, process_instance_id INTEGER NOT NULL, task_name TEXT NOT NULL,
			display_name TEXT NOT NULL, task_type INTEGER, perform_type INTEGER, task_state INTEGER,
			operator TEXT, finish_time TIMESTAMP, expire_time TIMESTAMP, form_key TEXT,
			task_parent_id INTEGER, variable TEXT, create_time TIMESTAMP, create_user TEXT,
			update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_task_actor (
			id INTEGER PRIMARY KEY, process_task_id INTEGER NOT NULL, actor_id TEXT NOT NULL,
			create_time TIMESTAMP, create_user TEXT)`,
		`CREATE TABLE wf_process_cc_instance (
			id INTEGER PRIMARY KEY, process_instance_id INTEGER NOT NULL, actor_id TEXT NOT NULL,
			state INTEGER, create_time TIMESTAMP, create_user TEXT, update_time TIMESTAMP, update_user TEXT)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

// seed129 夹具（user1 与 userB 都有行，且行数不等；另放两行 operator/actor_id 为空串的脏行）：
//
//	实例：1/2 → user1、3 → userB、4 → ""（脏行）
//	待办：task 11（实例 1，DOING，actor user1）12（实例 2，DOING，actor user1）13（实例 3，DOING，actor userB）
//	已办：task 21（实例 1，DONE，operator user1）22/23（实例 3/4，DONE，operator userB / ""）
//	抄送：31 → user1、32 → userB、33 → ""（脏行）
func seed129(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	ex := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	ex(`INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (1,'g129','129测试流程','approval',1,0)`)

	type inst struct {
		id  int64
		biz string
		op  string
	}
	for _, r := range []inst{
		{1, "B1", "user1"}, {2, "B2", "user1"}, {3, "B3", "userB"}, {4, "B4", ""},
	} {
		ex(`INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
			VALUES (?,1,10,?,?, '{}')`, r.id, r.biz, r.op)
	}

	type task struct {
		id    int64
		inst  int64
		state int
		op    string
		actor string
	}
	for _, r := range []task{
		{11, 1, 10, "", "user1"}, {12, 2, 10, "", "user1"}, {13, 3, 10, "", "userB"},
		{21, 1, 20, "user1", "user1"}, {22, 3, 20, "userB", "userB"}, {23, 4, 20, "", ""},
	} {
		// form_key/display_name 给非 NULL 值：pageTasks 用裸 string 扫描（NULL 会炸），
		// 与本案无关，属夹具形状要求
		ex(`INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,operator,form_key,variable)
			VALUES (?,?,?,?,0,0,?,?,?, '{}')`, r.id, r.inst, "task1", "上级审批", r.state, r.op, "")
		ex(`INSERT INTO wf_process_task_actor (id,process_task_id,actor_id) VALUES (?,?,?)`, r.id*100, r.id, r.actor)
	}

	for _, r := range []struct {
		id   int64
		inst int64
		who  string
	}{{31, 1, "user1"}, {32, 3, "userB"}, {33, 4, ""}} {
		ex(`INSERT INTO wf_process_cc_instance (id,process_instance_id,actor_id,state) VALUES (?,?,?,0)`, r.id, r.inst, r.who)
	}
}

func q129(conds ...spi.Condition) spi.PageQuery {
	out := spi.PageQuery{PageNum: 1, PageSize: 100}
	if len(conds) > 0 {
		out.Conditions = conds
	}
	return out
}

// TestIssue129JdbcBlankOwnershipArgIsEmptyPage 归属入参三形态（空串/全空白/制表符）⇒ 空页。
// 夹具里真有一行 operator/actor_id 为空串的脏行 ⇒ 这一格同时证明"空值是按空页处理"而不是"按真实值比对"。
func TestIssue129JdbcBlankOwnershipArgIsEmptyPage(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seed129(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	// 夹具守卫：user1 档非空（否则下面的 0==0 是自等假绿）
	if rows, total, err := repo.PageInstances(ctx, q129(), "user1"); err != nil || len(rows) != 2 || total != 2 {
		t.Fatalf("前置失效 PageInstances(user1): rows=%d total=%d err=%v, want 2/2", len(rows), total, err)
	}
	if rows, total, err := repo.PageTodoTasks(ctx, q129(), "user1"); err != nil || len(rows) != 2 || total != 2 {
		t.Fatalf("前置失效 PageTodoTasks(user1): rows=%d total=%d err=%v, want 2/2", len(rows), total, err)
	}
	if rows, total, err := repo.PageDoneTasks(ctx, q129(), "user1"); err != nil || len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效 PageDoneTasks(user1): rows=%d total=%d err=%v, want 1/1", len(rows), total, err)
	}
	if rows, total, err := repo.PageCcInstances(ctx, q129(), "user1"); err != nil || len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效 PageCcInstances(user1): rows=%d total=%d err=%v, want 1/1", len(rows), total, err)
	}
	// 全库参照数（"空串⇒读全库"的反面教材：4 个实例 / 6 行参与者 / 3 行 cc）
	if rows, total, _ := repo.PageInstances(ctx, q129(), "userB"); len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效 PageInstances(userB): rows=%d total=%d, want 1/1", len(rows), total)
	}

	for _, blank := range []struct{ name, val string }{{"空串", ""}, {"全空白", "   "}, {"制表符", "\t"}} {
		if rows, total, err := repo.PageInstances(ctx, q129(), blank.val); err != nil || len(rows) != 0 || total != 0 {
			t.Fatalf("PageInstances(%s) 应空页, got rows=%d total=%d err=%v ⇒ 空串被当成真实值比对或条件被丢掉",
				blank.name, len(rows), total, err)
		}
		if rows, total, err := repo.PageTodoTasks(ctx, q129(), blank.val); err != nil || len(rows) != 0 || total != 0 {
			t.Fatalf("PageTodoTasks(%s) 应空页, got rows=%d total=%d err=%v", blank.name, len(rows), total, err)
		}
		if rows, total, err := repo.PageDoneTasks(ctx, q129(), blank.val); err != nil || len(rows) != 0 || total != 0 {
			t.Fatalf("PageDoneTasks(%s) 应空页, got rows=%d total=%d err=%v", blank.name, len(rows), total, err)
		}
		if rows, total, err := repo.PageCcInstances(ctx, q129(), blank.val); err != nil || len(rows) != 0 || total != 0 {
			t.Fatalf("PageCcInstances(%s) 应空页, got rows=%d total=%d err=%v", blank.name, len(rows), total, err)
		}
	}

	// 脏行对照：operator='' 的行确实存在（走全表计数），但空串档绝不返回它
	var dirty int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wf_process_instance WHERE operator = ''`).Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != 1 {
		t.Fatalf("夹具失效：应有 1 行 operator='' 的脏实例，实际 %d", dirty)
	}
}

// TestIssue129JdbcBuildWhereOwnershipBlankIsEmptyPage m_ 条件落在归属列 + 空值 ⇒ AND 1=0 ⇒ 空页。
// 四列逐格打（t.operator/pi.operator/pta.actor_id/cc.actor_id），非归属列留哨兵。
func TestIssue129JdbcBuildWhereOwnershipBlankIsEmptyPage(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seed129(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	for _, v := range []interface{}{"", "   ", nil} {
		// t.operator（实例分页/已办分页的归属列）
		if rows, _, err := repo.PageInstances(ctx, q129(spi.Condition{Column: "t.operator", Operator: "EQ", Value: v}), "user1"); err != nil || len(rows) != 0 {
			t.Fatalf("t.operator EQ %v 应空页, got rows=%d err=%v", v, len(rows), err)
		}
		// pi.operator（任务分页带出的实例发起人）
		if rows, _, err := repo.PageDoneTasks(ctx, q129(spi.Condition{Column: "pi.operator", Operator: "EQ", Value: v}), "user1"); err != nil || len(rows) != 0 {
			t.Fatalf("pi.operator EQ %v 应空页, got rows=%d err=%v", v, len(rows), err)
		}
		// pta.actor_id（待办归属列）
		if rows, _, err := repo.PageTodoTasks(ctx, q129(spi.Condition{Column: "pta.actor_id", Operator: "EQ", Value: v}), "user1"); err != nil || len(rows) != 0 {
			t.Fatalf("pta.actor_id EQ %v 应空页, got rows=%d err=%v", v, len(rows), err)
		}
		// cc.actor_id（抄送归属列）
		if rows, _, err := repo.PageCcInstances(ctx, q129(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: v}), "user1"); err != nil || len(rows) != 0 {
			t.Fatalf("cc.actor_id EQ %v 应空页, got rows=%d err=%v", v, len(rows), err)
		}
	}

	// 正向对照：同一列上的**非空** EQ 条件正常生效（本案没把归属列条件整个禁掉）
	if rows, _, _ := repo.PageInstances(ctx, q129(spi.Condition{Column: "t.operator", Operator: "EQ", Value: "user1"}), "user1"); len(rows) != 2 {
		t.Fatalf("t.operator EQ user1 应命中 2 行, got %d", len(rows))
	}

	// 哨兵：非归属列的空值条件仍被忽略（PageQuery 的通用放行没被改成"空值即空页"）
	// 注：JDBC 侧的通用放行有两种形态——`val == nil` 与 `val.(string) == ""`（精确空串），
	// 全空白串不在其列（Java 同形：`((String) val).isEmpty()` 也不 trim），故这里只打这两种。
	for _, v := range []interface{}{"", nil} {
		rows, total, err := repo.PageInstances(ctx, q129(spi.Condition{Column: "t.business_no", Operator: "LIKE", Value: v}), "user1")
		if err != nil || len(rows) != 2 || total != 2 {
			t.Fatalf("非归属列 t.business_no LIKE %v 应被当作没填（可选过滤照旧，2 行），got rows=%d total=%d err=%v",
				v, len(rows), total, err)
		}
	}
}

// ═══ issues/152 ② · 委托分页（扩展仓储）归属不变式的 SQL 侧取证 ═══════════════════
//
// spec 06-facade.md §4.5「processSurrogate/page 归属不变式」第二层：绕过门面直调
// `PageSurrogates` 时，落在归属列 t.operator 上的空值 ⇒ **空页**，绝不允许"这条条件不加"
// 退化成全库台账；内存仓一路（facade/facade_test.go 的
// TestIssue152MemoryPageSurrogatesBlankOwnershipIsEmptyPage）必须给同一个答案（条款 6）。
// 落点在 repository/jdbc/ext.go 两处：
//   - buildExtWhere 的归属列空值 EQ ⇒ `AND 1=0`（与核心仓 buildWhere 同一把尺子，
//     复用同一个 ownershipColumns/blankCondValue，不抄第二份）；
//   - PageSurrogates 的 Filters 形（本栈归属的第二形，Java 只有 conditions 一形）空值同样 `AND 1=0`。
// 哨兵 leg 钉"只收归属列"：buildExtWhere 同时服务 PageDesigns，且 m_LIKE_* 那套"空值当没填"
// 的通用放行必须原样保留。
//
// 跑法：go test ./repository/jdbc/ -run 'TestIssue152Jdbc'（sqlite :memory:，不连 160）

// openSurrogate152 建 wf_process_surrogate（列名对齐 repository/jdbc/ext.go 的 surrogateCols
// 与 schema-mysql.sql；类型按本文件 sqlite 习惯）
func openSurrogate152(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`CREATE TABLE IF NOT EXISTS wf_process_surrogate (
			id INTEGER PRIMARY KEY, process_name TEXT, operator TEXT NOT NULL, surrogate TEXT NOT NULL,
			start_time TIMESTAMP, end_time TIMESTAMP, enabled INTEGER DEFAULT 1,
			create_time TIMESTAMP, create_user TEXT, update_time TIMESTAMP, update_user TEXT)`); err != nil {
		t.Fatalf("create wf_process_surrogate: %v", err)
	}
}

func TestIssue152JdbcSurrogatePageBlankOwnershipIsEmptyPage(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	openSurrogate152(t, db)
	ext := jdbc.NewExt(db)
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	if err := ext.SaveSurrogate(ctx, &model.ProcessSurrogate{
		Operator: "op152-own", Surrogate: "ag152", ProcessName: "leave",
		StartTime: &start, EndTime: &end, Enabled: 1,
		CreateUser: "go-test", UpdateUser: "go-test",
	}); err != nil {
		t.Fatalf("save surrogate: %v", err)
	}
	// 夹具刻意再放一行 **operator='' 的死行**（issues/152 ③ 的产物形状）：这一行让空值档的
	// 两种旧答案都露馅——"条件被整个丢掉"会把它连同正常行一起放出全库台账，
	// "按真实值比对 `t.operator = ''`"会单独把它捞出来（1 行）。按 §4.5 归属不变式两档都必须 0 行。
	if err := ext.SaveSurrogate(ctx, &model.ProcessSurrogate{
		Operator: "", Surrogate: "ag152-dead", ProcessName: "dead-flow",
		StartTime: &start, EndTime: &end, Enabled: 1,
		CreateUser: "go-test", UpdateUser: "go-test",
	}); err != nil {
		t.Fatalf("save dead-row surrogate: %v", err)
	}
	var seeded int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wf_process_surrogate`).Scan(&seeded); err != nil || seeded != 2 {
		t.Fatalf("前置失效：台账应有 2 行（1 行真实授权人 + 1 行 operator='' 死行）, got %d err=%v", seeded, err)
	}

	totalByCond := func(v interface{}) int {
		t.Helper()
		_, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 50,
			Conditions: []spi.Condition{{Column: "t.operator", Operator: "EQ", Value: v}}})
		if err != nil {
			t.Fatalf("PageSurrogates(t.operator EQ %v): %v", v, err)
		}
		return total
	}
	totalByFilter := func(v interface{}) int {
		t.Helper()
		_, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 50,
			Filters: map[string]interface{}{"operator": v}})
		if err != nil {
			t.Fatalf("PageSurrogates(Filters[operator]=%v): %v", v, err)
		}
		return total
	}

	// 阳性对照（两形）：真实归属值必须出行，否则下面的 0==0 是自等假绿
	if n := totalByCond("op152-own"); n != 1 {
		t.Fatalf("前置失效：t.operator EQ 'op152-own' 应有 1 行, got %d", n)
	}
	if n := totalByFilter("op152-own"); n != 1 {
		t.Fatalf("前置失效：Filters[operator]='op152-own' 应有 1 行, got %d", n)
	}

	for _, v := range []interface{}{"", "   ", nil} {
		if n := totalByCond(v); n != 0 {
			t.Fatalf("归属列 t.operator EQ %v ⇒ 空页（AND 1=0），实得 %d 行 ⇒ 条件被整个丢掉、退化成全库台账", v, n)
		}
		if n := totalByFilter(v); n != 0 {
			t.Fatalf("Filters[operator] %v ⇒ 空页，实得 %d 行", v, n)
		}
	}

	// 哨兵：非归属列的空值条件仍按"没填/全匹配"放行（可选过滤没被一起收紧成"空值即空页"）。
	// 期望 2 ＝ 夹具的两行都在（真实授权人行 + operator='' 死行）——这一格恰恰证明
	// "空值当没填"那条通用放行原样保留，本案只收归属列。
	for _, v := range []interface{}{"", nil} {
		if _, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 50,
			Conditions: []spi.Condition{{Column: "t.operator", Operator: "EQ", Value: "op152-own"},
				{Column: "t.process_name", Operator: "LIKE", Value: v}}}); err != nil || total != 1 {
			t.Fatalf("非归属列 t.process_name LIKE %v 应照常按没填处理（op152-own 名下 1 行），got total=%d err=%v", v, total, err)
		}
	}
	if _, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 50}); err != nil || total != 0 {
		t.Fatalf("归属条件整条没给＝缺失档 ⇒ 空页（与内存仓同判据），got total=%d err=%v", total, err)
	}
	if _, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 50,
		Conditions: []spi.Condition{{Column: "t.operator", Operator: "EQ", Value: "op152-own"},
			{Column: "t.surrogate", Operator: "EQ", Value: "ag152"}}}); err != nil || total != 1 {
		t.Fatalf("归属列真实值 + 第二条非归属真实值应命中 1 行, got total=%d err=%v", total, err)
	}
}
