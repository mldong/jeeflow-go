// issues/141 G7 取证（T0，sqlite 内存库，不连 MySQL/PG，不碰 160）——G3 的同类，换**数值列**家族。
//
// 缺陷形状：repository/jdbc 的 pageTasks（PageTodoTasks / PageDoneTasks 共用一条实现）把可空
// 数值列裸扫进 int —— `t.task_type → &row.TaskType`、`t.perform_type → &row.PerformType`、
// `t.task_state → &row.TaskState`、`pd.version → &row.DefineVersion`。
// 这些列建表即允许 NULL（schema-mysql.sql:43-45 任务三列、:10 define.version），pd.* 还额外走
// LEFT JOIN ⇒ 定义行缺失必然 NULL。孤儿任务是真实形态：RemoveDefine 只删定义行，任务行不跟着删。
// database/sql 对 NULL → int 直接报 `converting NULL to int is unsupported`（sqlite；
// MySQL 驱动回 int64 ⇒ 同一病灶文案为 `converting NULL to int64 is unsupported`），
// ⇒ **整条待办/已办分页失败**（不是单列降级，用户视角＝待办列表打不开）。
// 同函数族的 pageInstances / pageCC 早已用 sql.NullInt64，**独漏 pageTasks**。
//
// 出口形状对齐 jeeflow-java JdbcProcessRepository.mapTaskRow:1122-1124（rs.getInt：
// SQL NULL ⇒ Java int 0，wasNull 只判空、不改出口值）⇒ 修法＝NullInt64 接收后取 Int64，
// **不**改成 *int64（G3 定下的指针投影只管 string 家族）。
//
// 夹具用**自己的 id 段（91417xxx）**，并且**一列一档**（每行只让一个可空数值列出 NULL）：
// 混跑时 rows.Scan 在第一个 NULL 列就中断，其余病灶列被它遮住 ⇒ 逐列隔离才叫逐列取证。
// TestIssue1417EachNullableIntColumnIsolated 用 t.id EQ 条件把每档单跑一次。
package jdbc_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

// 本案专用 id 段（避开 seed129 的 1..4、G3 的 91410xxx）
const (
	n1417defineOK      = int64(91417001) // version=7，正向基准定义
	n1417defineNullVer = int64(91417002) // 定义行**在**，但 version 列 NULL（列档）
	n1417defineMissing = int64(91417003) // **没有**定义行 ⇒ LEFT JOIN 整片 NULL（JOIN 档）
	n1417instOK        = int64(91417101) // → 91417001
	n1417instNullVer   = int64(91417102) // → 91417002
	n1417instOrphan    = int64(91417103) // → 91417003（孤儿）

	n1417taskOrphanVer = int64(91417201) // 只有 pd.version NULL（JOIN 档）
	n1417taskDefineVer = int64(91417202) // 只有 pd.version NULL（列档：定义在、version 空）
	n1417taskNullType  = int64(91417203) // 只有 task_type NULL，定义健康
	n1417taskNullPerf  = int64(91417204) // 只有 perform_type NULL，定义健康
	n1417taskAllValued = int64(91417205) // 数值列全有值 ⇒ 正向对照
	n1417taskDoneNull  = int64(91417206) // 已办：task_type+perform_type NULL ＋ 孤儿定义

	n1417actorBase = int64(91417301)
)

const (
	n1417TodoActor = "i1417todo"
	n1417DoneOpera = "i1417done"
)

// seedNullInts1417 造"任务行存在、可空数值列为 NULL（列档 / JOIN 档两种来源）"的夹具。
// 列名对齐 openSQLite129 的建表；task_state 一律非 NULL —— 分页谓词 `= 10` / `<> 10`
// 让 NULL 行根本进不了结果集（本案修复它属防御性收口，见报告备注）。
func seedNullInts1417(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	ex := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}

	// 定义：version=7（正向）/ version NULL（列档）；91417003 故意**不建**（JOIN 档）
	ex(`INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (?,?,?,'approval',1,7)`,
		n1417defineOK, "g1417ok", "1417健康定义")
	ex(`INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (?,?,?,'approval',1,NULL)`,
		n1417defineNullVer, "g1417nulver", "1417无版本定义")

	// 实例：健康 / 定义在但 version 列 NULL / 孤儿（process_define_id 指向不存在的定义）
	ex(`INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,'B1417ok','i1417op','{}')`, n1417instOK, n1417defineOK)
	ex(`INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,'B1417nv','i1417op','{}')`, n1417instNullVer, n1417defineNullVer)
	ex(`INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,'B1417orphan','i1417op','{}')`, n1417instOrphan, n1417defineMissing)

	// 待办四档 + 正向对照：interface{} 传 nil ⇒ 该列落 NULL
	type todo struct {
		id   int64
		inst int64
		typ  interface{}
		perf interface{}
	}
	todos := []todo{
		{n1417taskOrphanVer, n1417instOrphan, 0, 0},
		{n1417taskDefineVer, n1417instNullVer, 0, 0},
		{n1417taskNullType, n1417instOK, nil, 0},
		{n1417taskNullPerf, n1417instOK, 0, nil},
		{n1417taskAllValued, n1417instOK, 1, 1},
	}
	for i, r := range todos {
		// operator/form_key 给 NULL：G3 修过的 string 家族，顺带证明没被本次改动打破
		ex(`INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,operator,form_key,variable)
			VALUES (?,?,?,?,?,?,10,NULL,NULL,'{}')`,
			r.id, r.inst, "task1", "上级审批", r.typ, r.perf)
		ex(`INSERT INTO wf_process_task_actor (id,process_task_id,actor_id) VALUES (?,?,?)`,
			n1417actorBase+int64(i), r.id, n1417TodoActor)
	}

	// 已办一档：谓词 `t.task_state <> 10` ⇒ 用 20；task_type/perform_type NULL ＋ 孤儿定义
	ex(`INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,operator,form_key,variable)
		VALUES (?,?,?,?,NULL,NULL,20,?,NULL,'{}')`,
		n1417taskDoneNull, n1417instOrphan, "task2", "已办节点", n1417DoneOpera)
}

func mustHit1417(t *testing.T, rows []*model.TaskRow, id int64, label string) *model.TaskRow {
	t.Helper()
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("夹具失效：%s 应命中任务 %d（共 %d 行）", label, id, len(rows))
	return nil
}

// byID1417 单档分页：t.id EQ 条件把这一行单独捞出来（taskWhitelist 含 t.id）。
func byID1417(t *testing.T, repo *jdbc.Repository, taskID int64, done bool) *model.TaskRow {
	t.Helper()
	ctx := context.Background()
	q := q129(spi.Condition{Column: "t.id", Operator: "EQ", Value: taskID})
	var rows []*model.TaskRow
	var err error
	if done {
		rows, _, err = repo.PageDoneTasks(ctx, q, n1417DoneOpera)
	} else {
		rows, _, err = repo.PageTodoTasks(ctx, q, n1417TodoActor)
	}
	if err != nil {
		t.Fatalf("任务 %d 单档分页报错（修复前此处必为 converting NULL to int…）: %v", taskID, err)
	}
	return mustHit1417(t, rows, taskID, "单档")
}

// TestIssue1417PageTodoTasksNullableIntColumns 主证：待办分页里可空数值列为 NULL（列档 + JOIN 档）
// 时查询不报错，且这些列投成 **0**（对齐 Java rs.getInt）。修复前 rows.Scan 必报
// `converting NULL to int is unsupported` ⇒ 本格报红。
func TestIssue1417PageTodoTasksNullableIntColumns(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullInts1417(t, db)
	repo := jdbc.New(db)

	rows, _, err := repo.PageTodoTasks(context.Background(), q129(), n1417TodoActor)
	if err != nil {
		t.Fatalf("PageTodoTasks 含 NULL 数值列的行不应报错: %v", err)
	}

	// ① JOIN 档孤儿：LEFT JOIN 无定义行 ⇒ pd.version NULL ⇒ 0；定义名仍投 nil（G3 形状不回归）
	orphan := mustHit1417(t, rows, n1417taskOrphanVer, "孤儿任务")
	if orphan.DefineVersion != 0 {
		t.Fatalf("pd.version 缺失（JOIN 无定义行）应投 0（对齐 Java rs.getInt），实得 %d", orphan.DefineVersion)
	}
	if orphan.TaskType != 0 || orphan.PerformType != 0 {
		t.Fatalf("该档 task_type/perform_type 本就有值 0，不应变形: %d/%d", orphan.TaskType, orphan.PerformType)
	}
	if orphan.ProcessDefineName != nil {
		t.Fatalf("定义缺失时 processDefineName 应仍投 nil（G3 形状），实得 %q", *orphan.ProcessDefineName)
	}

	// ② 列档：定义行在、version 列本身 NULL ⇒ 同样 0（与 JOIN 档同出口，不同来源）
	defVer := mustHit1417(t, rows, n1417taskDefineVer, "定义 version 列为 NULL")
	if defVer.DefineVersion != 0 {
		t.Fatalf("define.version 列 NULL 应投 0，实得 %d", defVer.DefineVersion)
	}

	// ③ task_type / perform_type NULL，定义健康（同一条 SQL 里"这列出 0、那列照旧"的交叉验证）
	nullType := mustHit1417(t, rows, n1417taskNullType, "task_type NULL")
	if nullType.TaskType != 0 {
		t.Fatalf("task_type=NULL 应投 0，实得 %d", nullType.TaskType)
	}
	nullPerf := mustHit1417(t, rows, n1417taskNullPerf, "perform_type NULL")
	if nullPerf.PerformType != 0 {
		t.Fatalf("perform_type=NULL 应投 0，实得 %d", nullPerf.PerformType)
	}
	if nullPerf.DefineVersion != 7 {
		t.Fatalf("同一条查询里健康定义的 version 应照旧读出 7，实得 %d ⇒ 修成整列塌 0 了", nullPerf.DefineVersion)
	}
	if nullPerf.TaskState != model.TaskStateDoing {
		t.Fatalf("task_state 谓词保证非 NULL，应照旧读出 10，实得 %d", nullPerf.TaskState)
	}

	// ④ 出口 JSON 形状锁死：数值列是**有值的 0**，不是 null、不是缺键
	//（这一条同时挡掉"把 G3 的 *string 外溢成 *int64"的形状漂移）
	b, err := json.Marshal(orphan)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, want := range []string{`"taskType":0`, `"performType":0`, `"defineVersion":0`, `"taskState":10`} {
		if !strings.Contains(s, want) {
			t.Fatalf("孤儿任务行的 JSON 应含 %s，实得 %s", want, s)
		}
	}
	for _, bad := range []string{`"taskType":null`, `"performType":null`, `"defineVersion":null`} {
		if strings.Contains(s, bad) {
			t.Fatalf("数值列 NULL 的合法出口是 0（对齐 Java rs.getInt），不得出 %s，实得 %s", bad, s)
		}
	}
}

// TestIssue1417EachNullableIntColumnIsolated 逐列隔离取证：每档单跑一次分页，证明
// **"只有这一列出 NULL"也足以让整条分页报错**（修复前逐列报红、报错各指自己的列名），
// 修复后逐列投 0 且其余列照旧。混跑时 Scan 在第一个 NULL 列就中断，后面的病灶列会被遮住，
// 所以这一格才是"四列都有牙"的证据；正向对照档（全有值）修复前后都必须绿。
func TestIssue1417EachNullableIntColumnIsolated(t *testing.T) {
	cases := []struct {
		label  string
		taskID int64
		// 期望：该档独有的 NULL 列投 0，其余照旧
		wantType, wantPerf, wantState, wantVersion int
		healthy                                    bool // 正向对照：本档没有任何 NULL 数值列
	}{
		{"pd.version NULL（JOIN 档）", n1417taskOrphanVer, 0, 0, 10, 0, false},
		{"pd.version NULL（列档）", n1417taskDefineVer, 0, 0, 10, 0, false},
		{"task_type NULL", n1417taskNullType, 0, 0, 10, 7, false},
		{"perform_type NULL", n1417taskNullPerf, 0, 0, 10, 7, false},
		{"数值列全有值（正向对照）", n1417taskAllValued, 1, 1, 10, 7, true},
	}
	// 逐档 subtest：一档红不遮住另一档的读数（修复前后都要能逐列点名）
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			db := openSQLite129(t)
			defer db.Close()
			seedNullInts1417(t, db)
			repo := jdbc.New(db)
			row := byID1417(t, repo, c.taskID, false)
			if row.TaskType != c.wantType {
				t.Fatalf("%s: taskType 应=%d，实得 %d", c.label, c.wantType, row.TaskType)
			}
			if row.PerformType != c.wantPerf {
				t.Fatalf("%s: performType 应=%d，实得 %d", c.label, c.wantPerf, row.PerformType)
			}
			if int(row.TaskState) != c.wantState {
				t.Fatalf("%s: taskState 应=%d，实得 %d", c.label, c.wantState, row.TaskState)
			}
			if row.DefineVersion != c.wantVersion {
				t.Fatalf("%s: defineVersion 应=%d，实得 %d", c.label, c.wantVersion, row.DefineVersion)
			}
			// 正向对照档额外钉一行：修复不该把有值的行也投成 0
			if c.healthy && row.ProcessDefineName == nil {
				t.Fatalf("正向档 processDefineName 应有值，实得 nil")
			}
		})
	}
}

// TestIssue1417PageTodoTasksNonNullIntSemanticsUnchanged 正向对照：数值列**有值**时读回语义不变
// （task_type=1 / perform_type=1 / pd.version=7 照旧透出），防过度矫正成"全塌 0"。
func TestIssue1417PageTodoTasksNonNullIntSemanticsUnchanged(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullInts1417(t, db)
	repo := jdbc.New(db)

	rows, _, err := repo.PageTodoTasks(context.Background(), q129(), n1417TodoActor)
	if err != nil {
		t.Fatalf("PageTodoTasks: %v", err)
	}
	row := mustHit1417(t, rows, n1417taskAllValued, "正向对照")
	if row.TaskType != 1 {
		t.Fatalf("非空 task_type 应读回 1，实得 %d", row.TaskType)
	}
	if row.PerformType != 1 {
		t.Fatalf("非空 perform_type 应读回 1，实得 %d", row.PerformType)
	}
	if row.TaskState != model.TaskStateDoing {
		t.Fatalf("非空 task_state 应读回 10，实得 %d", row.TaskState)
	}
	if row.DefineVersion != 7 {
		t.Fatalf("非空 pd.version 应读回 7，实得 %d", row.DefineVersion)
	}
	if row.ProcessDefineName == nil || *row.ProcessDefineName != "g1417ok" {
		t.Fatalf("正向行的 processDefineName 应读回 g1417ok，实得 %v", row.ProcessDefineName)
	}
}

// TestIssue1417PageDoneTasksNullableIntColumns 已办分页（pageTasks 的另一条谓词 `task_state <> 10`）
// 同口径：NULL 数值列不报错且投 0。
func TestIssue1417PageDoneTasksNullableIntColumns(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullInts1417(t, db)
	repo := jdbc.New(db)

	rows, _, err := repo.PageDoneTasks(context.Background(), q129(), n1417DoneOpera)
	if err != nil {
		t.Fatalf("PageDoneTasks 含 NULL 数值列的行不应报错: %v", err)
	}
	row := mustHit1417(t, rows, n1417taskDoneNull, "已办 NULL 数值列")
	if row.TaskType != 0 || row.PerformType != 0 {
		t.Fatalf("已办行 NULL 数值列应投 0/0，实得 %d/%d", row.TaskType, row.PerformType)
	}
	if row.DefineVersion != 0 {
		t.Fatalf("已办行孤儿定义的 pd.version 应投 0，实得 %d", row.DefineVersion)
	}
	if row.TaskState != model.TaskStateDone {
		t.Fatalf("已办行 task_state 应读回 20，实得 %d", row.TaskState)
	}
}

// TestIssue1417NullIntColumnsAreNotPointerShape 形状守卫：分页 DTO 的数值列**仍是值类型**。
// G3 的 *string 不外溢到数值家族（owner 裁定）——这一格把"顺手改成 *int64"钉在编译期。
func TestIssue1417NullIntColumnsAreNotPointerShape(t *testing.T) {
	r := model.TaskRow{}
	var _ int = r.TaskType
	var _ int = r.PerformType
	var _ int = r.DefineVersion
	var _ model.TaskState = r.TaskState
	if r.TaskType != 0 || r.DefineVersion != 0 {
		t.Fatalf("零值应为 0，实得 %d/%d", r.TaskType, r.DefineVersion)
	}
}
