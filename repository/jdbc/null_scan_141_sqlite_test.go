// issues/141 G3 取证（T0，sqlite 内存库，不连 MySQL/PG，不碰 160）。
//
// 缺陷形状：repository/jdbc 的分页查询把可空字符串列（wf_process_task.operator / form_key、
// wf_process_instance.operator / business_no、wf_process_define.type，以及 LEFT JOIN wf_process_define
// 带出的 name/display_name——定义缺失即 NULL）扫进**非指针 string**。database/sql 对 NULL → string
// 直接报 `converting NULL to string is unsupported`，整条查询失败。
// 实测症状是 TestTransfer* 两格「整包跑红、单跑绿」——共享库混进别的栈写的 NULL 脏行 × 本栈裸 string 扫描。
//
// 本测试用**自己的 id 段（91410xxx）**在 sqlite 上手工造 NULL 行，复现并锁死修复：
//   1. 修复前：PageTodoTasks / PageInstances / PageCcInstances / PageDefines 对含 NULL 可空列的行返回 err
//      （rows.Scan 报错）→ 断言 err==nil 会红。
//   2. 修复后：查询不报错，且可空列的**出口形状**是 nil（→ JSON null），
//      既不是空串 ""（spec 06 §2.4 判红形状），也不是 Go 零值串化 "<nil>"。
//   3. 非空值读回语义不变（DONE 任务 operator/form_key 有值时照旧透出原串），防过度矫正。
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
)

// 本案专用 id 段（避开 seed129 的 1..4，也避开其他栈在共享库的段）
const (
	n141defineNull   = int64(91410001) // type NULL → PageDefines 扫 type
	n141defineValue  = int64(91410002) // type 'approval' 正向对照
	n141instNull     = int64(91410111) // operator/business_no 有值：供待办 LEFT JOIN，业务号正向
	n141instNullCols = int64(91410112) // operator=NULL, business_no=NULL：PageInstances/PageCc 出口
	n141taskTodoNull = int64(91410121) // DOING, operator=NULL, form_key=NULL → PageTodoTasks 扫（缺陷主证）
	n141taskDoneVal  = int64(91410122) // DONE, operator='d141', form_key='fkX' → PageDoneTasks 正向对照
	n141actorTodo    = int64(91410131)
	n141ccNullCols   = int64(91410141)
)

func exec141(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("seed %q: %v", q, err)
	}
}

// seedNullColumns141 造含 NULL 可空列的行（NULL 是本缺陷触发点；列名对齐 openSQLite129 建表）。
func seedNullColumns141(t *testing.T, db *sql.DB) {
	t.Helper()
	// 两个定义：一个 type NULL（PageDefines 扫可空列），一个 type 有值（正向对照）
	exec141(t, db, `INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (?,?,?,?,1,1)`,
		n141defineNull, "g141null", "141可空类型流程", nil)
	exec141(t, db, `INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (?,?,?,?,1,1)`,
		n141defineValue, "g141val", "141有类型流程", "approval")

	// 实例：9111 operator/business_no 有值（给待办 LEFT JOIN 用）；9112 两列均 NULL
	exec141(t, db, `INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,'B141','instOwner','{}')`, n141instNull, n141defineValue)
	exec141(t, db, `INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,NULL,NULL,'{}')`, n141instNullCols, n141defineValue)

	// 待办任务：DOING(task_state=10)，operator=NULL、form_key=NULL
	// → PageTodoTasks 谓词是 pta.actor_id=?，不受 operator 过滤影响 ⇒ 该行被返回 ⇒ 修复前 rows.Scan 炸
	exec141(t, db, `INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,operator,form_key,variable)
		VALUES (?,?,?,?,0,0,10,NULL,NULL,'{}')`, n141taskTodoNull, n141instNull, "task1", "上级审批")
	exec141(t, db, `INSERT INTO wf_process_task_actor (id,process_task_id,actor_id) VALUES (?,?,?)`,
		n141actorTodo, n141taskTodoNull, "n141")

	// 已办任务：DONE(20)，operator/form_key 均**有值**（正向对照，证明读回语义未变）
	exec141(t, db, `INSERT INTO wf_process_task (id,process_instance_id,task_name,display_name,task_type,perform_type,task_state,operator,form_key,variable)
		VALUES (?,?,?,?,0,0,20,'d141','fkX','{}')`, n141taskDoneVal, n141instNull, "task2", "已办节点")

	// 抄送：cc.actor_id='c141' 指向实例 9112（operator/business_no 均 NULL）→ PageCcInstances 扫可空列
	exec141(t, db, `INSERT INTO wf_process_cc_instance (id,process_instance_id,actor_id,state) VALUES (?,?,?,0)`,
		n141ccNullCols, n141instNullCols, "c141")
}

func findTask141(rows []*model.TaskRow, id int64) *model.TaskRow {
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func findInstance141(rows []*model.InstanceRow, id int64) *model.InstanceRow {
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func findDefine141(rows []*model.DefineRow, id int64) *model.DefineRow {
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// TestIssue141PageTodoTasksNullableOperatorFormKey 主证：待办行 operator/form_key 为 NULL 时
// 查询不报错，且可空列出 **null 出口**（nil 指针，非 ""/非 "<nil>"）。修复前 rows.Scan 报错 → 红。
func TestIssue141PageTodoTasksNullableOperatorFormKey(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullColumns141(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	rows, _, err := repo.PageTodoTasks(ctx, q129(), "n141")
	if err != nil {
		t.Fatalf("PageTodoTasks 含 NULL 可空列的行不应报错（修复前此处必为 converting NULL to string is unsupported）: %v", err)
	}
	row := findTask141(rows, n141taskTodoNull)
	if row == nil {
		t.Fatalf("夹具失效：待办应命中 task %d, got %d 行", n141taskTodoNull, len(rows))
	}
	if row.Operator != nil {
		t.Fatalf("可空列 operator=NULL 应投成 nil（JSON null），实得 %q ⇒ 违规形状", *row.Operator)
	}
	if row.FormKey != nil {
		t.Fatalf("可空列 form_key=NULL 应投成 nil（JSON null），实得 %q ⇒ 违规形状", *row.FormKey)
	}
	// 出口 JSON 形状锁死：null，不是空串 ""
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"operator":null`) || !strings.Contains(s, `"formKey":null`) {
		t.Fatalf(`JSON 出口应为 "operator":null / "formKey":null，实得 %s`, s)
	}
	if strings.Contains(s, `"operator":""`) || strings.Contains(s, `"formKey":""`) || strings.Contains(s, `<nil>`) {
		t.Fatalf(`JSON 出口不得把 NULL 伪造成 "" 或 "<nil>"，实得 %s`, s)
	}
}

// TestIssue141PageDoneTasksNonNullSemanticsUnchanged 正向对照：可空列**有值**时读回语义不变（原串透出）。
func TestIssue141PageDoneTasksNonNullSemanticsUnchanged(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullColumns141(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	rows, _, err := repo.PageDoneTasks(ctx, q129(), "d141")
	if err != nil {
		t.Fatalf("PageDoneTasks: %v", err)
	}
	row := findTask141(rows, n141taskDoneVal)
	if row == nil {
		t.Fatalf("夹具失效：已办应命中 task %d", n141taskDoneVal)
	}
	if row.Operator == nil || *row.Operator != "d141" {
		t.Fatalf("非空 operator 读回应为 d141，实得 %v", row.Operator)
	}
	if row.FormKey == nil || *row.FormKey != "fkX" {
		t.Fatalf("非空 form_key 读回应为 fkX，实得 %v", row.FormKey)
	}
}

// TestIssue141PageInstancesNullableBusinessNo 我发起的列表：business_no=NULL 不报错且投 null（issues/46 遗留补齐）。
func TestIssue141PageInstancesNullableBusinessNo(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullColumns141(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	// 实例 9112 operator/business_no 均 NULL，但 PageInstances 谓词是 t.operator=? ⇒ NULL 不匹配任何入参，
	// 拿不到该行的可空投影。改用 operator 有值、business_no=NULL 的夹具变体：直接补一行。
	exec141(t, db, `INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		VALUES (?,?,10,NULL,'bnOwner','{}')`, int64(91410113), n141defineValue)
	rows, _, err := repo.PageInstances(ctx, q129(), "bnOwner")
	if err != nil {
		t.Fatalf("PageInstances 含 NULL business_no 的行不应报错: %v", err)
	}
	row := findInstance141(rows, 91410113)
	if row == nil {
		t.Fatalf("夹具失效：PageInstances(bnOwner) 应命中实例 91410113")
	}
	if row.BusinessNo != nil {
		t.Fatalf("business_no=NULL 应投 nil，实得 %q", *row.BusinessNo)
	}
	if row.Operator == nil || *row.Operator != "bnOwner" {
		t.Fatalf("非空 operator 读回语义应变：实得 %v", row.Operator)
	}
}

// TestIssue141PageCcInstancesNullableColumns 抄送列表：命中 cc.actor_id 后带出的实例 operator/business_no
// 为 NULL ⇒ 不报错且投 null（覆盖 CcInstanceRow 同口径）。
func TestIssue141PageCcInstancesNullableColumns(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullColumns141(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	rows, _, err := repo.PageCcInstances(ctx, q129(), "c141")
	if err != nil {
		t.Fatalf("PageCcInstances 含 NULL 可空列的行不应报错: %v", err)
	}
	var hit *model.CcInstanceRow
	for _, r := range rows {
		if r.ID == n141instNullCols {
			hit = r
			break
		}
	}
	if hit == nil {
		t.Fatalf("夹具失效：ccList(c141) 应命中实例 %d, got %d 行", n141instNullCols, len(rows))
	}
	if hit.Operator != nil {
		t.Fatalf("cc 行 operator=NULL 应投 nil，实得 %q", *hit.Operator)
	}
	if hit.BusinessNo != nil {
		t.Fatalf("cc 行 business_no=NULL 应投 nil，实得 %q", *hit.BusinessNo)
	}
}

// TestIssue141PageDefinesNullableType 定义分页：type=NULL 不报错且投 null，有值时原样透出（DefineRow 同口径）。
func TestIssue141PageDefinesNullableType(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedNullColumns141(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()

	rows, _, err := repo.PageDefines(ctx, q129())
	if err != nil {
		t.Fatalf("PageDefines 含 NULL type 的行不应报错: %v", err)
	}
	nullRow := findDefine141(rows, n141defineNull)
	valRow := findDefine141(rows, n141defineValue)
	if nullRow == nil || valRow == nil {
		t.Fatalf("夹具失效：PageDefines 应同时命中两个定义, got %d 行", len(rows))
	}
	if nullRow.Type != nil {
		t.Fatalf("type=NULL 应投 nil，实得 %q", *nullRow.Type)
	}
	if valRow.Type == nil || *valRow.Type != "approval" {
		t.Fatalf("非空 type 读回语义应变：实得 %v", valRow.Type)
	}
}
