package persist_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/persist"
)

// 建业务表（与 Java 集成测试同构）
func setupDB(t *testing.T) (*sql.DB, *persist.JdbcDynamicTableWriter) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE biz_leave (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT,
		amount REAL,
		process_instance_id INTEGER,
		apply_user_id TEXT,
		apply_dept_id TEXT,
		create_time TEXT,
		create_user TEXT,
		update_time TEXT,
		update_user TEXT,
		is_deleted INTEGER
	)`)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	w := persist.NewJdbcDynamicTableWriter(db)
	return db, w
}

// ① 全字段插入：f_ 去前缀数据 + 系统字段 + 流程上下文
func TestInsertFull(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	data := map[string]interface{}{
		"title":              "年假申请",
		"amount":             800.0,
		"process_instance_id": int64(1),
		"apply_user_id":      "user1",
		"apply_dept_id":      "D01",
	}
	w.FillSystemFields(data, true)
	_, err := w.Insert("biz_leave", data)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	var title, createUser string
	var pi int64
	var deleted int
	err = db.QueryRow("SELECT title, process_instance_id, create_user, is_deleted FROM biz_leave").Scan(&title, &pi, &createUser, &deleted)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if title != "年假申请" || pi != 1 || createUser != "user1" || deleted != 0 {
		t.Fatalf("row mismatch: title=%s pi=%d createUser=%s deleted=%d", title, pi, createUser, deleted)
	}
}

// ② 缺列过滤：data 含目标表没有的列 → 自动过滤
func TestFilterColumns(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	kept, err := w.FilterColumns("biz_leave", []string{"title", "no_such_col", "amount"})
	if err != nil {
		t.Fatalf("filter failed: %v", err)
	}
	if len(kept) != 2 || kept[0] != "title" || kept[1] != "amount" {
		t.Fatalf("filter result mismatch: %v", kept)
	}
	// Insert 内部同样过滤
	_, err = w.Insert("biz_leave", map[string]interface{}{"title": "t", "ghost_col": "x"})
	if err != nil {
		t.Fatalf("insert with extra col failed: %v", err)
	}
}

// ③ 类型 null：值为 nil 的字段正常入库
func TestInsertNilValue(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	_, err := w.Insert("biz_leave", map[string]interface{}{"title": "t", "amount": nil, "apply_dept_id": nil})
	if err != nil {
		t.Fatalf("insert nil failed: %v", err)
	}
}

// ④ 防注入：值含 SQL 片段不生效
func TestInjectSafe(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	_, err := w.Insert("biz_leave", map[string]interface{}{"title": "x'); DROP TABLE biz_leave; --"})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(1) FROM biz_leave").Scan(&n); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("table corrupted, rows=%d", n)
	}
}

// ⑤ sys_ 前缀拒绝（安全）
func TestSysPrefixRejected(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	if _, err := w.Insert("sys_user", map[string]interface{}{"x": 1}); err == nil {
		t.Fatal("sys_ prefix should be rejected")
	}
	if _, err := w.FilterColumns("sys_user", []string{"x"}); err == nil {
		t.Fatal("sys_ prefix should be rejected")
	}
}

// ⑥ 非法字符拒绝（表名注入）
func TestIllegalTableName(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	if _, err := w.Insert("biz_leave; DROP TABLE biz_leave", map[string]interface{}{"x": 1}); err == nil {
		t.Fatal("illegal table name should be rejected")
	}
}

// ⑦ 幂等：exists 先查后插，重复键不重复
func TestExistsIdempotent(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	_, err := w.Insert("biz_leave", map[string]interface{}{"title": "t", "process_instance_id": int64(99)})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	ok, err := w.Exists("biz_leave", "process_instance_id", int64(99))
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if !ok {
		t.Fatal("exists should be true")
	}
	ok, _ = w.Exists("biz_leave", "process_instance_id", int64(100))
	if ok {
		t.Fatal("exists should be false for other key")
	}
}

// ⑧ 系统字段填充：insert=true 填 create/update/is_deleted；false 只填 update；
// 用户列默认值优先 apply_user_id（issues/19）
func TestFillSystemFields(t *testing.T) {
	db, w := setupDB(t)
	defer db.Close()
	data := map[string]interface{}{"title": "t"}
	w.FillSystemFields(data, true)
	if data["create_user"] != "system" || data["is_deleted"] != 0 {
		t.Fatalf("insert fill mismatch: %v", data)
	}
	if _, ok := data["create_time"]; !ok {
		t.Fatal("create_time should be filled")
	}
	if !strings.Contains(data["create_time"].(string), "-") {
		t.Fatalf("create_time format unexpected: %v", data["create_time"])
	}
	// 禁用列
	w.CreateTimeColumn = ""
	w2data := map[string]interface{}{"title": "t"}
	w.FillSystemFields(w2data, true)
	if _, ok := w2data["create_time"]; ok {
		t.Fatal("disabled column should not be filled")
	}
	// issues/19：data 已注入 apply_user_id（拦截器场景）→ 用户列取 operator
	w3data := map[string]interface{}{"title": "t", "apply_user_id": "123"}
	w.FillSystemFields(w3data, true)
	if w3data["create_user"] != "123" || w3data["update_user"] != "123" {
		t.Fatalf("user column should use operator: %v", w3data)
	}
	// 可配置默认值回落
	w.DefaultUserValue = 0
	w4data := map[string]interface{}{"title": "t"}
	w.FillSystemFields(w4data, true)
	if w4data["create_user"] != 0 {
		t.Fatalf("configured default user expected: %v", w4data)
	}
}
