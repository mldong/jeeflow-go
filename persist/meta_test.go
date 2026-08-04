// 元数据驱动读写测试（issues/23 阶段①②③，SQLite 内存库全链路）
package persist_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/persist"
)

// ① JSON 配置加载：storageType 名称/数字双解析 + columnName 缺省转下划线
func TestJsonMetaProvider(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "biz_leave.json"), []byte(`{
		"tableName": "biz_leave",
		"primaryKey": "id",
		"fields": [
			{"name": "companyName", "columnName": "company_name", "storageType": "NORMAL"},
			{"name": "address", "storageType": 2, "expandFields": {"province": "province", "city": "city"}},
			{"name": "extra", "storageType": "JSON"},
			{"name": "items", "storageType": 5, "targetTable": "biz_leave_item", "foreignKey": "leave_id"}
		]
	}`), 0644)
	p := persist.NewJsonMetaProvider(dir)
	meta := p.LoadTableMeta("biz_leave")
	if meta == nil {
		t.Fatal("meta should load")
	}
	if meta.FindField("companyName").Column() != "company_name" {
		t.Fatalf("columnName mismatch: %s", meta.FindField("companyName").Column())
	}
	if meta.FindField("address").StorageType != persist.StorageExpand {
		t.Fatal("expand storageType mismatch")
	}
	if meta.FindField("items").StorageType != persist.StorageOne2Many {
		t.Fatal("one2many storageType mismatch")
	}
	if meta.FindField("items").TargetTable != "biz_leave_item" {
		t.Fatal("targetTable mismatch")
	}
	if p.LoadTableMeta("no_such") != nil {
		t.Fatal("undefined table should return nil")
	}
}

// ② 全链路：NORMAL/JSON/EXPAND + ONE2ONE/ONE2MANY 子表写入与回显组装
func TestMetaTableWriterReader(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE biz_leave (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_name TEXT, amount REAL,
		extra TEXT, province TEXT, city TEXT, detail_addr TEXT,
		process_instance_id INTEGER
	)`)
	db.Exec(`CREATE TABLE biz_leave_address (
		id INTEGER PRIMARY KEY AUTOINCREMENT, leave_id INTEGER,
		province TEXT, city TEXT, detail_addr TEXT
	)`)
	db.Exec(`CREATE TABLE biz_leave_item (
		id INTEGER PRIMARY KEY AUTOINCREMENT, leave_id INTEGER,
		name TEXT, qty INTEGER
	)`)

	provider := &mapMetaProvider{metas: map[string]*persist.TableMeta{
		"biz_leave": {
			TableName: "biz_leave", PrimaryKey: "id",
			Fields: []persist.FieldMeta{
				{Name: "companyName"},
				{Name: "amount"},
				{Name: "extra", StorageType: persist.StorageJSON},
				{Name: "address", StorageType: persist.StorageExpand,
					ExpandFields: map[string]string{"province": "province", "city": "city", "detail": "detail_addr"}},
				{Name: "addressRel", StorageType: persist.StorageOne2One,
					TargetTable: "biz_leave_address", ForeignKey: "leave_id"},
				{Name: "items", StorageType: persist.StorageOne2Many,
					TargetTable: "biz_leave_item", ForeignKey: "leave_id"},
			},
		},
		"biz_leave_address": {
			TableName: "biz_leave_address", PrimaryKey: "id",
			Fields: []persist.FieldMeta{{Name: "province"}, {Name: "city"}, {Name: "detail", ColumnName: "detail_addr"}},
		},
		"biz_leave_item": {
			TableName: "biz_leave_item", PrimaryKey: "id",
			Fields: []persist.FieldMeta{{Name: "name"}, {Name: "qty"}},
		},
	}}

	base := persist.NewJdbcDynamicTableWriter(db)
	writer := persist.NewMetaTableWriter(base, provider)
	reader := persist.NewMetaTableReader(persist.NewJdbcTableReader(db), provider)

	_, err = writer.Insert("biz_leave", map[string]interface{}{
		"companyName":   "复杂公司",
		"amount":        800,
		"extra":         map[string]interface{}{"tag": "vip", "level": 3},
		"address":       map[string]interface{}{"province": "广东省", "city": "深圳市", "detail": "科技园路1号"},
		"addressRel":    map[string]interface{}{"province": "广东省", "city": "广州市", "detail": "天河区"},
		"items": []interface{}{
			map[string]interface{}{"name": "电脑", "qty": 2},
			map[string]interface{}{"name": "键盘", "qty": 3},
		},
		"process_instance_id": int64(888),
	})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// 落库断言
	var province, city string
	var itemCount int
	if err := db.QueryRow("SELECT province, city FROM biz_leave").Scan(&province, &city); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if province != "广东省" || city != "深圳市" {
		t.Fatalf("expand mismatch: %s/%s", province, city)
	}
	if err := db.QueryRow("SELECT COUNT(1) FROM biz_leave_item").Scan(&itemCount); err != nil || itemCount != 2 {
		t.Fatalf("one2many count mismatch: %d err=%v", itemCount, err)
	}

	// 回显组装
	result, err := reader.ReadByProcessInstance("biz_leave", int64(888))
	if err != nil || result == nil {
		t.Fatalf("read failed: %v", err)
	}
	if result["companyName"] != "复杂公司" {
		t.Fatalf("normal mismatch: %v", result["companyName"])
	}
	extra, ok := result["extra"].(map[string]interface{})
	if !ok || extra["tag"] != "vip" {
		t.Fatalf("json mismatch: %v", result["extra"])
	}
	addr, ok := result["address"].(map[string]interface{})
	if !ok || addr["city"] != "深圳市" {
		t.Fatalf("expand read mismatch: %v", result["address"])
	}
	rel, ok := result["addressRel"].(map[string]interface{})
	if !ok || rel["city"] != "广州市" {
		t.Fatalf("one2one read mismatch: %v", result["addressRel"])
	}
	items, ok := result["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("one2many read mismatch: %v", result["items"])
	}
	first := items[0].(map[string]interface{})
	if first["name"] != "电脑" || first["qty"] != int64(2) {
		t.Fatalf("one2many item mismatch: %v", first)
	}
	if result["process_instance_id"] != int64(888) {
		t.Fatalf("unconsumed column mismatch: %v", result["process_instance_id"])
	}
}

// ③ 无元数据回落：委托基础 writer + 原始行回显
func TestMetaFallback(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE biz_leave (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, process_instance_id INTEGER)`)
	base := persist.NewJdbcDynamicTableWriter(db)
	provider := &mapMetaProvider{metas: map[string]*persist.TableMeta{}}
	writer := persist.NewMetaTableWriter(base, provider)
	reader := persist.NewMetaTableReader(persist.NewJdbcTableReader(db), provider)
	if _, err := writer.Insert("biz_leave", map[string]interface{}{"title": "回落", "process_instance_id": int64(1)}); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	result, err := reader.ReadByProcessInstance("biz_leave", int64(1))
	if err != nil || result == nil {
		t.Fatalf("read failed: %v", err)
	}
	if result["title"] != "回落" {
		t.Fatalf("fallback mismatch: %v", result)
	}
}

type mapMetaProvider struct {
	metas map[string]*persist.TableMeta
}

func (p *mapMetaProvider) LoadTableMeta(tableName string) *persist.TableMeta {
	return p.metas[tableName]
}

// ⑧ issues/24：ONE2MANY 子表递归继承主表 apply_user_id（BIGINT create_user 不回落 "system"）
// + EXPAND 列不重复平铺带出 + MetaTableWriter.Update 组装
func TestSubTableUserPropagationAndUpdate(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE biz_parent (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		title TEXT, apply_user_id INTEGER, create_user INTEGER, finish INTEGER, process_instance_id INTEGER,
		province TEXT, city TEXT, is_deleted INTEGER
	)`)
	db.Exec(`CREATE TABLE biz_child (
		id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER,
		item_name TEXT, create_user INTEGER, update_user INTEGER, is_deleted INTEGER
	)`)

	provider := &mapMetaProvider{metas: map[string]*persist.TableMeta{
		"biz_parent": {
			TableName: "biz_parent", PrimaryKey: "id",
			Fields: []persist.FieldMeta{
				{Name: "title"},
				{Name: "address", StorageType: persist.StorageExpand,
					ExpandFields: map[string]string{"province": "province", "city": "city"}},
				{Name: "items", StorageType: persist.StorageOne2Many,
					TargetTable: "biz_child", ForeignKey: "parent_id"},
			},
		},
		"biz_child": {
			TableName: "biz_child", PrimaryKey: "id",
			Fields: []persist.FieldMeta{{Name: "itemName"}},
		},
	}}
	base := persist.NewJdbcDynamicTableWriter(db)
	writer := persist.NewMetaTableWriter(base, provider)
	reader := persist.NewMetaTableReader(persist.NewJdbcTableReader(db), provider)

	operator := int64(1567738052492341249)
	pk, err := writer.Insert("biz_parent", map[string]interface{}{
		"title": "传播测试", "apply_user_id": operator,
		"address": map[string]interface{}{"province": "广东省", "city": "深圳市"},
		"items":   []interface{}{map[string]interface{}{"itemName": "测试项目A"}},
		"process_instance_id": int64(999),
	})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	// 子表 create_user = operator（不回落 "system"）
	var childUser int64
	if err := db.QueryRow("SELECT create_user FROM biz_child WHERE parent_id = ?", pk).Scan(&childUser); err != nil {
		t.Fatalf("query child failed: %v", err)
	}
	if childUser != operator {
		t.Fatalf("child create_user should be operator: %d", childUser)
	}
	// 主表 create_user 同 operator
	var parentUser int64
	if err := db.QueryRow("SELECT create_user FROM biz_parent WHERE id = ?", pk).Scan(&parentUser); err != nil {
		t.Fatalf("query parent failed: %v", err)
	}
	if parentUser != operator {
		t.Fatalf("parent create_user should be operator: %d", parentUser)
	}
	// Update：EXPAND 展开列 + 状态字段直通（子表不参与中途更新）
	if _, err := writer.Update("biz_parent", map[string]interface{}{
		"address": map[string]interface{}{"province": "北京市", "city": "海淀区"},
		"finish":  20,
	}, "process_instance_id", int64(999)); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	var province, city string
	var finish int
	if err := db.QueryRow("SELECT province, city, finish FROM biz_parent WHERE id = ?", pk).
		Scan(&province, &city, &finish); err != nil {
		t.Fatalf("query after update failed: %v", err)
	}
	if province != "北京市" || city != "海淀区" {
		t.Fatalf("expand update mismatch: %s %s", province, city)
	}
	if finish != 20 {
		t.Fatalf("state update mismatch: %d", finish)
	}
	// 读侧：EXPAND 展开列不重复平铺带出（对象形式已消费，issues/24）
	result, err := reader.ReadByProcessInstance("biz_parent", int64(999))
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if _, ok := result["province"]; ok {
		t.Fatalf("expand column should not be flat: %v", result)
	}
	addr, _ := result["address"].(map[string]interface{})
	if addr == nil || addr["city"] != "海淀区" {
		t.Fatalf("address object mismatch: %v", result["address"])
	}
}
