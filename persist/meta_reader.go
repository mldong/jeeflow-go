// 元数据驱动的动态读取引擎（issues/23 读侧最小闭环）——与写入侧 MetaTableWriter 职责分离
package persist

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// JdbcTableReader 业务表查询器（读侧底层）——按列等值查询原始行。
// 与写入器分离：JdbcDynamicTableWriter 保持纯写职责。
type JdbcTableReader struct {
	db *sql.DB
}

// NewJdbcTableReader 创建查询器
func NewJdbcTableReader(db *sql.DB) *JdbcTableReader {
	return &JdbcTableReader{db: db}
}

// QueryFirst 按指定列等值查询首行（列名→值，列名为数据库 label）
func (r *JdbcTableReader) QueryFirst(tableName, whereColumn string, value interface{}) (map[string]interface{}, error) {
	rows, err := r.QueryList(tableName, whereColumn, value, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// QueryList 按指定列等值查询列表（limit 0=不限制）
func (r *JdbcTableReader) QueryList(tableName, whereColumn string, value interface{}, limit int) ([]map[string]interface{}, error) {
	if err := checkTableName(tableName); err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, whereColumn)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := r.db.Query(query, value)
	if err != nil {
		return nil, fmt.Errorf("persist: query %s failed: %w", tableName, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			row[c] = decodeDriverValue(vals[i])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// ─── MetaTableReader ────────────────────────────────────────────────────────────

// MetaTableReader 元数据驱动的动态读取器——按流程实例回显业务数据。
// 边界（不做）：通用条件分页 / 动态条件语法 / 数据权限 / 排序。
type MetaTableReader struct {
	Reader   *JdbcTableReader
	Provider IDynamicMetaProvider
}

// NewMetaTableReader 创建读取器
func NewMetaTableReader(reader *JdbcTableReader, provider IDynamicMetaProvider) *MetaTableReader {
	return &MetaTableReader{Reader: reader, Provider: provider}
}

// ReadByProcessInstance 按 relTableName + process_instance_id 回显主表单条，
// 按 TableMeta.storageType 反序列化（子表为对象/数组）；无记录返回 nil；无元数据回落原始行。
func (r *MetaTableReader) ReadByProcessInstance(tableName string, processInstanceId interface{}) (map[string]interface{}, error) {
	row, err := r.Reader.QueryFirst(tableName, "process_instance_id", processInstanceId)
	if err != nil || row == nil {
		return row, err
	}
	meta := r.Provider.LoadTableMeta(tableName)
	if meta == nil {
		return row, nil // 无元数据：原样返回（列名→值）
	}
	return r.Assemble(meta, row)
}

// Assemble 按元数据组装回显结果（字段名 → 值）
func (r *MetaTableReader) Assemble(meta *TableMeta, row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, f := range meta.Fields {
		v := findRowValue(row, f.Column())
		switch f.StorageType {
		case StorageJSON:
			if v != nil {
				var obj interface{}
				if err := json.Unmarshal([]byte(fmt.Sprintf("%v", v)), &obj); err == nil {
					result[f.Name] = obj
				} else {
					result[f.Name] = v
				}
			}
		case StorageExpand:
			if obj := expandFrom(row, f); obj != nil {
				result[f.Name] = obj
			}
		case StorageOne2One, StorageOne2Many:
			sub, err := r.readSubTable(meta, &f, row)
			if err != nil {
				return nil, err
			}
			if sub != nil {
				result[f.Name] = sub
			}
		default:
			if v != nil {
				result[f.Name] = v
			}
		}
	}
	// 未在元数据中的列（process_instance_id/apply_user_id/系统字段）带出（key 统一小写）；
	// EXPAND 展开列（挂在某字段 expandFields 映射里，对象形式已带出）不重复平铺（issues/24）
	for k, v := range row {
		if meta.FindFieldByColumn(k) == nil && !meta.isExpandColumn(k) {
			if _, ok := result[strings.ToLower(k)]; !ok {
				result[strings.ToLower(k)] = v
			}
		}
	}
	return result, nil
}

// isExpandColumn 判断列是否为某字段的 EXPAND 展开列（issues/24：已消费，不重复平铺带出）
func (m *TableMeta) isExpandColumn(column string) bool {
	for _, f := range m.Fields {
		for _, col := range f.ExpandFields {
			if strings.EqualFold(col, column) {
				return true
			}
		}
	}
	return false
}

// expandFrom EXPAND 反展开：多列 → 对象
func expandFrom(row map[string]interface{}, f FieldMeta) map[string]interface{} {
	obj := make(map[string]interface{})
	for sub, col := range f.ExpandFields {
		if v := findRowValue(row, col); v != nil {
			obj[sub] = v
		}
	}
	if len(obj) == 0 {
		return nil
	}
	return obj
}

// readSubTable ONE2ONE/ONE2MANY 子表读取（外键=主表主键，递归按子表元数据组装）
func (r *MetaTableReader) readSubTable(parentMeta *TableMeta, f *FieldMeta, row map[string]interface{}) (interface{}, error) {
	parentPk := findRowValue(row, parentMeta.PK())
	if parentPk == nil {
		return nil, nil
	}
	fk := f.ForeignKey
	if fk == "" {
		fk = parentMeta.PK()
	}
	subMeta := r.Provider.LoadTableMeta(f.TargetTable)
	if f.StorageType == StorageOne2One {
		sub, err := r.Reader.QueryFirst(f.TargetTable, fk, parentPk)
		if err != nil || sub == nil {
			return sub, err
		}
		if subMeta != nil {
			return r.Assemble(subMeta, sub)
		}
		return sub, nil
	}
	// ONE2MANY
	subs, err := r.Reader.QueryList(f.TargetTable, fk, parentPk, 0)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(subs))
	for _, sub := range subs {
		if subMeta != nil {
			assembled, err := r.Assemble(subMeta, sub)
			if err != nil {
				return nil, err
			}
			result = append(result, assembled)
		} else {
			result = append(result, sub)
		}
	}
	return result, nil
}

// decodeDriverValue 把 database/sql Scan 进 interface{} 的 []byte（MySQL VARCHAR/DECIMAL）
// 转成 string，避免 encoding/json 编成 Base64（issues/65）。
// 扫描 NULL 是无类型 nil，原样返回；[]byte(nil) 也当 nil。
func decodeDriverValue(v interface{}) interface{} {
	b, ok := v.([]byte)
	if !ok {
		return v
	}
	if b == nil {
		return nil
	}
	return string(b)
}
