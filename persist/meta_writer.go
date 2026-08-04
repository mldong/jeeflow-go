// 元数据驱动的动态写入引擎（issues/23 阶段②③）——DynamicTableWriter 的增强实现（纯写职责）
package persist

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MetaTableWriter 元数据驱动的动态写入器——按 TableMeta.storageType 语义执行插入。
// 无元数据时完全委托基础 writer（回落现状，零破坏）。读侧由 MetaTableReader 提供。
type MetaTableWriter struct {
	Base     DynamicTableWriter
	Provider IDynamicMetaProvider
}

// NewMetaTableWriter 创建元数据驱动写入器
func NewMetaTableWriter(base DynamicTableWriter, provider IDynamicMetaProvider) *MetaTableWriter {
	return &MetaTableWriter{Base: base, Provider: provider}
}

// FilterColumns 委托基础 writer
func (w *MetaTableWriter) FilterColumns(tableName string, columns []string) ([]string, error) {
	return w.Base.FilterColumns(tableName, columns)
}

// Insert 按元数据 storageType 分派插入；子表递归（外键=主表主键）
func (w *MetaTableWriter) Insert(tableName string, data map[string]interface{}) (interface{}, error) {
	meta := w.Provider.LoadTableMeta(tableName)
	if meta == nil {
		return w.Base.Insert(tableName, data) // 无元数据：回落现状
	}
	// 子表数据先收集（主表插入后处理，外键=主表主键）
	subData := make(map[string]interface{})
	row := make(map[string]interface{})
	for _, f := range meta.Fields {
		v, ok := data[f.Name]
		if !ok || v == nil {
			continue
		}
		switch f.StorageType {
		case StorageJSON:
			bs, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("persist: json marshal %s failed: %w", f.Name, err)
			}
			row[f.Column()] = string(bs)
		case StorageExpand:
			expandInto(f, v, row)
		case StorageOne2One, StorageOne2Many:
			subData[f.Name] = v
		default:
			row[f.Column()] = v
		}
	}
	// 未消费字段（流程上下文 process_instance_id 等）直通基础 writer
	for k, v := range data {
		if meta.FindField(k) == nil {
			if _, ok := row[k]; !ok {
				row[k] = v
			}
		}
	}
	w.Base.FillSystemFields(row, true)
	pk, err := w.Base.Insert(tableName, row) // 主表插入（自增/生成器返回主键）
	if err != nil {
		return nil, err
	}
	if pk == nil {
		pk = findRowValue(row, meta.PK()) // 兜底：data 显式主键
	}
	// 子表递归插入
	for name, v := range subData {
		f := meta.FindField(name)
		if err := w.insertSubTable(meta, f, v, pk, data); err != nil {
			return nil, err
		}
	}
	return pk, nil
}

// Update 按元数据 storageType 组装 SET 列（SYNC 同步演进，issues/24）——
// NORMAL/JSON/EXPAND 参与更新；ONE2ONE/ONE2MANY 子表不参与中途更新
// （任务推进只更新主表行状态，子表数据变动走重新提交）；未消费字段直通。
func (w *MetaTableWriter) Update(tableName string, data map[string]interface{}, whereColumn string, whereValue interface{}) (int64, error) {
	meta := w.Provider.LoadTableMeta(tableName)
	if meta == nil {
		return w.Base.Update(tableName, data, whereColumn, whereValue) // 无元数据：回落基础 writer
	}
	row := make(map[string]interface{})
	for _, f := range meta.Fields {
		v, ok := data[f.Name]
		if !ok || v == nil {
			continue
		}
		switch f.StorageType {
		case StorageJSON:
			bs, err := json.Marshal(v)
			if err != nil {
				return 0, fmt.Errorf("persist: json marshal %s failed: %w", f.Name, err)
			}
			row[f.Column()] = string(bs)
		case StorageExpand:
			expandInto(f, v, row)
		case StorageOne2One, StorageOne2Many:
			continue // 子表不参与中途更新
		default:
			row[f.Column()] = v
		}
	}
	// 未消费字段（流程上下文/状态字段等）直通基础 writer
	for k, v := range data {
		if meta.FindField(k) == nil {
			if _, ok := row[k]; !ok {
				row[k] = v
			}
		}
	}
	return w.Base.Update(tableName, row, whereColumn, whereValue)
}

// Exists 委托基础 writer（幂等语义不变）
func (w *MetaTableWriter) Exists(tableName, bizKey string, bizKeyValue interface{}) (bool, error) {
	return w.Base.Exists(tableName, bizKey, bizKeyValue)
}

// FillSystemFields 委托基础 writer
func (w *MetaTableWriter) FillSystemFields(data map[string]interface{}, isInsert bool) {
	w.Base.FillSystemFields(data, isInsert)
}

// ─── 内部 ───────────────────────────────────────────────────────────────────────

// expandInto EXPAND：对象字段展开为多列（子字段名 → 表列名）
func expandInto(f FieldMeta, v interface{}, row map[string]interface{}) {
	obj, ok := v.(map[string]interface{})
	if !ok {
		return
	}
	for sub, col := range f.ExpandFields {
		if fv, ok := obj[sub]; ok && fv != nil {
			row[col] = fv
		}
	}
}

// insertSubTable ONE2ONE/ONE2MANY：子表递归插入（外键=主表主键）
func (w *MetaTableWriter) insertSubTable(parentMeta *TableMeta, f *FieldMeta, v interface{}, parentPk interface{}, parentData map[string]interface{}) error {
	if parentPk == nil {
		return fmt.Errorf("persist: parent primary key missing, cannot insert sub table %s", f.Name)
	}
	fk := f.ForeignKey
	if fk == "" {
		fk = parentMeta.PK()
	}
	if f.StorageType == StorageOne2One {
		if m, ok := v.(map[string]interface{}); ok {
			return w.insertSubRow(f, m, fk, parentPk, parentData)
		}
		return nil
	}
	// ONE2MANY
	if items, ok := v.([]interface{}); ok {
		for _, item := range items {
			if m, ok := item.(map[string]interface{}); ok {
				if err := w.insertSubRow(f, m, fk, parentPk, parentData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// insertSubRow 子表单行插入（issues/24）：继承主表 apply_user_id（拦截器场景=流程 operator），
// 子表单显式同名字段优先（putIfAbsent）——fillSystemFields 的用户列默认值可解析到 operator，
// 避免 BIGINT create_user/update_user 列回落 "system" 严格模式报错
func (w *MetaTableWriter) insertSubRow(f *FieldMeta, subData map[string]interface{}, fk string, parentPk interface{}, parentData map[string]interface{}) error {
	row := make(map[string]interface{}, len(subData)+1)
	for k, v := range subData {
		row[k] = v
	}
	if v, ok := parentData["apply_user_id"]; ok {
		if _, exists := row["apply_user_id"]; !exists {
			row["apply_user_id"] = v
		}
	}
	row[fk] = parentPk
	_, err := w.Insert(f.TargetTable, row) // 递归走子表自身元数据
	return err
}

// findRowValue 按列名取值（宽松：忽略大小写）
func findRowValue(row map[string]interface{}, columnName string) interface{} {
	if row == nil || columnName == "" {
		return nil
	}
	for k, v := range row {
		if strings.EqualFold(k, columnName) {
			return v
		}
	}
	return nil
}
