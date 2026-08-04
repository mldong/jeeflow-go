// 元数据模型 + 加载 SPI（issues/23）——与 Java jeeflow-persist meta 包契约一致
package persist

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ─── StorageType ───────────────────────────────────────────────────────────────

// StorageType 字段存储类型（对齐 mldong dev_schema_field 1-5 语义）
type StorageType int

const (
	StorageNormal   StorageType = 1 // 直写列
	StorageExpand   StorageType = 2 // 对象展开为多列（expandFields 定义子字段列映射）
	StorageJSON     StorageType = 3 // 对象/数组序列化为 JSON 串写列
	StorageOne2One  StorageType = 4 // 子表单条（外键=主表主键，同事务）
	StorageOne2Many StorageType = 5 // 子表多条（外键=主表主键，同事务）
)

// UnmarshalJSON 支持名称（"EXPAND"）与数字（2，mldong dev_schema_field 语义）
func (s *StorageType) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch n := v.(type) {
	case float64:
		*s = StorageType(int(n))
	case string:
		switch strings.ToUpper(n) {
		case "NORMAL":
			*s = StorageNormal
		case "EXPAND":
			*s = StorageExpand
		case "JSON":
			*s = StorageJSON
		case "ONE2ONE":
			*s = StorageOne2One
		case "ONE2MANY":
			*s = StorageOne2Many
		default:
			*s = StorageNormal
		}
	default:
		*s = StorageNormal
	}
	return nil
}

// ─── FieldMeta / TableMeta ─────────────────────────────────────────────────────

// FieldMeta 字段元数据——表单字段 → 存储语义映射
type FieldMeta struct {
	Name         string            `json:"name"`         // 表单字段名（f_ 去前缀）
	ColumnName   string            `json:"columnName"`   // 主表列名（缺省 = name 转下划线）
	StorageType  StorageType       `json:"storageType"`  // 存储类型（默认 NORMAL）
	ExpandFields map[string]string `json:"expandFields"` // EXPAND：子字段名 → 表列名
	TargetTable  string            `json:"targetTable"`  // ONE2ONE/ONE2MANY：子表表名
	ForeignKey   string            `json:"foreignKey"`   // 子表外键列（缺省 = 主表主键列名）
}

// Column 解析列名（缺省驼峰转下划线）
func (f *FieldMeta) Column() string {
	if f.ColumnName != "" {
		return f.ColumnName
	}
	return ToUnderline(f.Name)
}

// TableMeta 表元数据——一张业务表的字段存储规范
type TableMeta struct {
	TableName  string      `json:"tableName"`
	PrimaryKey string      `json:"primaryKey"` // 默认 id
	Fields     []FieldMeta `json:"fields"`
}

// PK 主键列名（缺省 id）
func (m *TableMeta) PK() string {
	if m.PrimaryKey != "" {
		return m.PrimaryKey
	}
	return "id"
}

// FindField 按字段名查 FieldMeta（大小写不敏感）
func (m *TableMeta) FindField(name string) *FieldMeta {
	for i := range m.Fields {
		if strings.EqualFold(m.Fields[i].Name, name) {
			return &m.Fields[i]
		}
	}
	return nil
}

// FindFieldByColumn 按列名查 FieldMeta（大小写不敏感，未消费列判定）
func (m *TableMeta) FindFieldByColumn(columnName string) *FieldMeta {
	for i := range m.Fields {
		if strings.EqualFold(m.Fields[i].Column(), columnName) {
			return &m.Fields[i]
		}
	}
	return nil
}

// ToUnderline 驼峰转下划线（companyName → company_name）
func ToUnderline(name string) string {
	var sb strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				sb.WriteByte('_')
			}
			sb.WriteRune(r + 32)
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// ─── IDynamicMetaProvider ──────────────────────────────────────────────────────

// IDynamicMetaProvider 动态元数据提供者 SPI——集成方只实现这一件事。
// 写、读共用；未定义返回 nil（调用方回落表结构探测，全 NORMAL 语义）。
type IDynamicMetaProvider interface {
	LoadTableMeta(tableName string) *TableMeta
}

// ─── JsonMetaProvider（内置 JSON 配置加载器） ──────────────────────────────────

// JsonMetaProvider 从文件系统目录加载元数据 JSON（文件名 = 表名，如 biz_leave.json）。
// 配置格式与 Java JsonMetaProvider 一致（storageType 支持名称或 1-5 数字）。
type JsonMetaProvider struct {
	dir string
	mu  sync.RWMutex
	cache map[string]*TableMeta
}

// NewJsonMetaProvider 创建 JSON 加载器（dir 为配置目录）
func NewJsonMetaProvider(dir string) *JsonMetaProvider {
	return &JsonMetaProvider{dir: dir, cache: make(map[string]*TableMeta)}
}

func (p *JsonMetaProvider) LoadTableMeta(tableName string) *TableMeta {
	if tableName == "" {
		return nil
	}
	p.mu.RLock()
	m, ok := p.cache[tableName]
	p.mu.RUnlock()
	if ok {
		return m
	}
	path := filepath.Join(p.dir, tableName+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil // 未定义：回落表结构探测
	}
	var meta TableMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		panic(fmt.Sprintf("persist: parse meta %s failed: %v", path, err))
	}
	p.mu.Lock()
	p.cache[tableName] = &meta
	p.mu.Unlock()
	return &meta
}
