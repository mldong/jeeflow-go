// Package persist 动态表写入组件（引擎无关）+ 工作流入库适配拦截器。
//
// 对标 Java jeeflow-persist（issues/18）：
//   - DynamicTableWriter：给「表名 + 字段 Map」安全写入任意业务表
//     （列过滤 / 参数化 INSERT / 幂等 / 系统字段），不依赖工作流引擎
//   - JdbcDynamicTableWriter：*sql.DB 默认实现（MySQL/PG 走 information_schema，
//     SQLite 走 PRAGMA；H2 由 UPPER() 兼容）
//   - PersistPostInterceptor：流程结束同意后，f_ 表单数据写入业务表
package persist

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ─── DynamicTableWriter ────────────────────────────────────────────────────────

// DynamicTableWriter 动态表写入组件接口——引擎无关，四语言契约一致
type DynamicTableWriter interface {
	// FilterColumns 按目标表过滤列（表结构探测），返回表内实际存在的列
	FilterColumns(tableName string, columns []string) ([]string, error)
	// Insert 参数化 INSERT（按 FilterColumns 结果落库），返回生成主键
	Insert(tableName string, data map[string]interface{}) (interface{}, error)
	// Exists 幂等检查：指定业务键（如 process_instance_id）是否已存在
	Exists(tableName, bizKey string, bizKeyValue interface{}) (bool, error)
	// FillSystemFields 按配置列名填充系统字段（未配置的列跳过）
	FillSystemFields(data map[string]interface{}, isInsert bool)
}

// ─── JdbcDynamicTableWriter ────────────────────────────────────────────────────

// 表名安全校验：sys_ 前缀（框架保留）与非法字符拒绝
var (
	tableNameRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	sysPrefixRe = regexp.MustCompile(`^sys_`)
)

// 时间格式化（对齐 Java ColumnMeta.now()）
const timeLayout = "2006-01-02 15:04:05"

// JdbcDynamicTableWriter 是 DynamicTableWriter 的 *sql.DB 默认实现
type JdbcDynamicTableWriter struct {
	db *sql.DB

	// 系统字段列名（空串=禁用该列）
	CreateTimeColumn string
	CreateUserColumn string
	UpdateTimeColumn string
	UpdateUserColumn string
	IsDeletedColumn  string
	// 用户列默认值（issues/19）：优先取 data 中已注入的 apply_user_id=流程 operator，
	// 否则用此配置值，缺省 "system"——多数框架业务表 create_user/update_user 为 BIGINT 存 userId
	DefaultUserValue interface{}
	// 列匹配（issues/20）：默认宽松——驼峰↔下划线归一匹配（表单字段 companyName ↔ 表列 company_name）；
	// 需要精确控制列名的集成方显式开启严格模式（忽略大小写精确匹配）
	StrictColumnMatch bool

	mu     sync.RWMutex
	cache  map[string][]string // 表名 -> 实际列（大写）
	sqlite bool                // 方言：SQLite 走 PRAGMA table_info
}

// NewJdbcDynamicTableWriter 创建 JDBC 写入器（方言自动探测：
// 驱动类型名含 "sqlite" 走 PRAGMA，其余走 information_schema）
func NewJdbcDynamicTableWriter(db *sql.DB) *JdbcDynamicTableWriter {
	w := &JdbcDynamicTableWriter{
		db:               db,
		CreateTimeColumn: "create_time",
		CreateUserColumn: "create_user",
		UpdateTimeColumn: "update_time",
		UpdateUserColumn: "update_user",
		IsDeletedColumn:  "is_deleted",
		DefaultUserValue: "system",
		cache:            make(map[string][]string),
	}
	w.sqlite = strings.Contains(fmt.Sprintf("%T", db.Driver()), "sqlite")
	return w
}

// 校验表名：非空、合法字符、拒绝 sys_ 前缀
func checkTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("persist: table name is empty")
	}
	if sysPrefixRe.MatchString(tableName) {
		return fmt.Errorf("persist: table %q with sys_ prefix is not allowed", tableName)
	}
	if !tableNameRe.MatchString(tableName) {
		return fmt.Errorf("persist: table %q contains illegal characters", tableName)
	}
	return nil
}

// tableColumns 探测表结构（缓存）：
//   - SQLite: PRAGMA table_info
//   - MySQL/PG/H2: information_schema.columns（UPPER 比较兼容 H2 大写存储）
func (w *JdbcDynamicTableWriter) tableColumns(ctx context.Context, tableName string) ([]string, error) {
	w.mu.RLock()
	cols, ok := w.cache[tableName]
	w.mu.RUnlock()
	if ok {
		return cols, nil
	}

	var names []string
	var rows *sql.Rows
	var err error
	if w.sqlite {
		// PRAGMA 不支持占位符——表名已过安全校验
		rows, err = w.db.QueryContext(ctx, "PRAGMA table_info("+tableName+")")
		if err != nil {
			return nil, fmt.Errorf("persist: probe columns of %s failed: %w", tableName, err)
		}
	} else {
		rows, err = w.db.QueryContext(ctx,
			"SELECT column_name FROM information_schema.columns WHERE UPPER(table_name) = UPPER(?) ORDER BY ordinal_position",
			tableName)
		if err != nil {
			return nil, fmt.Errorf("persist: probe columns of %s failed: %w", tableName, err)
		}
	}
	defer rows.Close()
	if w.sqlite {
		for rows.Next() {
			var cid, notNull, pk int
			var name, typ string
			var dflt interface{}
			if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
				return nil, fmt.Errorf("persist: scan columns of %s failed: %w", tableName, err)
			}
			names = append(names, strings.ToUpper(name))
		}
	} else {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, fmt.Errorf("persist: scan columns of %s failed: %w", tableName, err)
			}
			names = append(names, strings.ToUpper(name))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("persist: iterate columns of %s failed: %w", tableName, err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("persist: table %q not found", tableName)
	}

	w.mu.Lock()
	w.cache[tableName] = names
	w.mu.Unlock()
	return names, nil
}

// FilterColumns 按目标表过滤列（宽松模式驼峰↔下划线归一匹配，issues/20）
func (w *JdbcDynamicTableWriter) FilterColumns(tableName string, columns []string) ([]string, error) {
	if err := checkTableName(tableName); err != nil {
		return nil, err
	}
	cols, err := w.tableColumns(context.Background(), tableName)
	if err != nil {
		return nil, err
	}
	var kept []string
	for _, c := range columns {
		if w.findColumn(cols, c) != "" {
			kept = append(kept, c)
		}
	}
	return kept, nil
}

// Insert 参数化 INSERT（列过滤 + 值过滤，防注入；写入用表列原名，issues/20）
func (w *JdbcDynamicTableWriter) Insert(tableName string, data map[string]interface{}) (interface{}, error) {
	if err := checkTableName(tableName); err != nil {
		return nil, err
	}
	cols, err := w.tableColumns(context.Background(), tableName)
	if err != nil {
		return nil, err
	}
	var names []string
	var values []interface{}
	// 保持插入顺序稳定（map 无序，先按表列顺序取）
	for _, col := range cols {
		key := w.findDataKey(data, col)
		if key == "" {
			continue
		}
		names = append(names, col)
		values = append(values, data[key])
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("persist: no matching columns for %s", tableName)
	}
	placeholders := strings.Repeat("?,", len(names))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName,
		strings.Join(names, ","), placeholders)
	res, err := w.db.ExecContext(context.Background(), query, values...)
	if err != nil {
		return nil, fmt.Errorf("persist: insert into %s failed: %w", tableName, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, nil // 不支持自增键的驱动（如 PG）不返回 ID，不算错误
	}
	return id, nil
}

// findColumn 列匹配（issues/20）：严格=忽略大小写精确；宽松（默认）=驼峰↔下划线归一匹配
func (w *JdbcDynamicTableWriter) findColumn(cols []string, column string) string {
	for _, col := range cols {
		if w.StrictColumnMatch {
			if strings.EqualFold(col, column) {
				return col
			}
		} else if normalizeColumn(col) == normalizeColumn(column) {
			return col
		}
	}
	return ""
}

// findDataKey 在 data 中找匹配指定表列的 key（宽松模式驼峰 key 匹配下划线列）
func (w *JdbcDynamicTableWriter) findDataKey(data map[string]interface{}, col string) string {
	for k := range data {
		if w.findColumn([]string{col}, k) != "" {
			return k
		}
	}
	return ""
}

// normalizeColumn 列名归一：转小写 + 去下划线（companyName / company_name / COMPANY_NAME 等价）
func normalizeColumn(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

// Exists 幂等检查
func (w *JdbcDynamicTableWriter) Exists(tableName, bizKey string, bizKeyValue interface{}) (bool, error) {
	if err := checkTableName(tableName); err != nil {
		return false, err
	}
	if _, err := w.tableColumns(context.Background(), tableName); err != nil {
		return false, err
	}
	query := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE %s = ?", tableName, bizKey)
	var n int
	if err := w.db.QueryRowContext(context.Background(), query, bizKeyValue).Scan(&n); err != nil {
		return false, fmt.Errorf("persist: exists check on %s failed: %w", tableName, err)
	}
	return n > 0, nil
}

// FillSystemFields 按配置列名填充系统字段（未配置的列跳过）
func (w *JdbcDynamicTableWriter) FillSystemFields(data map[string]interface{}, isInsert bool) {
	now := time.Now().Format(timeLayout)
	if isInsert {
		if w.CreateTimeColumn != "" {
			data[w.CreateTimeColumn] = now
		}
		if w.CreateUserColumn != "" {
			data[w.CreateUserColumn] = w.resolveDefaultUser(data)
		}
		if w.UpdateTimeColumn != "" {
			data[w.UpdateTimeColumn] = now
		}
		if w.UpdateUserColumn != "" {
			data[w.UpdateUserColumn] = w.resolveDefaultUser(data)
		}
		if w.IsDeletedColumn != "" {
			data[w.IsDeletedColumn] = 0
		}
	} else {
		if w.UpdateTimeColumn != "" {
			data[w.UpdateTimeColumn] = now
		}
		if w.UpdateUserColumn != "" {
			data[w.UpdateUserColumn] = w.resolveDefaultUser(data)
		}
	}
}

// resolveDefaultUser 默认用户值（issues/19）：优先取 data 中已注入的 apply_user_id
// （拦截器场景 = 流程 operator，BIGINT 用户列表开箱即用），否则回落配置默认值。
func (w *JdbcDynamicTableWriter) resolveDefaultUser(data map[string]interface{}) interface{} {
	if operator, ok := data["apply_user_id"]; ok && operator != nil {
		return operator
	}
	return w.DefaultUserValue
}
