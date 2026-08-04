// Package persist 动态表写入组件（引擎无关）+ 工作流入库适配拦截器。
//
// 对标 Java jeeflow-persist（issues/18~21）：
//   - DynamicTableWriter：给「表名 + 字段 Map」安全写入任意业务表
//     （列过滤 / 参数化 INSERT / 幂等 / 系统字段），不依赖工作流引擎
//   - JdbcDynamicTableWriter：*sql.DB 默认实现（MySQL/PG 走 information_schema，
//     SQLite 走 PRAGMA；H2 由 UPPER() 兼容），宽松列匹配（驼峰↔下划线）、
//     非自增主键生成（issues/21）
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

// columnMeta 列元数据（issues/21：主键/自增用于主键生成决策）
type columnMeta struct {
	name          string // 表列原名（UPPER）
	primaryKey    bool
	autoIncrement bool
}

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
	// 主键生成器（issues/21）：非自增主键表（雪花/应用生成）插入时生成主键值，入参表名
	PrimaryKeyGenerator func(tableName string) interface{}

	mu     sync.RWMutex
	cache  map[string][]columnMeta
	sqlite bool // 方言：SQLite 走 PRAGMA table_info
	mysql  bool // 方言：MySQL 走 EXTRA/COLUMN_KEY
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
		cache:            make(map[string][]columnMeta),
	}
	driverType := fmt.Sprintf("%T", db.Driver())
	w.sqlite = strings.Contains(driverType, "sqlite")
	w.mysql = strings.Contains(driverType, "mysql")
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
//   - SQLite: PRAGMA table_info（INTEGER PRIMARY KEY = rowid 别名，视为自增）
//   - MySQL: information_schema EXTRA/COLUMN_KEY
//   - PG/H2: information_schema IS_IDENTITY + 主键约束 JOIN（标准 SQL 兼容）
func (w *JdbcDynamicTableWriter) tableColumns(ctx context.Context, tableName string) ([]columnMeta, error) {
	w.mu.RLock()
	cols, ok := w.cache[tableName]
	w.mu.RUnlock()
	if ok {
		return cols, nil
	}

	var metas []columnMeta
	var err error
	if w.sqlite {
		metas, err = w.probeSQLite(ctx, tableName)
	} else if w.mysql {
		metas, err = w.probeMySQL(ctx, tableName)
	} else {
		metas, err = w.probeStd(ctx, tableName)
	}
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, fmt.Errorf("persist: table %q not found", tableName)
	}

	w.mu.Lock()
	w.cache[tableName] = metas
	w.mu.Unlock()
	return metas, nil
}

func (w *JdbcDynamicTableWriter) probeSQLite(ctx context.Context, tableName string) ([]columnMeta, error) {
	// PRAGMA 不支持占位符——表名已过安全校验
	rows, err := w.db.QueryContext(ctx, "PRAGMA table_info("+tableName+")")
	if err != nil {
		return nil, fmt.Errorf("persist: probe columns of %s failed: %w", tableName, err)
	}
	defer rows.Close()
	var metas []columnMeta
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, fmt.Errorf("persist: scan columns of %s failed: %w", tableName, err)
		}
		// INTEGER PRIMARY KEY 是 rowid 别名（自动生成）
		autoInc := pk == 1 && strings.EqualFold(strings.TrimSpace(typ), "INTEGER")
		metas = append(metas, columnMeta{name: strings.ToUpper(name), primaryKey: pk == 1, autoIncrement: autoInc})
	}
	return metas, rows.Err()
}

func (w *JdbcDynamicTableWriter) probeMySQL(ctx context.Context, tableName string) ([]columnMeta, error) {
	// issues/22：限定当前 schema（DATABASE()），防多库同名表列重复
	rows, err := w.db.QueryContext(ctx,
		"SELECT column_name, extra, column_key FROM information_schema.columns "+
			"WHERE UPPER(table_name) = UPPER(?) AND table_schema = DATABASE() "+
			"ORDER BY ordinal_position", tableName)
	if err != nil {
		return nil, fmt.Errorf("persist: probe columns of %s failed: %w", tableName, err)
	}
	defer rows.Close()
	var metas []columnMeta
	for rows.Next() {
		var name, extra, key string
		if err := rows.Scan(&name, &extra, &key); err != nil {
			return nil, fmt.Errorf("persist: scan columns of %s failed: %w", tableName, err)
		}
		metas = append(metas, columnMeta{
			name:          strings.ToUpper(name),
			primaryKey:    strings.EqualFold(key, "PRI"),
			autoIncrement: strings.Contains(strings.ToLower(extra), "auto_increment"),
		})
	}
	return metas, rows.Err()
}

func (w *JdbcDynamicTableWriter) probeStd(ctx context.Context, tableName string) ([]columnMeta, error) {
	// PG/H2 标准 SQL：IS_IDENTITY（identity）+ column_default nextval（PG serial）+ 主键约束 JOIN
	// issues/22：限定当前 schema（CURRENT_SCHEMA()，H2/PG 均支持），防多库同名表列重复
	rows, err := w.db.QueryContext(ctx,
		"SELECT c.column_name, c.is_identity, c.column_default, "+
			"CASE WHEN kcu.column_name IS NOT NULL THEN 'PRI' ELSE '' END AS column_key "+
			"FROM information_schema.columns c "+
			"LEFT JOIN information_schema.table_constraints tc "+
			"  ON tc.table_name = c.table_name AND tc.constraint_type = 'PRIMARY KEY' "+
			"  AND tc.table_schema = c.table_schema "+
			"LEFT JOIN information_schema.key_column_usage kcu "+
			"  ON kcu.constraint_name = tc.constraint_name AND kcu.column_name = c.column_name "+
			"  AND kcu.table_schema = c.table_schema "+
			"WHERE UPPER(c.table_name) = UPPER(?) AND c.table_schema = CURRENT_SCHEMA() "+
			"ORDER BY c.ordinal_position", tableName)
	if err != nil {
		return nil, fmt.Errorf("persist: probe columns of %s failed: %w", tableName, err)
	}
	defer rows.Close()
	var metas []columnMeta
	for rows.Next() {
		var name, isIdentity string
		var columnDefault, key sql.NullString
		if err := rows.Scan(&name, &isIdentity, &columnDefault, &key); err != nil {
			return nil, fmt.Errorf("persist: scan columns of %s failed: %w", tableName, err)
		}
		autoInc := strings.EqualFold(isIdentity, "YES") ||
			(columnDefault.Valid && strings.Contains(columnDefault.String, "nextval"))
		metas = append(metas, columnMeta{
			name:          strings.ToUpper(name),
			primaryKey:    strings.EqualFold(key.String, "PRI"),
			autoIncrement: autoInc,
		})
	}
	return metas, rows.Err()
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

// Insert 参数化 INSERT（列过滤 + 值过滤，防注入；写入用表列原名，issues/20；
// 非自增主键生成，issues/21）
func (w *JdbcDynamicTableWriter) Insert(tableName string, data map[string]interface{}) (interface{}, error) {
	if err := checkTableName(tableName); err != nil {
		return nil, err
	}
	metas, err := w.tableColumns(context.Background(), tableName)
	if err != nil {
		return nil, err
	}
	var names []string
	var values []interface{}
	// 保持插入顺序稳定（map 无序，先按表列顺序取）
	for _, m := range metas {
		key := w.findDataKey(data, m.name)
		if key != "" {
			names = append(names, m.name)
			values = append(values, data[key])
			continue
		}
		// 主键生成（issues/21）：非自增主键表且 data 无主键值 → 调生成器；未配置 → 清晰报错
		if m.primaryKey && !m.autoIncrement {
			if w.PrimaryKeyGenerator == nil {
				return nil, fmt.Errorf("persist: table %q primary key %q is not auto-increment and no primary key generator configured (set PrimaryKeyGenerator, e.g. snowflake)", tableName, m.name)
			}
			names = append(names, m.name)
			values = append(values, w.PrimaryKeyGenerator(tableName))
		}
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
func (w *JdbcDynamicTableWriter) findColumn(cols []columnMeta, column string) string {
	for _, m := range cols {
		if w.StrictColumnMatch {
			if strings.EqualFold(m.name, column) {
				return m.name
			}
		} else if normalizeColumn(m.name) == normalizeColumn(column) {
			return m.name
		}
	}
	return ""
}

// findDataKey 在 data 中找匹配指定表列的 key（宽松模式驼峰 key 匹配下划线列）
func (w *JdbcDynamicTableWriter) findDataKey(data map[string]interface{}, col string) string {
	for k := range data {
		if w.findColumn([]columnMeta{{name: col}}, k) != "" {
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
