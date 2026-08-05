// 扩展仓储 JDBC 参考实现（v1.1.0）——流程设计 / 设计历史 / 委托代理
package jdbc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ExtRepository 是 ProcessExtRepository 的 JDBC 实现。
// 分页为单表简单过滤（Filters 按字段名 EQ 匹配），占位符与 Repository 同一约定。
type ExtRepository struct {
	db      *sql.DB
	idGen   spi.IDGenerator
	phStyle string
}

// NewExt 构造扩展仓储（db 复用主仓储的连接池）。
func NewExt(db *sql.DB) *ExtRepository {
	return &ExtRepository{db: db, idGen: &tsIDGen{}, phStyle: detectPlaceholder(db)}
}

// conn 返回当前连接：有事务绑定用事务连接，否则用池连接（与 Repository 同一约定）
func (r *ExtRepository) conn(ctx context.Context) phConn {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return phConn{inner: tx, phStyle: r.phStyle}
	}
	return phConn{inner: r.db, phStyle: r.phStyle}
}

// ── 流程设计 ──────────────────────────────────────────────────────────────────

const designCols = "id, name, display_name, type, icon, is_deployed, remark, create_time, create_user, update_time, update_user"

func (r *ExtRepository) FindDesignByID(ctx context.Context, id int64) (*model.ProcessDesign, error) {
	row := r.conn(ctx).QueryRowContext(ctx,
		"SELECT "+designCols+" FROM wf_process_design WHERE id = ?", id)
	d, err := scanDesign(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return d, err
}

func (r *ExtRepository) SaveDesign(ctx context.Context, d *model.ProcessDesign) error {
	if d.ID == 0 {
		d.ID = r.idGen.NextID()
	}
	now := time.Now()
	if d.CreateTime.IsZero() {
		d.CreateTime = now
	}
	if d.UpdateTime.IsZero() {
		d.UpdateTime = now
	}
	_, err := r.conn(ctx).ExecContext(ctx,
		"INSERT INTO wf_process_design (id, name, display_name, type, icon, is_deployed, remark, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
		d.ID, d.Name, d.DisplayName, d.Type, d.Icon, d.IsDeployed, d.Remark,
		d.CreateTime, d.CreateUser, d.UpdateTime, d.UpdateUser)
	return err
}

func (r *ExtRepository) UpdateDesign(ctx context.Context, d *model.ProcessDesign) error {
	_, err := r.conn(ctx).ExecContext(ctx,
		"UPDATE wf_process_design SET name=?, display_name=?, type=?, icon=?, is_deployed=?, remark=?, update_time=?, update_user=? WHERE id=?",
		d.Name, d.DisplayName, d.Type, d.Icon, d.IsDeployed, d.Remark,
		time.Now(), d.UpdateUser, d.ID)
	return err
}

func (r *ExtRepository) RemoveDesign(ctx context.Context, id int64) error {
	c := r.conn(ctx)
	if _, err := c.ExecContext(ctx, "DELETE FROM wf_process_design WHERE id=?", id); err != nil {
		return err
	}
	_, err := c.ExecContext(ctx, "DELETE FROM wf_process_design_his WHERE process_design_id=?", id)
	return err
}

func (r *ExtRepository) PageDesigns(ctx context.Context, query spi.PageQuery) ([]*model.ProcessDesign, int, error) {
	sqlStr := "SELECT " + designCols + " FROM wf_process_design t WHERE 1=1"
	countStr := "SELECT COUNT(*) FROM wf_process_design t WHERE 1=1"
	args := []interface{}{}
	args2 := []interface{}{}
	// 简单过滤：字段名（无别名）EQ 匹配（白名单）
	for col, val := range query.Filters {
		switch col {
		case "name", "display_name", "type":
			sqlStr += " AND t." + col + " = ?"
			countStr += " AND t." + col + " = ?"
			args = append(args, val)
			args2 = append(args2, val)
		}
	}
	// m_ 条件（issues/05-5）：LIKE/EQ 等走白名单
	condSQL, condArgs := buildExtWhere(query.Conditions, designWhitelist)
	sqlStr += condSQL
	countStr += condSQL
	args = append(args, condArgs...)
	args2 = append(args2, condArgs...)
	var total int
	_ = r.conn(ctx).QueryRowContext(ctx, countStr, args2...).Scan(&total)

	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	pageNum := query.PageNum
	if pageNum <= 0 {
		pageNum = 1
	}
	sqlStr += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
	args = append(args, pageSize, (pageNum-1)*pageSize)
	rows, err := r.conn(ctx).QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var list []*model.ProcessDesign
	for rows.Next() {
		d, err := scanDesign(rows)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, d)
	}
	return list, total, rows.Err()
}

// ── 设计历史 ──────────────────────────────────────────────────────────────────

func (r *ExtRepository) SaveDesignHis(ctx context.Context, his *model.ProcessDesignHis) error {
	if his.ID == 0 {
		his.ID = r.idGen.NextID()
	}
	if his.CreateTime.IsZero() {
		his.CreateTime = time.Now()
	}
	_, err := r.conn(ctx).ExecContext(ctx,
		"INSERT INTO wf_process_design_his (id, process_design_id, content, create_time, create_user) VALUES (?,?,?,?,?)",
		his.ID, his.ProcessDesignID, his.Content, his.CreateTime, his.CreateUser)
	return err
}

func (r *ExtRepository) ListDesignHis(ctx context.Context, designID int64) ([]*model.ProcessDesignHis, error) {
	rows, err := r.conn(ctx).QueryContext(ctx,
		"SELECT id, process_design_id, content, create_time, create_user FROM wf_process_design_his WHERE process_design_id = ? ORDER BY id DESC",
		designID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*model.ProcessDesignHis
	for rows.Next() {
		his := &model.ProcessDesignHis{}
		if err := rows.Scan(&his.ID, &his.ProcessDesignID, &his.Content,
			&nullTimeScan{&his.CreateTime}, &nullStrScan{&his.CreateUser}); err != nil {
			return nil, err
		}
		list = append(list, his)
	}
	return list, rows.Err()
}

// ── 委托代理 ──────────────────────────────────────────────────────────────────

const surrogateCols = "id, process_name, operator, surrogate, start_time, end_time, enabled, create_time, create_user, update_time, update_user"

func (r *ExtRepository) FindSurrogateByID(ctx context.Context, id int64) (*model.ProcessSurrogate, error) {
	row := r.conn(ctx).QueryRowContext(ctx,
		"SELECT "+surrogateCols+" FROM wf_process_surrogate WHERE id = ?", id)
	s, err := scanSurrogate(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return s, err
}

func (r *ExtRepository) SaveSurrogate(ctx context.Context, s *model.ProcessSurrogate) error {
	if s.ID == 0 {
		s.ID = r.idGen.NextID()
	}
	now := time.Now()
	if s.CreateTime.IsZero() {
		s.CreateTime = now
	}
	if s.UpdateTime.IsZero() {
		s.UpdateTime = now
	}
	if s.Enabled == 0 {
		s.Enabled = 1
	}
	_, err := r.conn(ctx).ExecContext(ctx,
		"INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, start_time, end_time, enabled, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
		s.ID, s.ProcessName, s.Operator, s.Surrogate, s.StartTime, s.EndTime, s.Enabled,
		s.CreateTime, s.CreateUser, s.UpdateTime, s.UpdateUser)
	return err
}

func (r *ExtRepository) UpdateSurrogate(ctx context.Context, s *model.ProcessSurrogate) error {
	_, err := r.conn(ctx).ExecContext(ctx,
		"UPDATE wf_process_surrogate SET process_name=?, operator=?, surrogate=?, start_time=?, end_time=?, enabled=?, update_time=?, update_user=? WHERE id=?",
		s.ProcessName, s.Operator, s.Surrogate, s.StartTime, s.EndTime, s.Enabled,
		time.Now(), s.UpdateUser, s.ID)
	return err
}

func (r *ExtRepository) RemoveSurrogate(ctx context.Context, id int64) error {
	_, err := r.conn(ctx).ExecContext(ctx, "DELETE FROM wf_process_surrogate WHERE id=?", id)
	return err
}

// ═══ 列白名单 + m_ 条件 WHERE 构建（issues/05-5） ═══

var designWhitelist = map[string]bool{
	"t.id": true, "t.name": true, "t.display_name": true, "t.type": true,
	"t.is_deployed": true, "t.remark": true, "t.create_time": true, "t.update_time": true,
}

var surrogateWhitelist = map[string]bool{
	"t.id": true, "t.process_name": true, "t.operator": true, "t.surrogate": true,
	"t.enabled": true, "t.start_time": true, "t.end_time": true,
	"t.create_time": true, "t.update_time": true,
}

// buildExtWhere m_ 条件 WHERE 构建（白名单 + 参数化）
func buildExtWhere(conditions []spi.Condition, whitelist map[string]bool) (string, []interface{}) {
	var b strings.Builder
	var params []interface{}
	for _, c := range conditions {
		if !whitelist[c.Column] {
			continue
		}
		val := c.Value
		if val == nil {
			continue
		}
		if sv, ok := val.(string); ok && sv == "" {
			continue
		}
		switch strings.ToUpper(c.Operator) {
		case "EQ":
			b.WriteString(" AND " + c.Column + " = ?")
			params = append(params, val)
		case "LIKE":
			b.WriteString(" AND " + c.Column + " LIKE ?")
			params = append(params, "%"+fmt.Sprint(val)+"%")
		case "LLIKE":
			b.WriteString(" AND " + c.Column + " LIKE ?")
			params = append(params, "%"+fmt.Sprint(val))
		case "RLIKE":
			b.WriteString(" AND " + c.Column + " LIKE ?")
			params = append(params, fmt.Sprint(val)+"%")
		case "IN":
			if list, ok := val.([]interface{}); ok && len(list) > 0 {
				marks := strings.TrimSuffix(strings.Repeat("?,", len(list)), ",")
				b.WriteString(" AND " + c.Column + " IN (" + marks + ")")
				params = append(params, list...)
			}
		}
	}
	return b.String(), params
}

func (r *ExtRepository) PageSurrogates(ctx context.Context, query spi.PageQuery) ([]*model.ProcessSurrogate, int, error) {
	countStr := "SELECT COUNT(*) FROM wf_process_surrogate t WHERE 1=1"
	sqlStr := "SELECT " + surrogateCols + " FROM wf_process_surrogate t WHERE 1=1"
	args := []interface{}{}
	args2 := []interface{}{}
	for col, val := range query.Filters {
		switch col {
		case "operator", "surrogate", "process_name", "enabled":
			countStr += " AND t." + col + " = ?"
			sqlStr += " AND t." + col + " = ?"
			args2 = append(args2, val)
			args = append(args, val)
		}
	}
	// m_ 条件（issues/05-5）
	condSQL, condArgs := buildExtWhere(query.Conditions, surrogateWhitelist)
	countStr += condSQL
	sqlStr += condSQL
	args2 = append(args2, condArgs...)
	args = append(args, condArgs...)
	var total int
	_ = r.conn(ctx).QueryRowContext(ctx, countStr, args2...).Scan(&total)

	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	pageNum := query.PageNum
	if pageNum <= 0 {
		pageNum = 1
	}
	sqlStr += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
	args = append(args, pageSize, (pageNum-1)*pageSize)
	rows, err := r.conn(ctx).QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var list []*model.ProcessSurrogate
	for rows.Next() {
		s, err := scanSurrogate(rows)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, s)
	}
	return list, total, rows.Err()
}

func (r *ExtRepository) GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error) {
	// 1. 精确匹配流程
	hit, err := r.querySurrogate(ctx, operator, processName, at)
	if err != nil || hit != nil {
		return hit, err
	}
	// 2. 全流程委托兜底（process_name 为空）
	return r.querySurrogate(ctx, operator, "", at)
}

func (r *ExtRepository) querySurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error) {
	sqlStr := "SELECT " + surrogateCols + " FROM wf_process_surrogate WHERE operator = ? AND enabled = 1 AND surrogate <> ?"
	args := []interface{}{operator, operator}
	if processName == "" {
		sqlStr += " AND (process_name IS NULL OR process_name = '')"
	} else {
		sqlStr += " AND process_name = ?"
		args = append(args, processName)
	}
	if !at.IsZero() {
		sqlStr += " AND (start_time IS NULL OR start_time <= ?) AND (end_time IS NULL OR end_time >= ?)"
		args = append(args, at, at)
	}
	sqlStr += " ORDER BY id DESC LIMIT 1"
	rows, err := r.conn(ctx).QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		return scanSurrogate(rows)
	}
	return nil, rows.Err()
}

// ── 行扫描 ────────────────────────────────────────────────────────────────────

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanDesign(row rowScanner) (*model.ProcessDesign, error) {
	d := &model.ProcessDesign{}
	var isDeployed sql.NullInt64
	err := row.Scan(&d.ID, &nullStrScan{&d.Name}, &nullStrScan{&d.DisplayName},
		&nullStrScan{&d.Type}, &nullStrScan{&d.Icon}, &isDeployed,
		&nullStrScan{&d.Remark},
		&nullTimeScan{&d.CreateTime}, &nullStrScan{&d.CreateUser},
		&nullTimeScan{&d.UpdateTime}, &nullStrScan{&d.UpdateUser})
	if err != nil {
		return nil, err
	}
	d.IsDeployed = int(isDeployed.Int64)
	return d, nil
}

func scanSurrogate(row rowScanner) (*model.ProcessSurrogate, error) {
	s := &model.ProcessSurrogate{}
	var enabled sql.NullInt64
	err := row.Scan(&s.ID, &nullStrScan{&s.ProcessName}, &nullStrScan{&s.Operator},
		&nullStrScan{&s.Surrogate},
		&nullTimePtrScan{&s.StartTime}, &nullTimePtrScan{&s.EndTime}, &enabled,
		&nullTimeScan{&s.CreateTime}, &nullStrScan{&s.CreateUser},
		&nullTimeScan{&s.UpdateTime}, &nullStrScan{&s.UpdateUser})
	if err != nil {
		return nil, err
	}
	s.Enabled = int(enabled.Int64)
	return s, nil
}

// 编译期断言：实现 spi.ProcessExtRepository
var _ spi.ProcessExtRepository = (*ExtRepository)(nil)
