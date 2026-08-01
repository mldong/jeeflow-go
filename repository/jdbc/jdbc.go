// Package jdbc 提供 ProcessRepository 的 JDBC 参考实现。
//
// 对齐 spec §7.4 事务约定：仓储方法接收 ctx，WithTx 把事务连接绑定到 ctx，
// 同事务内所有仓储调用走同一连接；无事务上下文时回退为独立连接。
// 引擎核心零依赖，本包依赖 database/sql（stdlib）+ 驱动（调用方引入）——
// 换数据库只需换驱动 import 与 DSN（database/sql 统一抽象，占位符 `?` 由驱动转换）。
// 建表 SQL 见本包 schema/ 目录（mysql.sql / postgres.sql，各语言统一）。
package jdbc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// txKey 是 context 中事务连接的键
type txKey struct{}

// Repository 是 ProcessRepository 的 MySQL 实现。
type Repository struct {
	db    *sql.DB
	idGen spi.IDGenerator
}

// New 构造仓储（db 由调用方创建，支持连接池）。
// 关系表（task_actor / cc_instance）主键使用内置时间戳生成器；
// 需要自定义（如雪花）时用 NewWithIDGen。
func New(db *sql.DB) *Repository {
	return &Repository{db: db, idGen: &tsIDGen{}}
}

// NewWithIDGen 构造仓储并注入 ID 生成器。
func NewWithIDGen(db *sql.DB, idGen spi.IDGenerator) *Repository {
	return &Repository{db: db, idGen: idGen}
}

// tsIDGen 对齐 Java 默认 nextId：时间戳毫秒 + 同毫秒内递增序号
type tsIDGen struct {
	lastMillis int64
	seq        int
}

func (g *tsIDGen) NextID() int64 {
	now := time.Now().UnixMilli()
	if now == g.lastMillis {
		g.seq++
	} else {
		g.lastMillis = now
		g.seq = 0
	}
	return now*1000 + int64(g.seq)
}

// WithTx 开启事务并把事务连接绑定到 ctx，回调内所有仓储方法走同一连接（spec §7.4）。
func (r *Repository) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, txKey{}, tx)
	if err := fn(ctx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// execer / queryer 抽象：事务连接与池连接的最小公共接口
type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// conn 返回当前连接：有事务绑定用事务连接，否则用池连接。
func (r *Repository) conn(ctx context.Context) (interface {
	execer
	queryer
}) {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return r.db
}

// ─── ProcessDefine ─────────────────────────────────────────────────────────────

func (r *Repository) FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error) {
	c := r.conn(ctx)
	row := c.QueryRowContext(ctx,
		"SELECT id, name, display_name, type, state, content, version, create_time, create_user, update_time, update_user FROM wf_process_define WHERE id = ?",
		id)
	def := &model.ProcessDefine{}
	var content []byte
	err := row.Scan(&def.ID, &def.Name, &def.DisplayName, &def.Type, &def.State, &content, &def.Version,
		&def.CreateTime, &def.CreateUser, &def.UpdateTime, &def.UpdateUser)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	def.Content = content
	return def, nil
}

// ─── ProcessInstance ───────────────────────────────────────────────────────────

const instanceCols = "id, parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_time, create_user, update_time, update_user"

func (r *Repository) FindInstanceByID(ctx context.Context, id int64) (*model.ProcessInstance, error) {
	c := r.conn(ctx)
	row := c.QueryRowContext(ctx,
		"SELECT "+instanceCols+" FROM wf_process_instance WHERE id = ?", id)
	inst := &model.ProcessInstance{}
	var parentID sql.NullInt64
	var expire sql.NullTime
	var variable []byte
	err := row.Scan(&inst.ID, &parentID, &inst.DefineID, &inst.State, &inst.ParentNodeName,
		&inst.BusinessNo, &inst.Operator, &expire, &variable, &inst.CreateTime, &inst.CreateUser,
		&inst.UpdateTime, &inst.UpdateUser)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if parentID.Valid {
		v := parentID.Int64
		inst.ParentID = &v
	}
	if expire.Valid {
		t := expire.Time
		inst.ExpireTime = &t
	}
	if len(variable) > 0 {
		_ = json.Unmarshal(variable, &inst.Variables)
	}
	return inst, nil
}

func (r *Repository) SaveInstance(ctx context.Context, inst *model.ProcessInstance) error {
	c := r.conn(ctx)
	variable, _ := json.Marshal(inst.Variables)
	_, err := c.ExecContext(ctx,
		"INSERT INTO wf_process_instance (id, parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
		inst.ID, inst.ParentID, inst.DefineID, inst.State, inst.ParentNodeName, inst.BusinessNo,
		inst.Operator, inst.ExpireTime, variable, inst.CreateTime, inst.CreateUser, inst.UpdateTime, inst.UpdateUser)
	return err
}

func (r *Repository) UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error {
	c := r.conn(ctx)
	variable, _ := json.Marshal(inst.Variables)
	_, err := c.ExecContext(ctx,
		"UPDATE wf_process_instance SET state=?, parent_node_name=?, business_no=?, operator=?, expire_time=?, variable=?, update_time=?, update_user=? WHERE id=?",
		inst.State, inst.ParentNodeName, inst.BusinessNo, inst.Operator, inst.ExpireTime, variable,
		inst.UpdateTime, inst.UpdateUser, inst.ID)
	return err
}

// ─── ProcessTask ───────────────────────────────────────────────────────────────

const taskCols = "id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user"

// scanTask 从 row 扫描任务（含 Nullable 字段处理）
type scanner interface {
	Scan(dest ...interface{}) error
}

func scanTask(row scanner) (*model.ProcessTask, error) {
	t := &model.ProcessTask{}
	var finish, expire sql.NullTime
	var parentTaskID sql.NullInt64
	var variable []byte
	err := row.Scan(&t.ID, &t.ProcessInstanceID, &t.TaskName, &t.DisplayName, &t.TaskType,
		&t.PerformType, &t.TaskState, &t.ActorID, &finish, &expire, &t.FormKey, &parentTaskID,
		&variable, &t.CreateTime, &t.CreateUser, &t.UpdateTime, &t.UpdateUser)
	if err != nil {
		return nil, err
	}
	if finish.Valid {
		v := finish.Time
		t.FinishTime = &v
	}
	if expire.Valid {
		v := expire.Time
		t.ExpireTime = &v
	}
	if parentTaskID.Valid {
		v := parentTaskID.Int64
		t.ParentTaskID = &v
	}
	if len(variable) > 0 {
		_ = json.Unmarshal(variable, &t.Variables)
	}
	return t, nil
}

func (r *Repository) FindTaskByID(ctx context.Context, taskID int64) (*model.ProcessTask, error) {
	c := r.conn(ctx)
	row := c.QueryRowContext(ctx, "SELECT "+taskCols+" FROM wf_process_task WHERE id = ?", taskID)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actors, err := r.findTaskActors(ctx, taskID)
	if err != nil {
		return nil, err
	}
	t.ActorIDs = actors
	return t, nil
}

func (r *Repository) SaveTask(ctx context.Context, task *model.ProcessTask) error {
	c := r.conn(ctx)
	variable, _ := json.Marshal(task.Variables)
	_, err := c.ExecContext(ctx,
		"INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name, task_type, perform_type, task_state, operator, finish_time, expire_time, form_key, task_parent_id, variable, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		task.ID, task.ProcessInstanceID, task.TaskName, task.DisplayName, task.TaskType, task.PerformType,
		task.TaskState, task.ActorID, task.FinishTime, task.ExpireTime, task.FormKey, task.ParentTaskID,
		variable, task.CreateTime, task.CreateUser, task.UpdateTime, task.UpdateUser)
	if err != nil {
		return err
	}
	return r.replaceTaskActors(ctx, task.ID, task.ActorIDs)
}

func (r *Repository) UpdateTask(ctx context.Context, task *model.ProcessTask) error {
	c := r.conn(ctx)
	variable, _ := json.Marshal(task.Variables)
	_, err := c.ExecContext(ctx,
		"UPDATE wf_process_task SET task_state=?, operator=?, finish_time=?, expire_time=?, variable=?, update_time=?, update_user=? WHERE id=?",
		task.TaskState, task.ActorID, task.FinishTime, task.ExpireTime, variable, task.UpdateTime, task.UpdateUser, task.ID)
	return err
}

func (r *Repository) findTasksByState(ctx context.Context, instanceID int64, state model.TaskState, taskNames []string) ([]*model.ProcessTask, error) {
	c := r.conn(ctx)
	sqlStr := "SELECT " + taskCols + " FROM wf_process_task WHERE process_instance_id = ?"
	args := []interface{}{instanceID}
	if state >= 0 {
		sqlStr += " AND task_state = ?"
		args = append(args, state)
	}
	if len(taskNames) > 0 {
		sqlStr += " AND task_name IN (?" + repeatPlaceholder(len(taskNames)-1) + ")"
		for _, n := range taskNames {
			args = append(args, n)
		}
	}
	sqlStr += " ORDER BY id ASC"
	rows, err := c.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []*model.ProcessTask
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		actors, _ := r.findTaskActors(ctx, t.ID)
		t.ActorIDs = actors
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func repeatPlaceholder(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += ", ?"
	}
	return s
}

func (r *Repository) FindDoingTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error) {
	return r.findTasksByState(ctx, instanceID, model.TaskStateDoing, taskNames)
}

func (r *Repository) FindDoneTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error) {
	return r.findTasksByState(ctx, instanceID, model.TaskStateDone, taskNames)
}

func (r *Repository) FindHistoryTasks(ctx context.Context, instanceID int64) ([]*model.ProcessTask, error) {
	return r.findTasksByState(ctx, instanceID, -1, nil)
}

// ─── TaskActor ─────────────────────────────────────────────────────────────────

func (r *Repository) findTaskActors(ctx context.Context, taskID int64) ([]string, error) {
	c := r.conn(ctx)
	rows, err := c.QueryContext(ctx,
		"SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id ASC", taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actors []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		actors = append(actors, a)
	}
	return actors, rows.Err()
}

func (r *Repository) FindTaskActors(ctx context.Context, taskID int64) ([]string, error) {
	return r.findTaskActors(ctx, taskID)
}

func (r *Repository) replaceTaskActors(ctx context.Context, taskID int64, actors []string) error {
	c := r.conn(ctx)
	if _, err := c.ExecContext(ctx, "DELETE FROM wf_process_task_actor WHERE process_task_id = ?", taskID); err != nil {
		return err
	}
	return r.insertTaskActors(ctx, c, taskID, actors)
}

// insertTaskActors 批量插入参与者关系（关系表主键非自增，需显式生成）
func (r *Repository) insertTaskActors(ctx context.Context, c interface {
	execer
}, taskID int64, actors []string) error {
	now := time.Now()
	for _, a := range actors {
		if _, err := c.ExecContext(ctx,
			"INSERT INTO wf_process_task_actor (id, process_task_id, actor_id, create_time, create_user) VALUES (?,?,?,?,?)",
			r.idGen.NextID(), taskID, a, now, "jeeflow"); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) AddTaskActor(ctx context.Context, taskID int64, actors []string) error {
	return r.insertTaskActors(ctx, r.conn(ctx), taskID, actors)
}

func (r *Repository) RemoveTaskActor(ctx context.Context, taskID int64, actors []string) error {
	if len(actors) == 0 {
		return nil
	}
	c := r.conn(ctx)
	sqlStr := "DELETE FROM wf_process_task_actor WHERE process_task_id = ? AND actor_id IN (?" + repeatPlaceholder(len(actors)-1) + ")"
	args := []interface{}{taskID}
	for _, a := range actors {
		args = append(args, a)
	}
	_, err := c.ExecContext(ctx, sqlStr, args...)
	return err
}

// ─── CcInstance（抄送）─────────────────────────────────────────────────────────

func (r *Repository) CreateCcInstance(ctx context.Context, instanceID int64, creator string, actorIDs ...string) error {
	c := r.conn(ctx)
	now := time.Now()
	for _, actorID := range actorIDs {
		if _, err := c.ExecContext(ctx,
			"INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state, create_time, create_user, update_time, update_user) VALUES (?,?,?,0,?,?,?,?)",
			r.idGen.NextID(), instanceID, actorID, now, creator, now, creator); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) UpdateCcStatus(ctx context.Context, instanceID int64, actorID string) error {
	c := r.conn(ctx)
	_, err := c.ExecContext(ctx,
		"UPDATE wf_process_cc_instance SET state=1, update_time=? WHERE process_instance_id=? AND actor_id=?",
		time.Now(), instanceID, actorID)
	return err
}

// 编译期断言：实现 spi.ProcessRepository
var _ spi.ProcessRepository = (*Repository)(nil)
