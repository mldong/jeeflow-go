package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

type Repository struct {
	mu          sync.RWMutex
	defines     map[int64]*model.ProcessDefine
	instances   map[int64]*model.ProcessInstance
	tasks       map[int64]*model.ProcessTask
	actors      map[int64][]string
	ccInstances map[int64][]string
	nextID      atomic.Int64
}

func New() *Repository {
	r := &Repository{
		defines:     make(map[int64]*model.ProcessDefine),
		instances:   make(map[int64]*model.ProcessInstance),
		tasks:       make(map[int64]*model.ProcessTask),
		actors:      make(map[int64][]string),
		ccInstances: make(map[int64][]string),
	}
	r.nextID.Store(1)
	return r
}

func (r *Repository) AddDefine(def *model.ProcessDefine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if def.ID == 0 {
		def.ID = r.nextID.Add(1)
	}
	r.defines[def.ID] = def
}

func (r *Repository) FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.defines[id]
	if !ok {
		return nil, nil
	}
	cp := *d
	return &cp, nil
}

// FindDefineByName 按流程编码查最新一条定义（id 倒序取首条）
func (r *Repository) FindDefineByName(ctx context.Context, name string) (*model.ProcessDefine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var latest *model.ProcessDefine
	for _, d := range r.defines {
		if d.Name == name && (latest == nil || d.ID > latest.ID) {
			latest = d
		}
	}
	if latest == nil {
		return nil, nil
	}
	cp := *latest
	return &cp, nil
}

// ─── 定义写操作（v1.0.1，对齐 SPI） ──────────────────────────────────────────

func (r *Repository) SaveDefine(ctx context.Context, def *model.ProcessDefine) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if def.ID == 0 {
		def.ID = r.nextID.Add(1)
	}
	cp := *def
	r.defines[def.ID] = &cp
	return nil
}

func (r *Repository) UpdateDefine(ctx context.Context, def *model.ProcessDefine) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *def
	r.defines[def.ID] = &cp
	return nil
}

func (r *Repository) UpdateDefineState(ctx context.Context, defineID int64, state int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.defines[defineID]; ok {
		d.State = state
	}
	return nil
}

func (r *Repository) RemoveDefine(ctx context.Context, defineID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.defines, defineID)
	return nil
}

func (r *Repository) FindInstanceByID(ctx context.Context, id int64) (*model.ProcessInstance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	inst, ok := r.instances[id]
	if !ok {
		return nil, nil
	}
	cp := *inst
	for _, t := range r.tasks {
		if t.ProcessInstanceID == id {
			tc := *t
			tc.ActorIDs = r.actors[t.ID]
			cp.Tasks = append(cp.Tasks, &tc)
		}
	}
	return &cp, nil
}

func (r *Repository) SaveInstance(ctx context.Context, inst *model.ProcessInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inst.ID == 0 {
		inst.ID = r.nextID.Add(1)
	}
	cp := *inst
	cp.Tasks = nil
	r.instances[inst.ID] = &cp
	return nil
}

func (r *Repository) UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *inst
	cp.Tasks = nil
	r.instances[inst.ID] = &cp
	// v1.0.1：级联保存聚合根内任务状态变更
	for _, t := range inst.Tasks {
		if t.ID == 0 {
			continue
		}
		tc := *t
		tc.ActorIDs = nil
		r.tasks[t.ID] = &tc
		if len(t.ActorIDs) > 0 {
			r.actors[t.ID] = append([]string{}, t.ActorIDs...)
		}
	}
	return nil
}

func (r *Repository) FindTaskByID(ctx context.Context, taskID int64) (*model.ProcessTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[taskID]
	if !ok {
		return nil, nil
	}
	cp := *t
	cp.ActorIDs = r.actors[taskID]
	return &cp, nil
}

func (r *Repository) SaveTask(ctx context.Context, task *model.ProcessTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task.ID == 0 {
		task.ID = r.nextID.Add(1)
	}
	cp := *task
	cp.ActorIDs = nil
	r.tasks[task.ID] = &cp
	if len(task.ActorIDs) > 0 {
		r.actors[task.ID] = append([]string{}, task.ActorIDs...)
	}
	return nil
}

func (r *Repository) UpdateTask(ctx context.Context, task *model.ProcessTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *task
	cp.ActorIDs = nil
	r.tasks[task.ID] = &cp
	if len(task.ActorIDs) > 0 {
		r.actors[task.ID] = append([]string{}, task.ActorIDs...)
	}
	return nil
}

func (r *Repository) FindDoingTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessTask
	for _, t := range r.tasks {
		if t.ProcessInstanceID == instanceID && t.TaskState == model.TaskStateDoing {
			if len(taskNames) > 0 {
				found := false
				for _, n := range taskNames {
					if t.TaskName == n {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			cp := *t
			cp.ActorIDs = r.actors[t.ID]
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (r *Repository) FindDoneTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessTask
	for _, t := range r.tasks {
		if t.ProcessInstanceID == instanceID && t.TaskState == model.TaskStateDone {
			cp := *t
			cp.ActorIDs = r.actors[t.ID]
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (r *Repository) FindHistoryTasks(ctx context.Context, instanceID int64) ([]*model.ProcessTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessTask
	for _, t := range r.tasks {
		if t.ProcessInstanceID == instanceID {
			cp := *t
			cp.ActorIDs = r.actors[t.ID]
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (r *Repository) FindTaskActors(ctx context.Context, taskID int64) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string{}, r.actors[taskID]...), nil
}

func (r *Repository) AddTaskActor(ctx context.Context, taskID int64, actors []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := r.actors[taskID]
	seen := make(map[string]bool)
	for _, a := range existing {
		seen[a] = true
	}
	for _, a := range actors {
		if !seen[a] {
			existing = append(existing, a)
			seen[a] = true
		}
	}
	r.actors[taskID] = existing
	return nil
}

func (r *Repository) RemoveTaskActor(ctx context.Context, taskID int64, actors []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	remove := make(map[string]bool)
	for _, a := range actors {
		remove[a] = true
	}
	var kept []string
	for _, a := range r.actors[taskID] {
		if !remove[a] {
			kept = append(kept, a)
		}
	}
	r.actors[taskID] = kept
	return nil
}

func (r *Repository) CreateCcInstance(ctx context.Context, instanceID int64, creator string, actorIDs ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ccInstances[instanceID] = append(r.ccInstances[instanceID], actorIDs...)
	return nil
}

func (r *Repository) UpdateCcStatus(ctx context.Context, instanceID int64, actorID string) error {
	return nil // 内存实现无已读状态
}

// PageCcInstances 我的抄送分页（v1.3.0）：按抄送人 actorID 过滤，join 实例 + 定义
func (r *Repository) PageCcInstances(ctx context.Context, query spi.PageQuery, actorID string) ([]*model.CcInstanceRow, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pageNum, pageSize := query.PageNum, query.PageSize
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	var rows []*model.CcInstanceRow
	for instID, actors := range r.ccInstances {
		if actorID != "" {
			hit := false
			for _, a := range actors {
				if a == actorID {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		inst, ok := r.instances[instID]
		if !ok {
			continue
		}
		row := &model.CcInstanceRow{
			ID:             inst.ID,
			ParentID:       inst.ParentID,
			DefineID:       inst.DefineID,
			State:          inst.State,
			ParentNodeName: inst.ParentNodeName,
			BusinessNo:     inst.BusinessNo,
			Operator:       inst.Operator,
			ExpireTime:     inst.ExpireTime,
			Variables:      inst.Variables,
			CreateTime:     inst.CreateTime,
			CreateUser:     inst.CreateUser,
			UpdateTime:     inst.UpdateTime,
			UpdateUser:     inst.UpdateUser,
		}
		if def, ok := r.defines[inst.DefineID]; ok {
			row.DefineName = def.Name
			row.DefineDisplayName = def.DisplayName
			row.DefineVersion = def.Version
		}
		rows = append(rows, row)
	}
	total := len(rows)
	// 简单分页（对齐 Java 按 id 升序的默认行为）
	start := (pageNum - 1) * pageSize
	if start >= total {
		return []*model.CcInstanceRow{}, total, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return rows[start:end], total, nil
}

// ─── Demo helpers ──────────────────────────────────────────────────────────────

func (r *Repository) AllDefines() []*model.ProcessDefine {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessDefine
	for _, d := range r.defines {
		cp := *d
		result = append(result, &cp)
	}
	return result
}

func (r *Repository) AllInstances() []*model.ProcessInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessInstance
	for _, inst := range r.instances {
		cp := *inst
		result = append(result, &cp)
	}
	return result
}

func (r *Repository) AllTasks() []*model.ProcessTask {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*model.ProcessTask
	for _, t := range r.tasks {
		cp := *t
		cp.ActorIDs = r.actors[t.ID]
		result = append(result, &cp)
	}
	return result
}

// ─── 核心表分页（v1.5.0） ──────────────────────────────────────────────────────

// PageDefines 流程定义分页
func (r *Repository) PageDefines(ctx context.Context, query spi.PageQuery) ([]*model.DefineRow, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var rows []*model.DefineRow
	for _, d := range r.defines {
		row := &model.DefineRow{
			ID: d.ID, Name: d.Name, DisplayName: d.DisplayName, Type: d.Type,
			State: d.State, Version: d.Version,
			CreateTime: d.CreateTime, CreateUser: d.CreateUser,
			UpdateTime: d.UpdateTime, UpdateUser: d.UpdateUser,
		}
		if matchConditions(query.Conditions, defineFields(row)) {
			rows = append(rows, row)
		}
	}
	return slicePage(rows, query), len(rows), nil
}

// PageInstances 我发起的流程实例分页（operator 过滤，join 定义名）
func (r *Repository) PageInstances(ctx context.Context, query spi.PageQuery, operator string) ([]*model.InstanceRow, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var rows []*model.InstanceRow
	for _, inst := range r.instances {
		if operator != "" && inst.Operator != operator {
			continue
		}
		row := &model.InstanceRow{
			ID: inst.ID, ParentID: inst.ParentID, DefineID: inst.DefineID, State: inst.State,
			ParentNodeName: inst.ParentNodeName, BusinessNo: inst.BusinessNo, Operator: inst.Operator,
			ExpireTime: inst.ExpireTime, Variables: inst.Variables,
			CreateTime: inst.CreateTime, CreateUser: inst.CreateUser,
			UpdateTime: inst.UpdateTime, UpdateUser: inst.UpdateUser,
		}
		if def, ok := r.defines[inst.DefineID]; ok {
			row.DefineName = def.Name
			row.DefineDisplayName = def.DisplayName
			row.DefineVersion = def.Version
		}
		if matchConditions(query.Conditions, instanceFields(row)) {
			rows = append(rows, row)
		}
	}
	return slicePage(rows, query), len(rows), nil
}

// PageTodoTasks 我的待办分页（actorID 过滤，仅进行中任务）
func (r *Repository) PageTodoTasks(ctx context.Context, query spi.PageQuery, actorID string) ([]*model.TaskRow, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var rows []*model.TaskRow
	for _, t := range r.tasks {
		if t.TaskState != model.TaskStateDoing {
			continue
		}
		if actorID != "" {
			hit := false
			for _, a := range r.actors[t.ID] {
				if a == actorID {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		row := r.taskRow(t)
		fields := taskFields(row)
		fields["pta.actor_id"] = r.actors[t.ID]
		if matchConditions(query.Conditions, fields) {
			rows = append(rows, row)
		}
	}
	return slicePage(rows, query), len(rows), nil
}

// PageDoneTasks 我的已办分页（operator 过滤，非进行中任务）
func (r *Repository) PageDoneTasks(ctx context.Context, query spi.PageQuery, operator string) ([]*model.TaskRow, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var rows []*model.TaskRow
	for _, t := range r.tasks {
		if t.TaskState == model.TaskStateDoing {
			continue
		}
		if operator != "" && t.ActorID != operator {
			continue
		}
		row := r.taskRow(t)
		if matchConditions(query.Conditions, taskFields(row)) {
			rows = append(rows, row)
		}
	}
	return slicePage(rows, query), len(rows), nil
}

// ═══ 条件匹配基建（issues/05-5，对齐 JDBC 白名单语义） ═══

// 行字段映射（列名 → 行属性，白名单列均可匹配）
func taskFields(r *model.TaskRow) map[string]interface{} {
	return map[string]interface{}{
		"t.id": r.ID, "t.task_name": r.TaskName, "t.display_name": r.DisplayName,
		"t.task_type": r.TaskType, "t.perform_type": r.PerformType, "t.task_state": r.TaskState,
		"t.operator": r.Operator, "t.form_key": r.FormKey, "t.create_time": r.CreateTime,
		"t.finish_time": r.FinishTime, "t.expire_time": r.ExpireTime,
		"t.process_instance_id": r.ProcessInstanceID, "t.task_parent_id": r.TaskParentID,
		"pd.name": r.ProcessDefineName, "pd.display_name": r.ProcessDefineDisplayName,
		"pd.version": r.DefineVersion,
	}
}

func instanceFields(r *model.InstanceRow) map[string]interface{} {
	return map[string]interface{}{
		"t.id": r.ID, "t.parent_id": r.ParentID, "t.process_define_id": r.DefineID,
		"t.state": r.State, "t.parent_node_name": r.ParentNodeName, "t.business_no": r.BusinessNo,
		"t.operator": r.Operator, "t.expire_time": r.ExpireTime, "t.create_time": r.CreateTime,
		"pd.name": r.DefineName, "pd.display_name": r.DefineDisplayName, "pd.version": r.DefineVersion,
	}
}

func defineFields(r *model.DefineRow) map[string]interface{} {
	return map[string]interface{}{
		"t.id": r.ID, "t.name": r.Name, "t.display_name": r.DisplayName, "t.type": r.Type,
		"t.state": r.State, "t.version": r.Version, "t.create_time": r.CreateTime,
		"t.update_time": r.UpdateTime,
	}
}

// matchConditions 条件全匹配（操作符对齐 JDBC buildWhere；列不在字段中则跳过）
func matchConditions(conditions []spi.Condition, fields map[string]interface{}) bool {
	for _, c := range conditions {
		v, ok := fields[c.Column]
		if !ok || v == nil {
			continue
		}
		expect := c.Value
		if expect == nil {
			continue
		}
		switch strings.ToUpper(c.Operator) {
		case "EQ":
			if !eqValue(v, expect) {
				return false
			}
		case "NE":
			if eqValue(v, expect) {
				return false
			}
		case "LIKE":
			if !strings.Contains(fmt.Sprint(v), fmt.Sprint(expect)) {
				return false
			}
		case "LLIKE":
			if !strings.HasSuffix(fmt.Sprint(v), fmt.Sprint(expect)) {
				return false
			}
		case "RLIKE":
			if !strings.HasPrefix(fmt.Sprint(v), fmt.Sprint(expect)) {
				return false
			}
		case "GT":
			if compareValues(v, expect) <= 0 {
				return false
			}
		case "GE":
			if compareValues(v, expect) < 0 {
				return false
			}
		case "LT":
			if compareValues(v, expect) >= 0 {
				return false
			}
		case "LE":
			if compareValues(v, expect) > 0 {
				return false
			}
		case "IN":
			if list, ok := expect.([]interface{}); ok && !containsAny(list, v) {
				return false
			}
		case "NIN":
			if list, ok := expect.([]interface{}); ok && containsAny(list, v) {
				return false
			}
		}
	}
	return true
}

// eqValue EQ 判断：值或集合包含（pta.actor_id/cc.actor_id 为切片）
func eqValue(v, expect interface{}) bool {
	if list, ok := v.([]string); ok {
		for _, a := range list {
			if a == fmt.Sprint(expect) {
				return true
			}
		}
		return false
	}
	return fmt.Sprint(v) == fmt.Sprint(expect)
}

func containsAny(list []interface{}, v interface{}) bool {
	sv := fmt.Sprint(v)
	for _, item := range list {
		if fmt.Sprint(item) == sv {
			return true
		}
	}
	return false
}

// compareValues 值比较：数字可比则数值比较，否则字符串比较
func compareValues(a, b interface{}) int {
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, bs := fmt.Sprint(a), fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// taskRow 构造任务行（join 实例 + 定义）
func (r *Repository) taskRow(t *model.ProcessTask) *model.TaskRow {
	row := &model.TaskRow{
		ID: t.ID, ProcessInstanceID: t.ProcessInstanceID, TaskName: t.TaskName,
		DisplayName: t.DisplayName, TaskType: t.TaskType, PerformType: t.PerformType,
		TaskState: t.TaskState, Operator: t.ActorID,
		FinishTime: t.FinishTime, ExpireTime: t.ExpireTime, FormKey: t.FormKey,
		TaskParentID: t.ParentTaskID, Variables: t.Variables,
		CreateTime: t.CreateTime, CreateUser: t.CreateUser,
		UpdateTime: t.UpdateTime, UpdateUser: t.UpdateUser,
	}
	if inst, ok := r.instances[t.ProcessInstanceID]; ok {
		row.InstanceCreateTime = inst.CreateTime
		if def, ok := r.defines[inst.DefineID]; ok {
			row.ProcessDefineName = def.Name
			row.ProcessDefineDisplayName = def.DisplayName
			row.DefineVersion = def.Version
		}
	}
	return row
}

// slicePage 简单分页切片
func slicePage[T any](rows []*T, query spi.PageQuery) []*T {
	pageNum, pageSize := query.PageNum, query.PageSize
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	start := (pageNum - 1) * pageSize
	if start >= len(rows) {
		return []*T{}
	}
	end := start + pageSize
	if end > len(rows) {
		end = len(rows)
	}
	return rows[start:end]
}
