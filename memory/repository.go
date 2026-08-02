package memory

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/mldong/jeeflow-go/model"
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
