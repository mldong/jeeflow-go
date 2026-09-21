// 扩展仓储内存实现（v1.1.0，测试用）
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ExtRepository 是 ProcessExtRepository 的内存实现（测试/演示用）。
type ExtRepository struct {
	mu         sync.RWMutex
	designs    map[int64]*model.ProcessDesign
	designHis  map[int64][]*model.ProcessDesignHis
	surrogates map[int64]*model.ProcessSurrogate
	nextID     int64
}

func NewExt() *ExtRepository {
	return &ExtRepository{
		designs:    make(map[int64]*model.ProcessDesign),
		designHis:  make(map[int64][]*model.ProcessDesignHis),
		surrogates: make(map[int64]*model.ProcessSurrogate),
		nextID:     1,
	}
}

// ── 流程设计 ──────────────────────────────────────────────────────────────────

func (r *ExtRepository) FindDesignByID(ctx context.Context, id int64) (*model.ProcessDesign, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.designs[id]
	if !ok {
		return nil, nil
	}
	cp := *d
	return &cp, nil
}

func (r *ExtRepository) SaveDesign(ctx context.Context, d *model.ProcessDesign) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d.ID == 0 {
		d.ID = r.nextID
		r.nextID++
	}
	now := time.Now()
	if d.CreateTime.IsZero() {
		d.CreateTime = now
	}
	if d.UpdateTime.IsZero() {
		d.UpdateTime = now
	}
	cp := *d
	r.designs[d.ID] = &cp
	return nil
}

func (r *ExtRepository) UpdateDesign(ctx context.Context, d *model.ProcessDesign) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d.UpdateTime = time.Now()
	cp := *d
	r.designs[d.ID] = &cp
	return nil
}

func (r *ExtRepository) RemoveDesign(ctx context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.designs, id)
	delete(r.designHis, id)
	return nil
}

func (r *ExtRepository) PageDesigns(ctx context.Context, query spi.PageQuery) ([]*model.ProcessDesign, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []*model.ProcessDesign
	for _, d := range r.designs {
		cp := *d
		list = append(list, &cp)
	}
	return list, len(list), nil
}

// ── 设计历史 ──────────────────────────────────────────────────────────────────

func (r *ExtRepository) SaveDesignHis(ctx context.Context, his *model.ProcessDesignHis) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if his.ID == 0 {
		his.ID = r.nextID
		r.nextID++
	}
	if his.CreateTime.IsZero() {
		his.CreateTime = time.Now()
	}
	cp := *his
	r.designHis[his.ProcessDesignID] = append([]*model.ProcessDesignHis{&cp}, r.designHis[his.ProcessDesignID]...)
	return nil
}

func (r *ExtRepository) ListDesignHis(ctx context.Context, designID int64) ([]*model.ProcessDesignHis, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []*model.ProcessDesignHis
	for _, h := range r.designHis[designID] {
		cp := *h
		list = append(list, &cp)
	}
	return list, nil
}

// ── 委托代理 ──────────────────────────────────────────────────────────────────

func (r *ExtRepository) FindSurrogateByID(ctx context.Context, id int64) (*model.ProcessSurrogate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.surrogates[id]
	if !ok {
		return nil, nil
	}
	cp := *s
	return &cp, nil
}

func (r *ExtRepository) SaveSurrogate(ctx context.Context, s *model.ProcessSurrogate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.ID == 0 {
		s.ID = r.nextID
		r.nextID++
	}
	now := time.Now()
	if s.CreateTime.IsZero() {
		s.CreateTime = now
	}
	if s.UpdateTime.IsZero() {
		s.UpdateTime = now
	}
	// 不在仓储层把 enabled=0 改回 1：显式停用是合法值，缺省（未传 enabled）由门面
	// parseSurrogateEnabled 兜底为 1（对齐 Java 可空 Integer 的 null 判断，issues/82-7；
	// 脏值按 issues/116 判据 d 落 0＝停用，不再回落 1）
	cp := *s
	r.surrogates[s.ID] = &cp
	return nil
}

func (r *ExtRepository) UpdateSurrogate(ctx context.Context, s *model.ProcessSurrogate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s.UpdateTime = time.Now()
	cp := *s
	r.surrogates[s.ID] = &cp
	return nil
}

func (r *ExtRepository) RemoveSurrogate(ctx context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.surrogates, id)
	return nil
}

func (r *ExtRepository) PageSurrogates(ctx context.Context, query spi.PageQuery) ([]*model.ProcessSurrogate, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// 对齐 JDBC PageSurrogates（issues/82-7）：Filters（operator/surrogate/process_name/enabled）
	// + m_ 条件（matchConditions，列白名单 t.*）+ t.id DESC 排序 + 分页切片
	var matched []*model.ProcessSurrogate
	for _, s := range r.surrogates {
		if !matchSurrogateFilters(s, query.Filters) {
			continue
		}
		if !matchConditions(query.Conditions, surrogateFields(s)) {
			continue
		}
		cp := *s
		matched = append(matched, &cp)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID > matched[j].ID })
	return slicePage(matched, query), len(matched), nil
}

// matchSurrogateFilters Filters 简单条件（对齐 JDBC operator/surrogate/process_name/enabled 白名单）
func matchSurrogateFilters(s *model.ProcessSurrogate, filters map[string]interface{}) bool {
	for col, val := range filters {
		if val == nil {
			continue
		}
		switch col {
		case "operator":
			if !eqValue(s.Operator, val) {
				return false
			}
		case "surrogate":
			if !eqValue(s.Surrogate, val) {
				return false
			}
		case "process_name":
			if !eqValue(s.ProcessName, val) {
				return false
			}
		case "enabled":
			if !eqValue(s.Enabled, val) {
				return false
			}
		}
	}
	return true
}

// surrogateFields 委托行字段（t.* 键，对齐 JDBC surrogateWhitelist 列名）
func surrogateFields(s *model.ProcessSurrogate) map[string]interface{} {
	return map[string]interface{}{
		"t.id":           s.ID,
		"t.process_name": s.ProcessName,
		"t.operator":     s.Operator,
		"t.surrogate":    s.Surrogate,
		"t.enabled":      s.Enabled,
		"t.start_time":   s.StartTime,
		"t.end_time":     s.EndTime,
		"t.create_time":  s.CreateTime,
		"t.update_time":  s.UpdateTime,
	}
}

// GetSurrogate 查询指定时间生效中的委托。
//
// 判据与 JDBC 实现（repository/jdbc/ext.go querySurrogate）**逐条等价**（issues/116 §5：
// 同一份数据在同一栈的内存仓与 SQL 仓不得给出不同结论）：
//  1. enabled 只认 1（其余值含 0 一律不生效）；
//  2. 时间窗 start<=at<=end，任一侧为 nil 表示该侧不限（at 为零值时整窗不限，
//     对齐 SQL 侧 `if !at.IsZero()` 分支）；
//  3. 自委托过滤 surrogate <> operator（自己委托给自己不生效）——本方法此前缺这条，
//     与 JDBC 侧 `AND surrogate <> ?` 分叉，同栈两仓结论相反；
//  4. 先按 processName 精确匹配，未命中再用空 processName（全流程委托）兜底；
//  5. 多行命中取 ID 最大者（对齐 SQL `ORDER BY id DESC LIMIT 1`）。内存 map 遍历序随机，
//     不显式取最大即"同一份数据两次调用可能返回不同行"。
func (r *ExtRepository) GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if processName != "" {
		if hit := r.pickSurrogateLocked(operator, processName, at, false); hit != nil {
			return hit, nil
		}
	}
	return r.pickSurrogateLocked(operator, "", at, true), nil
}

// pickSurrogateLocked 按判据取一条生效委托；allFlows=true 表示只匹配"全流程委托"
// （process_name 为空）。调用方须持读锁。
func (r *ExtRepository) pickSurrogateLocked(operator, processName string, at time.Time, allFlows bool) *model.ProcessSurrogate {
	var hit *model.ProcessSurrogate
	for _, s := range r.surrogates {
		if s.Operator != operator {
			continue
		}
		if s.Enabled != 1 {
			continue
		}
		if s.Surrogate == operator {
			continue
		}
		if !at.IsZero() {
			if s.StartTime != nil && s.StartTime.After(at) {
				continue
			}
			if s.EndTime != nil && s.EndTime.Before(at) {
				continue
			}
		}
		if allFlows {
			if s.ProcessName != "" {
				continue
			}
		} else if s.ProcessName != processName {
			continue
		}
		if hit == nil || s.ID > hit.ID {
			cp := *s
			hit = &cp
		}
	}
	return hit
}
