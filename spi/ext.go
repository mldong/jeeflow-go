package spi

import (
	"context"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

// ProcessExtRepository 扩展仓储 SPI（v1.1.0，可选）——流程设计 / 设计历史 / 委托代理
//
// 引擎核心不依赖本接口：设计稿与委托是"周边管理能力"，门面（Facade）与委托
// 参考实现使用。集成方不接入时，设计/委托功能由自身实现。
type ProcessExtRepository interface {
	// ── 流程设计（wf_process_design） ──
	FindDesignByID(ctx context.Context, id int64) (*model.ProcessDesign, error)
	SaveDesign(ctx context.Context, d *model.ProcessDesign) error
	UpdateDesign(ctx context.Context, d *model.ProcessDesign) error
	RemoveDesign(ctx context.Context, id int64) error
	PageDesigns(ctx context.Context, query PageQuery) ([]*model.ProcessDesign, int, error)

	// ── 设计历史（wf_process_design_his） ──
	SaveDesignHis(ctx context.Context, his *model.ProcessDesignHis) error
	ListDesignHis(ctx context.Context, designID int64) ([]*model.ProcessDesignHis, error)

	// ── 委托代理（wf_process_surrogate） ──
	FindSurrogateByID(ctx context.Context, id int64) (*model.ProcessSurrogate, error)
	SaveSurrogate(ctx context.Context, s *model.ProcessSurrogate) error
	UpdateSurrogate(ctx context.Context, s *model.ProcessSurrogate) error
	RemoveSurrogate(ctx context.Context, id int64) error
	PageSurrogates(ctx context.Context, query PageQuery) ([]*model.ProcessSurrogate, int, error)

	// GetSurrogate 查询指定时间生效中的委托：enabled=1 且时间窗内（起止为空表示不限）。
	// 优先 processName 精确匹配，其次 processName 为空的"全流程委托"兜底。
	GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error)
}

// PageQuery 分页查询参数（v1.1.0，扩展仓储分页用）
type PageQuery struct {
	PageNum  int
	PageSize int
	// 简单条件：字段名（无别名）→ 值，EQ 匹配（扩展仓储分页的最小集）
	Filters map[string]interface{}
}
