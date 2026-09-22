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

	// GetSurrogate 查询指定时间生效中的委托（规范 06 §4.5 条款 1.4 + issues/123）：
	// 先按主键 id 取该授权人在该流程作用域内的**最新一条**（不带生效判据过滤），再由
	// model.ProcessSurrogate.IsEffective 裁决四判据（enabled 严格 ==1 / 时间窗覆盖 at，
	// 起止为空 = 该侧不限 / 自委托不生效）；at 为零值时不做窗口比较。
	// 同层内最新一条判否 ⇒ 直接返回 nil，**不得**回落到该层更旧的记录；
	// 但精确作用域判否（含无记录）后仍要看 processName 为空的"全流程委托"作用域的最新一条。
	// 优先 processName 精确匹配，其次 processName 为空的"全流程委托"兜底。
	GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error)
}

// Condition 查询条件（issues/05-5：m_ 前缀参数解析产物，对齐 Java PageQuery.Condition）
type Condition struct {
	Column   string
	Operator string
	Value    interface{}
}

// PageQuery 分页查询参数（v1.1.0，扩展仓储分页用）
type PageQuery struct {
	PageNum  int
	PageSize int
	// 简单条件：字段名（无别名）→ 值，EQ 匹配（扩展仓储分页的最小集）
	Filters map[string]interface{}
	// 通用条件（issues/05-5）：白名单 + 参数化过滤（对齐 Java buildWhere）
	Conditions []Condition
}
