// Package surrparity 委托查询「四判据」的**共用数据集 + 期望表**（issues/116 §5、
// 规范 08 用例 27）：内存仓与 SQL 仓跑同一份数据、比对同一份期望，
// 任何一仓答错即红——"同栈两仓不同答案"本身就是缺陷，不能各写各的断言。
//
// 四判据（规范 05-spi.md / 06-facade.md §4.5 第 5 条）：
//
//	a. 空 processName 全流程兜底（先精确、未命中再查 process_name IS NULL OR = ''）
//	b. 时间窗 start<=now<=end，任一侧 NULL = 该侧不限
//	c. 自委托过滤 surrogate <> operator
//	d. enabled 只认 1，非 1 值（含脏值）不得当启用
//
// 命中多行时的取行规则也在这里对拍：两仓都取 ID 最大者（对齐 SQL
// `ORDER BY id DESC LIMIT 1`）；内存 map 遍历序随机，不显式取最大即两仓结论不同。
package surrparity

import (
	"context"
	"time"

	"github.com/mldong/jeeflow-go/spi"
)

// T 最小测试报告面（本包不 import testing，可被任意包的测试复用）
type T interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

// OpPrefix 全部用例操作人/代理人的命名空间后缀——两仓用同一批人，
// SQL 侧按 id 段清理、按该后缀兜底清理。
const (
	OpPrefix = "go116"
	// Flow 用例流程名
	Flow = "leave116"
	// OtherFlow 与 Flow 不同的流程名（兜底用例用）
	OtherFlow = "other116"
	// IDLow/IDHigh SQL 侧测试数据 id 段
	IDLow  = int64(910000)
	IDHigh = int64(910099)
)

// Row 一条委托：时间用相对查询时刻的偏移表达，用例不随挂钟过期。
type Row struct {
	ID          int64
	Operator    string
	Surrogate   string
	ProcessName string
	StartOff    time.Duration
	HasStart    bool
	EndOff      time.Duration
	HasEnd      bool
	Enabled     int
	// NullProcessName true = process_name 存 NULL（内存仓落 ""，两者同属"空 processName
	// 全流程委托"这一逻辑类，判据 a 要求两仓都当兜底命中）
	NullProcessName bool
}

// Case 一次 GetSurrogate 查询及期望命中的 surrogate（"" = 期望不命中）
type Case struct {
	Name        string
	Operator    string
	ProcessName string
	QueryOff    time.Duration // 查询时刻 = now + QueryOff
	Want        string
}

func name(base string) string { return base + "-" + OpPrefix }

// Rows 共用数据集。分组：
//   - zs-*         优先级与兜底（同一授权人多条候选）
//   - solo-*       单判据隔离（每个只有一条委托，答错无法被其它判据掩盖）
func Rows(now time.Time) []Row {
	return []Row{
		// ── a. 精确优先 + 全流程兜底 + 取最大 ID ──
		{ID: 910001, Operator: name("zs"), Surrogate: name("exact-old"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910002, Operator: name("zs"), Surrogate: name("exact-new"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910003, Operator: name("zs"), Surrogate: name("all-flow"), ProcessName: "", StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910004, Operator: name("zs"), Surrogate: name("other-flow"), ProcessName: OtherFlow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910005, Operator: name("zs"), Surrogate: name("disabled"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0},
		// zs 组内的自委托行：ID 比生效行更大——实现若不过滤自委托，a1 就会答成 zs 自己
		{ID: 910006, Operator: name("zs"), Surrogate: name("zs"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		// 与"自委托"形似但不同的正常行：被委托人恰好叫 self-go116（字符串不同于 zs-go116）
		{ID: 910007, Operator: name("zs"), Surrogate: name("self"), ProcessName: name("self"), StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},

		// ── b. 时间窗（单侧 NULL = 该侧不限）──
		{ID: 910010, Operator: name("solo-window"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -10 * time.Hour, HasStart: true, EndOff: -9 * time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910011, Operator: name("solo-future"), Surrogate: name("agent"), ProcessName: Flow, StartOff: 9 * time.Hour, HasStart: true, Enabled: 1},
		{ID: 910012, Operator: name("solo-openend"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -9 * time.Hour, HasStart: true, EndOff: -1 * time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910013, Operator: name("solo-halfopen"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -9 * time.Hour, HasStart: true, Enabled: 1},
		{ID: 910014, Operator: name("solo-nowindow"), Surrogate: name("agent"), ProcessName: Flow, Enabled: 1},

		// ── d. enabled 只认 1 ──
		{ID: 910015, Operator: name("solo-disabled"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0},
		{ID: 910016, Operator: name("solo-dirty"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 2},
		{ID: 910017, Operator: name("solo-null"), Surrogate: name("agent"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},

		// ── c. 自委托过滤（两条路径各测一次：精确 / 全流程兜底）──
		{ID: 910020, Operator: name("solo-self-exact"), Surrogate: name("solo-self-exact"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		{ID: 910021, Operator: name("solo-self-all"), Surrogate: name("solo-self-all"), ProcessName: "", StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},
		// 自委托 + 全流程 NULL 组合：兜底路径也不得命中自己
		{ID: 910022, Operator: name("solo-self-all"), Surrogate: name("solo-self-all"), ProcessName: "", NullProcessName: true, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1},

		// ── a 补：process_name 为 NULL 的全流程兜底（单条、无干扰）──
		{ID: 910023, Operator: name("solo-nullflow"), Surrogate: name("agent"), ProcessName: "", NullProcessName: true, Enabled: 1},

		// ── 不串人：别人的委托不得命中当前授权人 ──
		{ID: 910030, Operator: name("someone-else"), Surrogate: name("agent"), ProcessName: Flow, Enabled: 1},
	}
}

// Cases 期望表：判据名 + 期望 surrogate。
func Cases(now time.Time) []Case {
	return []Case{
		// a：精确命中优先于全流程；两条精确命中取最大 ID
		{Name: "a1 精确命中取最大ID", Operator: name("zs"), ProcessName: Flow, Want: name("exact-new")},
		{Name: "a2 未精确命中→全流程兜底", Operator: name("zs"), ProcessName: "unmatched116", Want: name("all-flow")},
		{Name: "a3 查询传空流程名=只走兜底", Operator: name("zs"), ProcessName: "", Want: name("all-flow")},
		{Name: "a4 非空 processName 的行不得充当兜底（它 ID 更大）", Operator: name("zs"), ProcessName: "unmatched116", Want: name("all-flow")},
		{Name: "a5 NULL process_name 也属全流程兜底", Operator: name("solo-nullflow"), ProcessName: "any116", Want: name("agent")},

		// b：时间窗，单侧 NULL = 该侧不限
		{Name: "b1 窗口已过期", Operator: name("solo-window"), ProcessName: Flow, Want: ""},
		{Name: "b2 窗口未开始（结束侧 NULL）", Operator: name("solo-future"), ProcessName: Flow, Want: ""},
		{Name: "b3 窗口已过（开始侧有值）", Operator: name("solo-openend"), ProcessName: Flow, Want: ""},
		{Name: "b4 单侧 NULL=该侧不限（正向）", Operator: name("solo-halfopen"), ProcessName: Flow, Want: name("agent")},
		{Name: "b5 双侧 NULL=不限（正向）", Operator: name("solo-nowindow"), ProcessName: Flow, Want: name("agent")},
		{Name: "b6 未来窗口按未来时刻查询命中", Operator: name("solo-future"), ProcessName: Flow, QueryOff: 10 * time.Hour, Want: name("agent")},

		// d：enabled 只认 1
		{Name: "d1 enabled=0 停用", Operator: name("solo-disabled"), ProcessName: Flow, Want: ""},
		{Name: "d2 enabled=2 非 1 值不得当启用", Operator: name("solo-dirty"), ProcessName: Flow, Want: ""},
		// 停用行与生效行同授权人：必须只取生效那条（zs 的 disabled 行 ID 更大，
		// 若实现把"非 1"折叠成"启用"就会返回 disabled）
		{Name: "d3 停用行不参与取最大", Operator: name("zs"), ProcessName: Flow, Want: name("exact-new")},

		// c：自委托过滤
		{Name: "c1 精确路径自委托不生效", Operator: name("solo-self-exact"), ProcessName: Flow, Want: ""},
		{Name: "c2 兜底路径自委托不生效", Operator: name("solo-self-all"), ProcessName: "any116", Want: ""},
		{Name: "c3 委托给同名用户≠自委托（zs→self-go116 应生效）", Operator: name("zs"), ProcessName: name("self"), Want: name("self")},

		// 边界：无委托 / 别人的委托
		{Name: "e1 无委托授权人", Operator: name("nobody"), ProcessName: Flow, Want: ""},
		{Name: "e2 他人委托在其自身流程上不命中他流程", Operator: name("someone-else"), ProcessName: OtherFlow, Want: ""},
		// 正向对照：证明"他人委托"这行确实落库且生效——否则 e2 之类负向用例
		// 会因为"根本没数据"空转通过（issues/113 教训：断言要落在真落库的值上）
		{Name: "e3 正向对照：他人自己的委托生效", Operator: name("someone-else"), ProcessName: Flow, Want: name("agent")},
	}
}

// Run 用同一份数据 + 同一份期望跑一仓：seed 由各仓提供（内存仓直存，SQL 仓可写 NULL），
// 查询统一走 SPI 的 GetSurrogate。
func Run(t T, ext spi.ProcessExtRepository, seed func(Row) error, now time.Time) {
	t.Helper()
	ctx := context.Background()
	rows := Rows(now)
	for _, r := range rows {
		if err := seed(r); err != nil {
			t.Fatalf("种子数据落库失败 id=%d: %v", r.ID, err)
		}
	}
	// 种子自证：逐行读回，确认全部落库。缺这一步时"未命中"类期望会因为
	// 数据根本没进去而空转通过（issues/113 教训）。
	for _, r := range rows {
		back, err := ext.FindSurrogateByID(ctx, r.ID)
		if err != nil {
			t.Fatalf("读回种子 id=%d: %v", r.ID, err)
		}
		if back == nil {
			t.Fatalf("种子数据未落库 id=%d（operator=%s surrogate=%s）——负向期望将失去意义",
				r.ID, r.Operator, r.Surrogate)
		}
	}
	for _, c := range Cases(now) {
		hit, err := ext.GetSurrogate(ctx, c.Operator, c.ProcessName, now.Add(c.QueryOff))
		if err != nil {
			t.Fatalf("%s: GetSurrogate 报错 %v", c.Name, err)
		}
		got := ""
		if hit != nil {
			got = hit.Surrogate
		}
		if got != c.Want {
			t.Fatalf("判据对拍 [%s] GetSurrogate(operator=%s, processName=%s, at=now%v) 命中=%q，期望=%q",
				c.Name, c.Operator, c.ProcessName, c.QueryOff, got, c.Want)
		}
	}
}
