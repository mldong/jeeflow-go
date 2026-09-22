// Package surrparity 委托查询「四判据」的**共用数据集 + 期望表**（issues/116 §5、
// 规范 08 用例 27、issues/123）：内存仓与 SQL 仓跑同一份数据、比对同一份期望，
// 任何一仓答错即红——"同栈两仓不同答案"本身就是缺陷，不能各写各的断言。
//
// 取行 + 裁决的顺序（规范 06 §4.5 条款 1.4 + issues/123，**本包按此顺序建期望**）：
//
//	① 先按流程作用域取「最新一条」（processName 精确作用域里有记录就用它；
//	   一条都没有才看 process_name 为空的全流程作用域）——只按主键 id 排序，不带生效判据；
//	② 再由四判据裁决**这一条**：
//	   d. enabled 严格 == 1（0 / 2 / 脏值 / NULL 一律不生效）
//	   c. 自委托过滤（surrogate == operator 不生效；surrogate 为空同理）
//	   b. 时间窗 start<=now<=end，任一侧 NULL = 该侧不限
//	③ 不生效 ⇒ 返回"未命中"，**不回落**到更旧那条，也**不回落**到另一流程作用域。
//
// ⚠️ 反过来写（①② 对调：先用 enabled/窗口/自委托把记录滤掉，剩下的才取最新）就是
// issues/123 的病灶——同一授权人历史上只要留过一条"窗内且 enabled=1"的记录，之后用户
// 新建的窗外 / enabled=0 / 脏值 / 自委托记录全都判不动它，代理人被永久并入。
// 老夹具里的 d3「停用行 ID 比生效行更大 ⇒ 仍答生效行」正是这种错法的期望，
// issues/123 已把它改写成 f/g/h 组「最新一条不生效 ⇒ 不命中且不回落」。
//
// ⚠️ **夹具的 id 序是刻意打乱的**（条款 1.4 的判别力所在）：多条命中那一组里，
// 插入顺序给的是 id 910002 → 910003 → 910001，即"裁决行（最大 id）"既不是插入
// 首条也不是插入末条。于是三种写法的结论互相可分：
//   - 取遍历首条 → 910002（红）
//   - 取插入末条 / 边扫边覆盖 → 910001（红）
//   - 取 id 最大 → 910003（绿，期望）
//
// 负向组（f/g/h）同理需要三条：裁决行夹在两条"更旧的生效行"中间，否则
// "取插入末条"的错误实现会因为末条恰好就是裁决行而跟着夹具一起绿（Verify 会当场红）。
//
// ⚠️ 已知事实（Java 实测踩到）：**SQL 侧这种打乱没有判别力**——H2/InnoDB 对
// `WHERE operator=? ORDER BY id DESC` 本来就按主键序回行，插入序在结果里根本不出现，
// 打乱与否答案都一样。钉住条款 1.4 的在 SQL 侧是 `ORDER BY id DESC` 这个排序子句本身
// （去掉它才会退化成"取物理首行"）；插入序打乱真正起作用的是**内存侧**（切片/映射实现
// 若按插入序取首条或末条即红）。两侧仍必须各跑一遍并对同一答案负责。
package surrparity

import (
	"context"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/spi"
)

// T 最小测试报告面（本包不 import testing，可被任意包的测试复用）
type T interface {
	Helper()
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
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
//   - zs-*         优先级与兜底（同一授权人多条候选，**id 序已刻意打乱**）
//   - zs2-*        兜底路径的多条候选（同样打乱，钉住兜底分支也按 id 取最大）
//   - solo-*       单判据隔离（每个只有一条委托，答错无法被其它判据掩盖）
//   - stale-*      issues/123：最新一条按某判据不生效 ⇒ 不命中，且不得回落到更旧的生效行
//   - scope-fb     issues/123 条款 1.4：精确作用域最新一条判否 ⇒ 仍须回落到生效的全流程行
//   - gscope-stale issues/123：同一规则在兜底（全局）作用域内同样成立
//   - only-effective / newest-ok  正向对照（判据写反成"恒不命中"时立刻红）
func Rows(now time.Time) []Row {
	win := Row{StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1}
	return []Row{
		// ── a. 精确优先 + 全流程兜底 + 取最大 ID ──
		// 三条"精确命中且生效"的候选按 id 910002 → 910003 → 910001 的顺序插入：
		// 期望的 910003 既非插入首条也非插入末条（判别力见包注释）。
		eff(910002, name("zs"), name("exact-first"), Flow, win),
		eff(910003, name("zs"), name("exact-max"), Flow, win),
		eff(910001, name("zs"), name("exact-last"), Flow, win),
		// 兜底行：a2/a3/a4 的期望。ID 必须小于下面 other-flow 行，才能钉住
		// "非空 processName 的行不得充当兜底"（兜底若不判空就会答成 other-flow）。
		eff(910005, name("zs"), name("all-flow"), "", win),
		eff(910007, name("zs"), name("other-flow"), OtherFlow, win),
		// 与"自委托"形似但不同的正常行：被委托人恰好叫 self-go116（字符串不同于 zs-go116）
		eff(910009, name("zs"), name("self"), name("self"), win),

		// ── a 补：兜底分支的多条候选（条款 1.4 在兜底路径同样要打乱序才看得出）──
		// 三条 process_name 为空（'' 与 NULL 混着放，两仓都属同一逻辑类）的生效兜底行，
		// 插入序 id 910042 → 910043 → 910041，期望 910043（最大，且它在 SQL 侧是 NULL，
		// 兜底若只写 process_name = '' 就会答成 910042）。
		eff(910042, name("zs2"), name("all2-first"), "", win),
		effNullFlow(910043, name("zs2"), name("all2-max"), win),
		eff(910041, name("zs2"), name("all2-last"), "", win),

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

		// ── f. issues/123 任务 A：最新一条不生效 ⇒ 不命中，且不得回落到更旧那条 ──
		// 每组三条、插入序固定为「生效旧行 → 裁决行(最大 id，不生效) → 生效旧行」：
		// 首末两条都是生效行，才能把"取遍历首条""取插入末条"两种错法一并问住（Verify 会核）。
		eff(910050, name("stale-window"), name("w-older1"), Flow, win),
		{ID: 910053, Operator: name("stale-window"), Surrogate: name("w-newest"), ProcessName: Flow, StartOff: 9 * time.Hour, HasStart: true, EndOff: 10 * time.Hour, HasEnd: true, Enabled: 1}, // 窗外
		eff(910052, name("stale-window"), name("w-older2"), Flow, win),

		eff(910054, name("stale-disabled"), name("d-older1"), Flow, win),
		{ID: 910056, Operator: name("stale-disabled"), Surrogate: name("d-newest"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0}, // enabled=0
		eff(910055, name("stale-disabled"), name("d-older2"), Flow, win),

		eff(910057, name("stale-dirty"), name("x-older1"), Flow, win),
		{ID: 910059, Operator: name("stale-dirty"), Surrogate: name("x-newest"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 2}, // 脏值
		eff(910058, name("stale-dirty"), name("x-older2"), Flow, win),

		eff(910060, name("stale-self"), name("s-older1"), Flow, win),
		{ID: 910062, Operator: name("stale-self"), Surrogate: name("stale-self"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1}, // 自委托
		eff(910061, name("stale-self"), name("s-older2"), Flow, win),

		eff(910063, name("stale-empty"), name("e-older1"), Flow, win),
		{ID: 910065, Operator: name("stale-empty"), Surrogate: "", ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 1}, // 被委托人为空
		eff(910064, name("stale-empty"), name("e-older2"), Flow, win),

		// ── g. issues/123：精确作用域里有记录就由它裁决，不得跨作用域回落到生效的全局行 ──
		eff(910066, name("scope-fb"), name("g-older"), Flow, win),
		{ID: 910068, Operator: name("scope-fb"), Surrogate: name("g-newest-off"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0}, // 最新精确行：停用
		eff(910067, name("scope-fb"), name("g-lastinsert"), Flow, win),
		eff(910069, name("scope-fb"), name("g-global"), "", win), // 生效的全局行，id 比谁都大

		// ── h. issues/123：兜底（全局）作用域内同样"最新一条裁决、不回落更旧" ──
		eff(910070, name("gscope-stale"), name("h-older1"), "", win),
		{ID: 910072, Operator: name("gscope-stale"), Surrogate: name("h-newest-off"), ProcessName: "", StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0},
		eff(910071, name("gscope-stale"), name("h-older2"), "", win),

		// ── i/j. 正向对照（issues/123 任务 B）：判据被写反成"恒不命中"时这两条立刻红 ──
		eff(910073, name("only-effective"), name("agent"), Flow, win), // 作用域内只有一条窗内 enabled=1
		{ID: 910081, Operator: name("newest-ok"), Surrogate: name("k-disabled"), ProcessName: Flow, StartOff: -time.Hour, HasStart: true, EndOff: time.Hour, HasEnd: true, Enabled: 0},
		eff(910083, name("newest-ok"), name("k-newest"), Flow, win), // 最新一条生效 ⇒ 命中它
		{ID: 910082, Operator: name("newest-ok"), Surrogate: name("k-expired"), ProcessName: Flow, StartOff: -10 * time.Hour, HasStart: true, EndOff: -9 * time.Hour, HasEnd: true, Enabled: 1},
	}
}

// eff 造一条"窗内 + enabled=1"的生效行（w 是窗口模板）
func eff(id int64, operator, surrogate, processName string, w Row) Row {
	return Row{ID: id, Operator: operator, Surrogate: surrogate, ProcessName: processName,
		StartOff: w.StartOff, HasStart: w.HasStart, EndOff: w.EndOff, HasEnd: w.HasEnd, Enabled: 1}
}

// effNullFlow 同 eff，但 process_name 存 NULL（与空串同属全流程兜底这一逻辑类）
func effNullFlow(id int64, operator, surrogate string, w Row) Row {
	r := eff(id, operator, surrogate, "", w)
	r.NullProcessName = true
	return r
}

// Cases 期望表：判据名 + 期望 surrogate。
func Cases(now time.Time) []Case {
	return []Case{
		// a：精确命中优先于全流程；多条精确命中取最大 ID
		// （夹具的 id 序已打乱：910002→910003→910001，故这里同时钉住"既不取遍历首条、
		// 也不取插入末条"；SQL 侧的钉住点是 ORDER BY id DESC，见包注释）
		{Name: "a1 多条精确命中取最大ID（非遍历首条/非插入末条）", Operator: name("zs"), ProcessName: Flow, Want: name("exact-max")},
		{Name: "a2 未精确命中→全流程兜底", Operator: name("zs"), ProcessName: "unmatched116", Want: name("all-flow")},
		{Name: "a3 查询传空流程名=只走兜底", Operator: name("zs"), ProcessName: "", Want: name("all-flow")},
		{Name: "a4 非空 processName 的行不得充当兜底（它 ID 更大）", Operator: name("zs"), ProcessName: "unmatched116", Want: name("all-flow")},
		{Name: "a5 NULL process_name 也属全流程兜底", Operator: name("solo-nullflow"), ProcessName: "any116", Want: name("agent")},
		// 兜底分支自己也要按 id 取最大（精确分支答对不代表兜底分支答对——两分支在
		// 两仓里都是各查一次的独立代码路径）
		{Name: "a6 多条兜底命中同样取最大ID（含 NULL 行）", Operator: name("zs2"), ProcessName: Flow, Want: name("all2-max")},

		// b：时间窗，单侧 NULL = 该侧不限
		{Name: "b1 窗口已过期", Operator: name("solo-window"), ProcessName: Flow, Want: ""},
		{Name: "b2 窗口未开始（结束侧 NULL）", Operator: name("solo-future"), ProcessName: Flow, Want: ""},
		{Name: "b3 窗口已过（开始侧有值）", Operator: name("solo-openend"), ProcessName: Flow, Want: ""},
		{Name: "b4 单侧 NULL=该侧不限（正向）", Operator: name("solo-halfopen"), ProcessName: Flow, Want: name("agent")},
		{Name: "b5 双侧 NULL=不限（正向）", Operator: name("solo-nowindow"), ProcessName: Flow, Want: name("agent")},
		{Name: "b6 未来窗口按未来时刻查询命中", Operator: name("solo-future"), ProcessName: Flow, QueryOff: 10 * time.Hour, Want: name("agent")},

		// d：enabled 只认 1（单条隔离）
		{Name: "d1 enabled=0 停用", Operator: name("solo-disabled"), ProcessName: Flow, Want: ""},
		{Name: "d2 enabled=2 非 1 值不得当启用", Operator: name("solo-dirty"), ProcessName: Flow, Want: ""},

		// c：自委托过滤
		{Name: "c1 精确路径自委托不生效", Operator: name("solo-self-exact"), ProcessName: Flow, Want: ""},
		{Name: "c2 兜底路径自委托不生效", Operator: name("solo-self-all"), ProcessName: "any116", Want: ""},
		{Name: "c3 委托给同名用户≠自委托（zs→self-go116 应生效）", Operator: name("zs"), ProcessName: name("self"), Want: name("self")},

		// f：issues/123 任务 A —— 同作用域里"更旧的生效行"不得替最新那条说话
		{Name: "f1 最新一条窗外 ⇒ 不命中、不回落到更旧生效行", Operator: name("stale-window"), ProcessName: Flow, Want: ""},
		{Name: "f1b 同数据按未来时刻查询 ⇒ 那条未来窗口记录生效", Operator: name("stale-window"), ProcessName: Flow, QueryOff: 9*time.Hour + 30*time.Minute, Want: name("w-newest")},
		{Name: "f2 最新一条 enabled=0 ⇒ 不命中、不回落", Operator: name("stale-disabled"), ProcessName: Flow, Want: ""},
		{Name: "f3 最新一条 enabled=2 脏值 ⇒ 不命中、不回落", Operator: name("stale-dirty"), ProcessName: Flow, Want: ""},
		{Name: "f4 最新一条自委托 ⇒ 不命中、不回落", Operator: name("stale-self"), ProcessName: Flow, Want: ""},
		{Name: "f5 最新一条被委托人为空 ⇒ 不命中、不回落", Operator: name("stale-empty"), ProcessName: Flow, Want: ""},

		// g/h：不回落的两条独立路径（跨流程作用域 / 兜底作用域内更旧行）
		{Name: "g1 精确作用域最新一条停用 ⇒ 仍兜底到生效的全流程行", Operator: name("scope-fb"), ProcessName: Flow, Want: name("g-global")},
		{Name: "h1 兜底作用域最新一条停用 ⇒ 不命中、不回落更旧全局生效行", Operator: name("gscope-stale"), ProcessName: "unmatched116", Want: ""},

		// i/j：正向对照（issues/123 任务 B）
		{Name: "i1 正向：作用域内只有一条窗内 enabled=1 ⇒ 命中", Operator: name("only-effective"), ProcessName: Flow, Want: name("agent")},
		{Name: "j1 正向：最新一条生效、更旧两条不生效 ⇒ 命中最新那条", Operator: name("newest-ok"), ProcessName: Flow, Want: name("k-newest")},

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
	passed := 0
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
			t.Fatalf("判据对拍 [%s] GetSurrogate(operator=%s, processName=%s, at=now%v) 命中=%q，期望=%q"+
				"（issues/123：先取该作用域 id 最新一条，再由四判据裁决这一条；同层内不生效即不命中、" +
					"不回落到更旧那条，但精确作用域判否后仍要看全流程作用域）",
				c.Name, c.Operator, c.ProcessName, c.QueryOff, got, c.Want)
		}
		passed++
	}
	t.Logf("判据对拍全绿：%d 条种子行、%d 条判据（内存仓与 SQL 仓跑的是同一份数据 + 同一份期望）",
		len(rows), passed)
}

// ─── 夹具自证（不依赖任何仓储实现）─────────────────────────────────────────────

// rowEffective 单行四判据（与 model.ProcessSurrogate.IsEffective 同口径，偏移直接比：
// start<=at<=end ⇔ StartOff<=QueryOff<=EndOff，偏移都相对同一起点）。
func rowEffective(r Row, at time.Duration) bool {
	if r.Enabled != 1 {
		return false
	}
	agent := strings.TrimSpace(r.Surrogate)
	if agent == "" || agent == r.Operator {
		return false
	}
	return !(r.HasStart && r.StartOff > at) && !(r.HasEnd && r.EndOff < at)
}

// scopeRows 按判据 a 的两段式解析出「裁决池」：processName 非空时先取该流程精确作用域，
// 精确作用域一条记录都没有才看全流程作用域（process_name 为空）。
// **只看作用域、不看生效判据**——生效与否由池内 id 最大那一条单独裁决（issues/123）。
func scopeRows(rows []Row, c Case) (scope []Row, fromGlobal bool) {
	if c.ProcessName != "" {
		for _, r := range rows {
			if r.Operator == c.Operator && r.ProcessName == c.ProcessName {
				scope = append(scope, r)
			}
		}
		if len(scope) > 0 {
			return scope, false
		}
	}
	for _, r := range rows {
		if r.Operator == c.Operator && r.ProcessName == "" {
			scope = append(scope, r)
		}
	}
	return scope, true
}

// newestIdx 池内 id 最大（最新）那条的下标；池空返回 -1。
// adjudicate 是夹具的**参考答案模型**（规范 06 §4.5 条款 1.4 + issues/123，与 Java 参考实现
// JdbcProcessExtRepositoryTest#testSurrogateCrudAndGet 同结论）：
// 精确作用域先取 id 最新一条交四判据裁决；判否（含该作用域没有记录）再看全流程作用域的最新一条。
// 同层内绝不回落到更旧那条——那正是 issues/123 的病灶。
func adjudicate(rows []Row, c Case, at time.Duration) (hit string, fromGlobal bool) {
	if c.ProcessName != "" {
		var exact []Row
		for _, r := range rows {
			if r.Operator == c.Operator && r.ProcessName == c.ProcessName {
				exact = append(exact, r)
			}
		}
		if d := newestIdx(exact); d >= 0 && rowEffective(exact[d], at) {
			return exact[d].Surrogate, false
		}
	}
	var global []Row
	for _, r := range rows {
		if r.Operator == c.Operator && r.ProcessName == "" {
			global = append(global, r)
		}
	}
	if d := newestIdx(global); d >= 0 && rowEffective(global[d], at) {
		return global[d].Surrogate, true
	}
	return "", false
}

func newestIdx(scope []Row) int {
	idx := -1
	for i, r := range scope {
		if idx < 0 || r.ID > scope[idx].ID {
			idx = i
		}
	}
	return idx
}

// Verify 钉住**夹具自己**的判别力（条款 1.4 + issues/123）：每个判据按「先取作用域内
// id 最新一条，再由四判据裁决这一条」算出来的答案，必须与期望表一致，并且：
//
//  1. 正向判据且裁决池 ≥2 条时，那条最大 id 记录的**插入位次既不能是首条也不能是末条**——
//     否则"取遍历首条""边扫边覆盖取末条"的错误实现会跟着夹具一起绿；
//  2. 负向判据（期望不命中）若池内另有**更旧的生效行**，同样要求裁决行既非首条也非末条，
//     且这条用例才算真正在测 issues/123 的"不得回落到更旧那条"；
//  3. 至少各存在一条：正向判据（防判据写反成"恒不命中"）、多条命中取最大（条款 1.4）、
//     同作用域不回落更旧行（issues/123 任务 A）、精确作用域判否后由全流程作用域兜底命中。
//
// 两仓测试各调一次（数据同一份、性质同一份），夹具退化时立刻红，不用等实现出错。
func Verify(t T, now time.Time) {
	t.Helper()
	rows := Rows(now)
	positive, multiHit, noOlderFallback, scopeFallbackHit := 0, 0, 0, 0
	for _, c := range Cases(now) {
		at := c.QueryOff
		scope, _ := scopeRows(rows, c)
		d := newestIdx(scope)
		got, hitFromGlobal := adjudicate(rows, c, at)
		if got != c.Want {
			t.Fatalf("夹具自证：判据 [%s] 按「先取 id 最新一条再裁决四判据、精确判否再兜底全流程」应为 %q，期望表却写了 %q"+
				"——期望与数据集不自洽（issues/123）", c.Name, got, c.Want)
		}
		if hitFromGlobal && c.Want != "" {
			scopeFallbackHit++ // 精确作用域判否 ⇒ 由全流程作用域救回（条款 1.4 后半句）
		}
		if c.Want != "" {
			positive++
			if len(scope) >= 2 {
				multiHit++
				if d == 0 || d == len(scope)-1 {
					t.Fatalf("夹具失去判别力：判据 [%s] 有 %d 条同作用域记录，裁决那条(id=%d)却落在插入序第 %d 位"+
						"（首位/末位）——「取遍历首条」「取插入末条」的错误实现会跟着这份夹具一起绿。"+
						"须把 id 序打乱到裁决行既非首条也非末条",
						c.Name, len(scope), scope[d].ID, d+1)
				}
			}
			continue
		}
		// 负向判据：池内是否另有"更旧的生效行"——有，才算在测"不得回落到更旧那条"
		olderEffective := false
		for i, r := range scope {
			if i != d && d >= 0 && r.ID < scope[d].ID && rowEffective(r, at) {
				olderEffective = true
			}
		}
		if olderEffective {
			noOlderFallback++
			if d == 0 || d == len(scope)-1 {
				t.Fatalf("夹具失去判别力：判据 [%s] 的裁决行(id=%d)落在插入序第 %d 位，而池内另有更旧的生效行"+
					"——「取插入末条」的错误实现会因末条恰是裁决行而跟着夹具一起绿。须把生效行补到首末两位",
					c.Name, scope[d].ID, d+1)
			}
		}
	}
	if positive == 0 {
		t.Fatalf("夹具自证：没有任何正向判据——判据若被写反成「恒不命中」不会被发现（issues/123 任务 B）")
	}
	if multiHit == 0 {
		t.Fatalf("夹具自证：没有任何一个判据存在 ≥2 条同作用域记录——条款 1.4「多条命中取 id 最大」" +
			"根本没被对拍到")
	}
	if noOlderFallback == 0 {
		t.Fatalf("夹具自证：没有任何一条「最新一条不生效 + 同作用域另有更旧生效行」的判据——" +
			"issues/123 的核心「不得回落到更旧那条」根本没对拍到")
	}
	if scopeFallbackHit == 0 {
		t.Fatalf("夹具自证：没有任何一条「精确作用域最新一条判否 ⇒ 由全流程作用域兜底命中」的判据——" +
			"条款 1.4 的后半句（判否仍要看全流程作用域）根本没对拍到")
	}
	t.Logf("夹具自证通过：正向 %d 条、多条命中取最大 %d 条、同层不回落更旧 %d 条、跨层兜底命中 %d 条",
		positive, multiHit, noOlderFallback, scopeFallbackHit)
}
