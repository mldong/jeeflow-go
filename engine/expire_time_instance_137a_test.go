// issues/137 A · 批二 §3-4（A 案）· go 腿：**实例级** `expire_time` 由**发起腿**按流程定义
// **顶层** `expireTime` 表达式真算。基准＝java `JeeflowEngineImpl.java:93-96`
// ＝ boot2 内置版 `ProcessInstanceServiceImpl.java:157-160`（先判非空再写）；
// 裁定原文＝issues/137 状态行 A（owner 2026-09-28「实例的处理方式和任务的差不多吧，算法一致」）。
//
// 修前形状：本栈**根本没有这一处写点**——`wf_process_instance.expire_time` 从建实例起就没被赋过值
// （issues/137 §A 记的「go/python/node 实例级零写点（配了也恒 NULL）」），
// 所以本文件全部格子是"补写点"的判据，不是"改搬运"的判据。
//
// 五条判据（批二工单 §3-4 go 腿）各自落格，函数名对号：
// 判据 1 列里是**求值结果**（一个时刻），不是表达式原串 → TestInstanceExpireTimeWritesEvaluatedInstantNotRawExpression
// 判据 2 求值的 args＝**发起参数**那一份（vars，已被 addUserInfo/addAutoGenTitle 注入 u_* 与 autoGenTitle）→ TestInstanceExpireTimeEvaluatesAgainstStartArgs（u_userId 那一格是正面判点）
// 判据 3 定义**没配**顶层 expireTime（缺键 / 空串 / null / 纯空白）⇒ 该列保持 NULL，不赋 now()、不赋空串 → TestInstanceExpireTimeUnconfiguredStaysNull
// 判据 4 配了但**算不出**（误配串 / 小数前缀 / 负数档）⇒ NULL，沿用既有落穿语义，不许兜底 now → TestInstanceExpireTimeUnparsableStaysNullNotNow
// 判据 5 求值器**复用 ProcessTime**、档位顺序与语义一字不改（变量档优先于相对档、类型不认识落穿）→ TestInstanceExpireTimeSharesTaskTierEvaluatorOrder
// 另加：两级互不干扰 TestInstanceExpireTimeTwoLevelsDoNotCrossFeed；终态实例与共享夹具两族回归。
//
// 判据一律打在**内存仓读回的持久行**上，不是引擎返回的聚合对象——issues/113 教训：
// `SaveInstance` 存的是那一刻的结构体拷贝，只有读回值能证明"这一列真进了库"。
// 真库（MySQL DATETIME(3)）那一路的读数见 repository/jdbc/instance_expire_137a_test.go。
//
// ⚠️ 本文件只新增用例，没动任何既有测试的期望值；夹具复用 expire_time_test.go 同包（engine_test）
// 的 expHarness/expFlowWith/expSpec/expStart/expMustDoing/expAt/expUserProv。
package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/internal/flowsutil"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// expInstFlow 在 expFlowWith 的根对象里插一段**顶层**键片段。
// rootSeg 收片段而不是收值，是因为判据 3 的四档（缺键 / 空串 / JSON null / 纯空白）
// 必须在 JSON 文本上彼此分得开——`"expireTime":""` 与"根本没有这个键"是两种定义形状，
// 用值表达就退化成一档。
func expInstFlow(name, rootSeg string, specs ...expSpec) string {
	flow := expFlowWith(name, specs...)
	if rootSeg == "" { // 缺键档
		return flow
	}
	i := strings.Index(flow, `"nodes":[`)
	if i < 0 {
		panic("expInstFlow: 夹具根对象里没有 nodes 键，插入点找不到")
	}
	return flow[:i] + rootSeg + flow[i:]
}

// expInstApprove 一个不带到期表达式的任务节点（实例级判据要排除任务级写点的干扰）
var expInstApprove = []expSpec{{"approve", `"assignee":"zhangsan"`}}

// expInstRow 读回内存仓里的**持久实例行**（FindInstanceByID 返回的是存储拷贝，
// 与引擎返回的聚合对象不是同一个对象——正是判据要的"进库了才算"）。
func expInstRow(t *testing.T, repo *memory.Repository, instID int64) *model.ProcessInstance {
	t.Helper()
	row, err := repo.FindInstanceByID(context.Background(), instID)
	if err != nil || row == nil {
		t.Fatalf("读回持久实例行 %d: row=%v err=%v", instID, row, err)
	}
	return row
}

// expInstMustDelta 同一行 expire − create 落进表达式偏移带宽（**不许只判非空**：
// 只判非空就会被占位 now()（差值≈0）与"根本没算"混过去，那正是本病灶的形状）
func expInstMustDelta(t *testing.T, row *model.ProcessInstance, want time.Duration) {
	t.Helper()
	if row.ExpireTime == nil {
		t.Fatalf("实例 %d 配了顶层到期表达式（期望偏移 %v），持久行 expire_time 却是空", row.ID, want)
	}
	if row.ExpireTime.IsZero() {
		t.Fatalf("实例 %d 的 expire_time 是零值时刻 %v（原串没被求值的典型形状）", row.ID, *row.ExpireTime)
	}
	delta := row.ExpireTime.Sub(row.CreateTime)
	// 带宽 [-5s, +60s]：容时钟/落库量纲，不容"根本没算"（与任务级 expMustDelta 同尺）
	if delta < want-5*time.Second || delta > want+60*time.Second {
		t.Errorf("实例 expire − create = %v，期望 %v±(5s,60s)；占位 now() 会算出≈0，搬原串/不求值会算出零值或空",
			delta, want)
	}
}

// expInstMustAt 判**具体时刻**（变量档/绝对档共用）
func expInstMustAt(t *testing.T, row *model.ProcessInstance, s string) {
	t.Helper()
	want := expAt(t, s)
	if row.ExpireTime == nil {
		t.Fatalf("实例 %d 的 expire_time 为空，期望 %v", row.ID, want)
	}
	if !row.ExpireTime.Equal(want) {
		t.Errorf("实例 expire_time = %v，期望 %v", *row.ExpireTime, want)
	}
}

// expInstMustNull 该列保持 NULL（未配 / 算不出两档共用）；同时钉"实例行确实在库里"，
// 否则"空"可能是"根本没建行"混过去的。
func expInstMustNull(t *testing.T, repo *memory.Repository, instID int64, why string) {
	t.Helper()
	row := expInstRow(t, repo, instID)
	if row.ExpireTime != nil {
		t.Errorf("%s：实例 expire_time 被赋成 %v，期望保持空（不造默认值、不写 now()、不写空串）",
			why, *row.ExpireTime)
	}
}

// ─── 判据 1：写进去的是**求值结果**（时刻），不是表达式原串 ───────────────────────

// 相对档三形（s/h/d 各一形，d 走 AddDate 的历日加法、与 s/h 的 Duration 通路不同形）：
// 持久行必须落进 expire − create 的偏移带宽。
//
// 为什么这组格子能判出"搬原串"：本栈这一列是 `*time.Time`（落库经 repository/jdbc/jdbc.go:405
// 的 INSERT 直接绑进 DATETIME(3)），把 `"2h"` 这类原串搬过去**在真库上是服务端硬错**
// （160 那台 MySQL `@@sql_mode` 含 STRICT_TRANS_TABLES；兄弟栈 rust/php 本轮实测服务端给的是
// 1292/22007 `Incorrect datetime value: '2h' for column 'expire_time'`），
// 连库都进不去。所以本栈"搬运"这一病灶在 Go 里唯一的可成形方式是**不经求值器、把原串按
// 绝对时刻档硬解**（解析失败⇒写零值/写空），两种结局都被下面这两道判点拦下：
// 带宽判点（零值时刻的差值是 -2026 年，绝不可能落进 +2h 带宽）＋ expInstMustDelta 里的 IsZero 判点。
func TestInstanceExpireTimeWritesEvaluatedInstantNotRawExpression(t *testing.T) {
	cases := []struct {
		expr string
		want time.Duration
	}{
		{"30m", 30 * time.Minute},
		{"2h", 2 * time.Hour},
		{"3d", 72 * time.Hour}, // 天档＝日历加 3 天（无 DST 环境里与 72h 同值，量纲判点在 expire_time_test.go 那族）
	}
	for _, c := range cases {
		name := "exp137a_rel_" + strings.ReplaceAll(c.expr, " ", "_")
		eng, repo, defID := expHarness(t, name, expInstFlow(name, `"expireTime":"`+c.expr+`",`, expInstApprove...))
		inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A1"})
		expInstMustDelta(t, expInstRow(t, repo, inst.ID), c.want)
	}
}

// 绝对时刻档：顶层表达式本身就是 "yyyy-MM-dd HH:mm:ss" ⇒ 该时刻逐字落进列。
// （这一形单独钉 ProcessTime 的 ③ 档在实例级同样生效；它与上一组互补——
// 原串搬运在绝对档上"看着对"，只有相对档能把它照出来。）
func TestInstanceExpireTimeAbsoluteExpressionIsThatInstant(t *testing.T) {
	name := "exp137a_abs"
	eng, repo, defID := expHarness(t, name,
		expInstFlow(name, `"expireTime":"2027-03-04 05:06:07",`, expInstApprove...))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A2"})
	expInstMustAt(t, expInstRow(t, repo, inst.ID), "2027-03-04 05:06:07")
}

// ─── 判据 2：求值的 args＝**发起参数**那份 vars（含引擎注入的 u_* / autoGenTitle）─────

// ① 表达式是调用方传入的变量名 ⇒ 取该变量的值（ProcessTime ① 档，实例级同样生效）。
// ② 表达式是 **u_userId** ⇒ 取的是 `addUserInfo(operator, vars)` 注入的那一份。
// 这一形是判据 2 的正面判点：写点若错接**原始 args**（engine_impl.go:64 之前那份、没有 u_*），
// 变量档命不中 ⇒ 落穿相对档 ⇒ 绝对档也解析不出 ⇒ 该列 NULL，这格当场红。
// 用户提供桩见 expire_time_test.go 的 expUserDates（boss1 的 UserID 直接做成合法日期串）。
func TestInstanceExpireTimeEvaluatesAgainstStartArgs(t *testing.T) {
	varName := "exp137a_var"
	eng, repo, defID := expHarness(t, varName,
		expInstFlow(varName, `"expireTime":"dueAt",`, expInstApprove...))
	inst := expStart(t, eng, defID, map[string]interface{}{"dueAt": "2026-12-31 10:00:00"})
	expInstMustAt(t, expInstRow(t, repo, inst.ID), "2026-12-31 10:00:00")

	uName := "exp137a_uvar"
	eng2, repo2, defID2 := expHarness(t, uName,
		expInstFlow(uName, `"expireTime":"u_userId",`, expInstApprove...))
	inst2 := expStart(t, eng2, defID2, map[string]interface{}{"BUSINESS_NO": "B137A3"})
	expInstMustAt(t, expInstRow(t, repo2, inst2.ID), "2028-01-01 01:01:01") // 发起人 boss1 在桩里的日期串
}

// ─── 判据 3：定义没配顶层 expireTime ⇒ 该列保持 NULL（四档并排）─────────────────────

// 缺键 / 空串 / JSON null / 纯空白四档共用"没有到期时间"这一结局：
// 不赋 now()、不赋空串、不赋 0。四档并排跑，是因为"投成空串还是缺键"这种形状差异
// 本身就藏过病灶（见 expireExprOf 与 ProcessTime ⓪ 档的注释）。
// 前三档在 Go 的 json.Unmarshal 里都落进零值 ""（null 进 string 是 no-op），
// 第四档（纯空白）过不了 ProcessTime 的 ⓪ 档 ⇒ 同为 NULL，但走的是**不同**那条门，
// 所以判非空那道门（java isNotEmpty）与求值器 ⓪ 档各被钉一次。
func TestInstanceExpireTimeUnconfiguredStaysNull(t *testing.T) {
	cases := []struct {
		rootSeg, label string
	}{
		{"", "顶层缺键（没配）"},
		{`"expireTime":"",`, "顶层配成空串"},
		{`"expireTime":null,`, "顶层配成 null"},
		{`"expireTime":"   ",`, "顶层配成纯空白"},
	}
	for i, c := range cases {
		name := "exp137a_none" + string(rune('0'+i))
		eng, repo, defID := expHarness(t, name, expInstFlow(name, c.rootSeg, expInstApprove...))
		inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A4"})
		expInstMustNull(t, repo, inst.ID, c.label)
		// 实例行必须在（否则"空"是"没建实例"混过去的），且首任务行照常被建出来
		row := expInstRow(t, repo, inst.ID)
		if row.State != model.InstanceStateDoing {
			t.Errorf("%s：实例 state = %v，期望 DOING(10)", c.label, row.State)
		}
		if len(expMustDoing(t, repo, inst.ID, "approve", 1)) != 1 {
			t.Errorf("%s：首任务行没建出来", c.label)
		}
	}
}

// ─── 判据 4：配了但算不出 ⇒ NULL，沿用既有落穿语义，不许兜底 now ────────────────────

// 三形：垃圾串（③ 档解析不出）/ 小数前缀（② 档认不出、③ 档也解析不出）/
// 负数相对档（issues/137 D 判非负 ⇒ 落穿 ⇒ NULL）。
// 另钉一格 `autoGenTitle`：这个键**确实在**发起参数里（addAutoGenTitle 注入），但它的值是
// "张三的流程-2026-10-01 21:30" 这种标题串 ⇒ 任何一档都算不出 ⇒ 仍 NULL。
// 它同时是判据 2 的反面证据（args 里有的键不等于能算出时刻，别把标题串当时刻写进去）。
func TestInstanceExpireTimeUnparsableStaysNullNotNow(t *testing.T) {
	exprs := []string{"not-a-time", "2.5h", "-5h", "-3d", "autoGenTitle"}
	for i, expr := range exprs {
		name := "exp137a_bad" + string(rune('0'+i))
		eng, repo, defID := expHarness(t, name,
			expInstFlow(name, `"expireTime":"`+expr+`",`, expInstApprove...))
		inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A5"})
		expInstMustNull(t, repo, inst.ID, "顶层表达式 "+expr+" 算不出")
	}

	// 正向对照（保证上面五格不是"实例级写点整条恒空"造成的假绿）：同一夹具形状换成合法 2h ⇒ 有值
	okName := "exp137a_bad_ok"
	eng, repo, defID := expHarness(t, okName,
		expInstFlow(okName, `"expireTime":"2h",`, expInstApprove...))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A5P"})
	expInstMustDelta(t, expInstRow(t, repo, inst.ID), 2*time.Hour)
}

// ─── 判据 5：复用 ProcessTime，档位顺序与语义一字不改 ────────────────────────────

// 实例级与任务级共用**同一把尺子**（不许新造第二把）的两条可观察形状：
//   - **变量档优先于相对档**：args 里真有个键名等于表达式（"2h"）⇒ 取变量值那个时刻，
//     不是 now+2h。第二把尺子（比如"实例级只认相对档/只认绝对档"）在这里必然给错。
//   - **变量存在但类型不认识 ⇒ 落穿**（不是提前 return nil）：{"2h": true} 落穿到相对档 ⇒ ≈now+2h。
//     这一形专防"实例级另写一份只处理 string 的解析"——那种实现这里会返回 NULL。
func TestInstanceExpireTimeSharesTaskTierEvaluatorOrder(t *testing.T) {
	prio := "exp137a_order"
	eng, repo, defID := expHarness(t, prio,
		expInstFlow(prio, `"expireTime":"2h",`, expInstApprove...))
	inst := expStart(t, eng, defID, map[string]interface{}{"2h": "2026-12-31 10:00:00"})
	expInstMustAt(t, expInstRow(t, repo, inst.ID), "2026-12-31 10:00:00")

	fall := "exp137a_fallthrough"
	eng2, repo2, defID2 := expHarness(t, fall,
		expInstFlow(fall, `"expireTime":"2h",`, expInstApprove...))
	inst2 := expStart(t, eng2, defID2, map[string]interface{}{"2h": true}) // bool ⇒ 类型不认识 ⇒ 落穿相对档
	expInstMustDelta(t, expInstRow(t, repo2, inst2.ID), 2*time.Hour)

	// 变量档字符串解析失败＝终局（对齐 java catch→return null，**不落穿**相对档）：
	// 表达式 "2h"、变量值是坏串 ⇒ NULL（若实现改成"落穿再走相对档"，这里会算出 now+2h 而红）
	term := "exp137a_strterm"
	eng3, repo3, defID3 := expHarness(t, term,
		expInstFlow(term, `"expireTime":"2h",`, expInstApprove...))
	inst3 := expStart(t, eng3, defID3, map[string]interface{}{"2h": "tomorrow"})
	expInstMustNull(t, repo3, inst3.ID, `变量档字符串解析失败即终局（不许落穿成相对档 2h）`)
}

// ─── 两级互不干扰（判据 1/3 的配对格）────────────────────────────────────────────

// 顶层配 "2h"、任务节点配 "3d" ⇒ 实例列 ≈now+2h、任务行 ≈now+3d：
// 两处写点各取各的表达式，共用同一个求值器但**不许互相串值**。
// 反向配对（顶层没配、节点配 "2h"）钉"实例列不会因为节点配了就跟着有值"——
// 只有正向档时，"实例级读错了源"这种实现也照样绿。
func TestInstanceExpireTimeTwoLevelsDoNotCrossFeed(t *testing.T) {
	both := "exp137a_two"
	eng, repo, defID := expHarness(t, both, expInstFlow(both, `"expireTime":"2h",`,
		expSpec{"approve", `"assignee":"zhangsan","expireTime":"3d"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A6"})
	expInstMustDelta(t, expInstRow(t, repo, inst.ID), 2*time.Hour)
	expMustDelta(t, expMustDoing(t, repo, inst.ID, "approve", 1)[0], 72*time.Hour)

	only := "exp137a_taskonly"
	eng2, repo2, defID2 := expHarness(t, only, expInstFlow(only, "",
		expSpec{"approve", `"assignee":"zhangsan","expireTime":"2h"`}))
	inst2 := expStart(t, eng2, defID2, map[string]interface{}{"BUSINESS_NO": "B137A7"})
	expInstMustNull(t, repo2, inst2.ID, "只有任务节点配了到期表达式（顶层没配）")
	expMustDelta(t, expMustDoing(t, repo2, inst2.ID, "approve", 1)[0], 2*time.Hour)
}

// ─── 回归：写点位置与既有夹具 ───────────────────────────────────────────────────

// 发起即办结的短流（start → end，没有任务节点）：值要活到**实例终态那一次 UpdateInstance** 之后，
// 且终态那一次覆写不许把这一列抹掉、更不许凭空写出 now()（未配档那一格）。
//
// ⚠️ 责任边界（变异对照实测划出来的）：把写点挪到 SaveInstance **之后**时，这一格照样绿——
// end 分支的 UpdateInstance（engine_impl.go:713）重抄的是聚合对象，值会跟着进库。
// 所以拦"写点位置挂错"的是上面那些 **DOING** 实例的格子（发起腿此后没有第二次 update 实例，
// 持久行那一列永远空），本格的责任是反方向的另一半：**终态覆写不许丢列值**。两族各自有牙，别混成一句。
func TestInstanceExpireTimeSurvivesShortFlowFinalUpdate(t *testing.T) {
	withExpr := "exp137a_short"
	eng, repo, defID := expHarness(t, withExpr,
		expInstFlow(withExpr, `"expireTime":"2h",`)) // specs 为空 ⇒ start 直接后继就是 end
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A9"})
	row := expInstRow(t, repo, inst.ID)
	if row.State != model.InstanceStateDone {
		t.Fatalf("短流实例 state = %v，期望 DONE(20)（本格的前置：终态那次 UpdateInstance 真发生了）", row.State)
	}
	expInstMustDelta(t, row, 2*time.Hour)

	noneExpr := "exp137a_shortnone"
	eng2, repo2, defID2 := expHarness(t, noneExpr, expInstFlow(noneExpr, ""))
	inst2 := expStart(t, eng2, defID2, map[string]interface{}{"BUSINESS_NO": "B137A10"})
	row2 := expInstRow(t, repo2, inst2.ID)
	if row2.State != model.InstanceStateDone {
		t.Fatalf("短流（未配）实例 state = %v，期望 DONE(20)", row2.State)
	}
	expInstMustNull(t, repo2, inst2.ID, "短流走到 end 的那次 UpdateInstance（顶层没配表达式）")
}

// 真实共享夹具回归：`flows/06-countersign-sequential-expire.json` 的 `expireTime` 配在**任务节点**
// properties 上（:52），根上**没有** expireTime 键。给 FlowModel 加顶层字段之后：
//   - 该定义照常解析（新字段不误伤既有 JSON）；
//   - 实例那一列仍是 NULL（判据 3 在**真实夹具**上的读数，不是内联串上的读数）；
//   - 任务级那一列不受影响（本批对任务级零改动，apply 行照旧没到期时间）。
func TestInstanceExpireTimeSharedFlowFixtureRegression(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(flowsutil.Dir(), "06-countersign-sequential-expire.json"))
	if err != nil {
		t.Fatalf("读共享夹具: %v", err)
	}
	text := string(content)
	// 夹具自证：表达式只出现在 nodes 之后（＝节点 properties 里），根对象上没有这个键——
	// 判点前提不成立的话本格就是空转，宁可直接红。
	head := text[:strings.Index(text, `"nodes"`)]
	if strings.Contains(head, "expireTime") {
		t.Fatalf("夹具自证失败：根对象上出现了 expireTime，本格的判点前提不成立")
	}
	if !strings.Contains(text, "expireTime") {
		t.Fatalf("夹具自证失败：这份共享夹具里根本没有 expireTime，换夹具或删本格")
	}
	eng, repo, defID := expHarness(t, "countersign-sequential", text)
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B137A8"})
	expInstMustNull(t, repo, inst.ID, "共享夹具 06（顶层没配、task1 节点配了 2h）")
	// 首任务行（apply）也没到期时间：它自己没配表达式
	expMustNull(t, expMustDoing(t, repo, inst.ID, "apply", 1)[0], "共享夹具 06 的 apply 行（节点未配）")
}
