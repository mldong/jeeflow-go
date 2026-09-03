package facade_test

// stats 三 action 契约对齐测试（issues/103 验收修复 A–E + 基线对齐）
//
// 数据集与 jeeflow-java JeeflowStatsTest 完全同构（Go↔Java 交叉核对同批）：
// 历史数据固定 2026-08-01/02（UTC 构造，与 parseSurrogateTime 的 UTC 语义一致、
// 与真实 today 隔离）；I106 为服务器当日（time.Now()，E 自证：todayNew 计它）。
//
// A：trend/group 的 data 本体是裸数组（非 {series}/{rows} 包装）
// B：stateIn 入参生效（缺省 [10,20,30,40,45,50]）
// C：trend 缺 start/end/granularity → code!=0（对齐内置线 20010012 语义）
// D：define 维度 count 全实例（无 state 过滤）、avg 仅对 state=20 聚合
// E：todayNew 不过滤 state（服务器当日全部实例）

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

func statsFacade() (*facade.Facade, *memory.Repository) {
	f, repo, _ := setupFacade()
	return f, repo
}

func statCall(t *testing.T, f *facade.Facade, action string, kv ...interface{}) map[string]interface{} {
	t.Helper()
	a := map[string]interface{}{}
	for i := 0; i+1 < len(kv); i += 2 {
		a[kv[i].(string)] = kv[i+1]
	}
	return f.Flow(action, a)
}

// statRows 断言 data 本体为裸数组（A 自证）并返回行列表
func statRows(t *testing.T, r map[string]interface{}) []map[string]interface{} {
	t.Helper()
	arr, ok := r["data"].([]interface{})
	if !ok {
		t.Fatalf("data 本体应为裸数组，实际：%T (%v)", r["data"], r["data"])
	}
	rows := make([]map[string]interface{}, 0, len(arr))
	for _, e := range arr {
		m, ok := e.(map[string]interface{})
		if !ok {
			t.Fatalf("裸数组元素应为 map，实际：%T", e)
		}
		rows = append(rows, m)
	}
	return rows
}

func statMap(t *testing.T, r map[string]interface{}) map[string]interface{} {
	t.Helper()
	m, ok := r["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data 本体应为对象，实际：%T", r["data"])
	}
	return m
}

// numOf 兼容 int/int64/float64 取值
func numOf(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return math.NaN()
}

func expectNum(t *testing.T, what string, got interface{}, want float64) {
	t.Helper()
	if math.Abs(numOf(got)-want) > 1e-9 {
		t.Fatalf("%s = %v，期望 %v", what, got, want)
	}
}

// ───────── 测试数据（与 Java JeeflowStatsTest.seed 同构） ─────────

func seedStats(t *testing.T, repo *memory.Repository) {
	t.Helper()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seed 失败：%v", err)
		}
	}
	repo.AddDefine(&model.ProcessDefine{ID: 1, Name: "leave", DisplayName: "请假流程", Type: "approval", State: 1})
	repo.AddDefine(&model.ProcessDefine{ID: 2, Name: "expense", DisplayName: "报销流程", Type: "finance", State: 1})

	d1a := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	d1b := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	d2a := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	d2b := time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC)
	d2c := time.Date(2026, 8, 2, 15, 0, 0, 0, time.UTC)
	d2d := time.Date(2026, 8, 2, 16, 0, 0, 0, time.UTC)
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 100, DefineID: 1, State: model.InstanceStateDoing, Operator: "u1", CreateTime: d1a}))
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 101, DefineID: 1, State: model.InstanceStateDone, Operator: "u1", CreateTime: d1b}))
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 102, DefineID: 2, State: model.InstanceStateDone, Operator: "u2", CreateTime: d2a}))
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 103, DefineID: 2, State: model.InstanceStateWithdraw, Operator: "u2", CreateTime: d2b}))
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 104, DefineID: 1, State: model.InstanceStateReject, Operator: "u3", CreateTime: d2c}))
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 105, DefineID: 1, State: model.InstanceStateAbandon, Operator: "u3", CreateTime: d2d}))  // 废弃（缺省 stateIn 剔除）
	must(repo.SaveInstance(ctx, &model.ProcessInstance{ID: 106, DefineID: 1, State: model.InstanceStateAbandon, Operator: "u1", CreateTime: time.Now()})) // 当日·废弃（E 自证）

	must(repo.SaveTask(ctx, &model.ProcessTask{ID: 200, ProcessInstanceID: 100, TaskState: model.TaskStateDoing, DisplayName: "部门经理审批", ActorID: "u9",
		CreateTime: d1a.Add(5 * time.Minute)})) // 在办
	must(repo.SaveTask(ctx, &model.ProcessTask{ID: 201, ProcessInstanceID: 100, TaskState: model.TaskStateDoing, DisplayName: "人事确认", ActorID: "u9",
		CreateTime: d1a.Add(6 * time.Minute)})) // 在办·会签
	must(repo.AddTaskActor(ctx, 201, []string{"u9", "u10"}))
	must(repo.SaveTask(ctx, &model.ProcessTask{ID: 202, ProcessInstanceID: 101, TaskState: model.TaskStateDone, DisplayName: "部门经理审批", ActorID: "u5",
		CreateTime: d1b.Add(10 * time.Minute), FinishTime: pt(d1b.Add(6 * time.Hour)), ExpireTime: pt(d1b.Add(24 * time.Hour))})) // 办结·及时
	must(repo.SaveTask(ctx, &model.ProcessTask{ID: 203, ProcessInstanceID: 102, TaskState: model.TaskStateDone, DisplayName: "财务审批", ActorID: "u6",
		CreateTime: d2a.Add(10 * time.Minute), FinishTime: pt(d2a.Add(9 * time.Hour)), ExpireTime: pt(d2a.Add(3 * time.Hour))})) // 办结·超时
	must(repo.SaveTask(ctx, &model.ProcessTask{ID: 204, ProcessInstanceID: 105, TaskState: model.TaskStateDone, DisplayName: "部门经理审批", ActorID: "u5",
		CreateTime: d2d.Add(10 * time.Minute), FinishTime: pt(d2d.Add(60 * time.Minute)), ExpireTime: pt(d2d.Add(90 * time.Minute))})) // 办结·及时（废弃实例的任务，时长 50m）
	t205 := &model.ProcessTask{ID: 205, ProcessInstanceID: 106, TaskState: model.TaskStateDone, DisplayName: "财务审批", ActorID: "u6",
		CreateTime: time.Now().Add(-2 * time.Hour), FinishTime: pt(time.Now().Add(-1 * time.Hour)), // 会签·当日
		PerformType: int(model.PerformTypeCountersign)}
	must(repo.SaveTask(ctx, t205))
	must(repo.AddTaskActor(ctx, 205, []string{"u6", "u7"}))
}

func pt(t time.Time) *time.Time { return &t }

// ───────── overview ─────────

func TestStatsOverviewFull(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/overview")
	if numOf(r["code"]) != 0 {
		t.Fatalf("code = %v", r["code"])
	}
	d := statMap(t, r)
	expectNum(t, "total", d["total"], 5)                 // 缺省 stateIn 剔除 99（I105/I106）
	expectNum(t, "inProgress", d["inProgress"], 1)
	expectNum(t, "completed", d["completed"], 2)
	expectNum(t, "rejected", d["rejected"], 1)
	expectNum(t, "withdrawn", d["withdrawn"], 1)
	expectNum(t, "suspended", d["suspended"], 0)
	expectNum(t, "todayNew", d["todayNew"], 1)           // E：只计当日 I106（state=99 也计）
	expectNum(t, "avgDurationSeconds", d["avgDurationSeconds"], 27000) // (21600+32400)/2
	expectNum(t, "rejectRate", d["rejectRate"], 0.3333)
	expectNum(t, "pendingTaskCount", d["pendingTaskCount"], 2)
	expectNum(t, "overdueTaskCount", d["overdueTaskCount"], 0)
	expectNum(t, "countersignRate", d["countersignRate"], 0.25) // 1/4 会签完成
	expectNum(t, "onTimeRate", d["onTimeRate"], 0.6667)         // 2/3（T205 无 expire 不入分母）
}

func TestStatsOverviewStateInRespected(t *testing.T) {
	// B 自证：非缺省 stateIn 六个计数随动，todayNew 不受影响
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/overview", "stateIn", []interface{}{10})
	d := statMap(t, r)
	expectNum(t, "total", d["total"], 1)
	expectNum(t, "inProgress", d["inProgress"], 1)
	expectNum(t, "completed", d["completed"], 0)
	expectNum(t, "todayNew", d["todayNew"], 1) // 仍计当日 I106
}

func TestStatsOverviewEmptyRepo(t *testing.T) {
	f, _ := statsFacade()
	r := statCall(t, f, "processInstance/stats/overview")
	if numOf(r["code"]) != 0 {
		t.Fatalf("code = %v", r["code"])
	}
	d := statMap(t, r)
	for _, k := range []string{"total", "inProgress", "completed", "rejected",
		"withdrawn", "suspended", "todayNew", "avgDurationSeconds",
		"pendingTaskCount", "overdueTaskCount"} {
		expectNum(t, k, d[k], 0)
	}
	expectNum(t, "rejectRate", d["rejectRate"], 0)
	expectNum(t, "onTimeRate", d["onTimeRate"], 0)
}

// ───────── trend ─────────

func TestStatsTrendDayBareArray(t *testing.T) {
	// A 自证：data 本体是裸数组（非 {granularity, series}）
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/trend",
		"start", "2026-08-01 00:00:00", "end", "2026-08-02 23:59:59", "granularity", "day")
	if numOf(r["code"]) != 0 {
		t.Fatalf("code = %v", r["code"])
	}
	arr := statRows(t, r)
	if len(arr) != 2 {
		t.Fatalf("day 桶数 = %d，期望 2", len(arr))
	}
	if arr[0]["bucket"] != "2026-08-01" || arr[1]["bucket"] != "2026-08-02" {
		t.Fatalf("桶标签 = %v / %v", arr[0]["bucket"], arr[1]["bucket"])
	}
	expectNum(t, "08-01 started", arr[0]["started"], 2)  // I100,I101（实例侧无 state 过滤）
	expectNum(t, "08-01 finished", arr[0]["finished"], 1) // T202
	expectNum(t, "08-02 started", arr[1]["started"], 4)  // I102,I103,I104,I105（无 state 过滤，含 99 的 I105）
	expectNum(t, "08-02 finished", arr[1]["finished"], 2) // T203,T204
}

func TestStatsTrendHourWeekMonth(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	h := statCall(t, f, "processInstance/stats/trend",
		"start", "2026-08-01 10:00:00", "end", "2026-08-01 12:00:00", "granularity", "hour")
	hb := statRows(t, h)
	if len(hb) != 3 {
		t.Fatalf("hour 桶数 = %d，期望 3", len(hb))
	}
	if hb[0]["bucket"] != "2026-08-01 10:00" || hb[1]["bucket"] != "2026-08-01 11:00" || hb[2]["bucket"] != "2026-08-01 12:00" {
		t.Fatalf("hour 桶标签 = %v/%v/%v", hb[0]["bucket"], hb[1]["bucket"], hb[2]["bucket"])
	}
	expectNum(t, "10:00 started", hb[0]["started"], 1) // I100
	expectNum(t, "12:00 started", hb[2]["started"], 1) // I101

	w := statCall(t, f, "processInstance/stats/trend",
		"start", "2026-08-01 00:00:00", "end", "2026-08-02 23:59:59", "granularity", "week")
	wb := statRows(t, w)
	if len(wb) != 1 {
		t.Fatalf("week 桶数 = %d，期望 1", len(wb))
	}
	if wb[0]["bucket"] != "2026-W31" {
		t.Fatalf("week 桶标签 = %v，期望 2026-W31", wb[0]["bucket"])
	}
	expectNum(t, "W31 started", wb[0]["started"], 6) // I100–I105 全部落在 W31（含 99 的 I105）

	m := statCall(t, f, "processInstance/stats/trend",
		"start", "2026-07-31 00:00:00", "end", "2026-08-31 23:59:59", "granularity", "month")
	mb := statRows(t, m)
	if len(mb) != 2 {
		t.Fatalf("month 桶数 = %d，期望 2", len(mb))
	}
	if mb[0]["bucket"] != "2026-07" || mb[1]["bucket"] != "2026-08" {
		t.Fatalf("month 桶标签 = %v/%v", mb[0]["bucket"], mb[1]["bucket"])
	}
	expectNum(t, "2026-07 started", mb[0]["started"], 0) // 补 0 桶
	expectNum(t, "2026-08 started", mb[1]["started"], 6) // I100–I105（含 99 的 I105）
}

func TestStatsTrendMissingParams(t *testing.T) {
	// C 自证：缺 start / 缺 end / 缺 granularity / granularity 非法 → 均 code!=0
	f, repo := statsFacade()
	seedStats(t, repo)
	cases := [][]interface{}{
		{"end", "2026-08-02 00:00:00", "granularity", "day"},
		{"start", "2026-08-01 00:00:00", "granularity", "day"},
		{"start", "2026-08-01 00:00:00", "end", "2026-08-02 00:00:00"},
		{"start", "2026-08-01 00:00:00", "end", "2026-08-02 00:00:00", "granularity", "abc"},
	}
	for _, kv := range cases {
		r := statCall(t, f, "processInstance/stats/trend", kv...)
		if numOf(r["code"]) == 0 {
			t.Fatalf("缺参应报错：%v", kv)
		}
	}
}

func TestStatsTrendEmptyRepoAllZero(t *testing.T) {
	f, _ := statsFacade()
	r := statCall(t, f, "processInstance/stats/trend",
		"start", "2026-08-01 00:00:00", "end", "2026-08-01 23:59:59", "granularity", "day")
	if numOf(r["code"]) != 0 {
		t.Fatalf("code = %v", r["code"])
	}
	arr := statRows(t, r)
	if len(arr) != 1 {
		t.Fatalf("桶数 = %d，期望 1", len(arr))
	}
	expectNum(t, "started", arr[0]["started"], 0)
	expectNum(t, "finished", arr[0]["finished"], 0)
}

// ───────── group ─────────

func TestStatsGroupAllDimensionsBareArray(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	// A 自证 + 正向：9 维度 data 本体均为裸数组
	for _, dim := range []string{"state", "define", "category", "approver",
		"applicant", "node", "stuckNode", "stuckApprover", "durationBucket"} {
		r := statCall(t, f, "processInstance/stats/group",
			"dimension", dim, "start", "2026-08-01 00:00:00", "end", "2026-08-02 23:59:59")
		if numOf(r["code"]) != 0 {
			t.Fatalf("dimension=%s code = %v", dim, r["code"])
		}
		statRows(t, r)
	}
}

func TestStatsGroupDefineNoStateFilter(t *testing.T) {
	// D 自证：count 全实例（含 45/99），avg 仅 state=20
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/group", "dimension", "define")
	rows := statRows(t, r)
	if len(rows) != 2 {
		t.Fatalf("define 行数 = %d，期望 2", len(rows))
	}
	leave, expense := rows[0], rows[1]
	if leave["key"] != "leave" || leave["label"] != "请假流程" {
		t.Fatalf("rows[0] = %v/%v，期望 leave/请假流程", leave["key"], leave["label"])
	}
	expectNum(t, "leave count", leave["count"], 5)        // I100,I101,I104,I105,I106（无 state 过滤，含 45/99）
	expectNum(t, "leave avg", leave["avgDurationSeconds"], 21600) // avg 仅对 state=20（I101=6h）
	expectNum(t, "expense count", expense["count"], 2)    // I102,I103
	expectNum(t, "expense avg", expense["avgDurationSeconds"], 32400) // I102=9h
}

func TestStatsGroupStateCategoryApplicant(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	st := statRows(t, statCall(t, f, "processInstance/stats/group", "dimension", "state"))
	// 无 state 过滤：99 也出现（I105,I106 两条 99）；5 个不同 state
	byKey := map[string]float64{}
	for _, m := range st {
		byKey[m["key"].(string)] = numOf(m["count"])
	}
	expectNum(t, "state 20", byKey["20"], 2) // I101,I102
	expectNum(t, "state 99", byKey["99"], 2) // I105,I106
	expectNum(t, "state 10", byKey["10"], 1) // I100
	expectNum(t, "state 30", byKey["30"], 1) // I103
	expectNum(t, "state 45", byKey["45"], 1) // I104
	if len(st) != 5 {
		t.Fatalf("state 行数 = %d，期望 5", len(st))
	}

	cat := statRows(t, statCall(t, f, "processInstance/stats/group", "dimension", "category"))
	expectNum(t, "approval", cat[0]["count"], 5) // 无 state 过滤，含 99
	expectNum(t, "finance", cat[1]["count"], 2)

	ap := statRows(t, statCall(t, f, "processInstance/stats/group", "dimension", "applicant"))
	if ap[0]["key"] != "u1" {
		t.Fatalf("applicant[0] = %v，期望 u1", ap[0]["key"])
	}
	expectNum(t, "u1", ap[0]["count"], 3) // I100,I101,I106（无 state 过滤）
}

func TestStatsGroupNodeApproverAvg(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/group", "dimension", "node",
		"start", "2026-08-01 00:00:00", "end", "2026-08-02 23:59:59")
	rows := statRows(t, r)
	// 8 月范围只含 T202/T203/T204（T205 的 finish 是真实当日，不入范围）
	// 部门经理审批：T202=21000s、T204=3000s → avg=(21000+3000)/2=12000
	if rows[0]["key"] != "部门经理审批" {
		t.Fatalf("node[0] = %v，期望 部门经理审批", rows[0]["key"])
	}
	expectNum(t, "部门经理审批 count", rows[0]["count"], 2)
	expectNum(t, "部门经理审批 avg", rows[0]["avgDurationSeconds"], 12000)
	if rows[1]["key"] != "财务审批" {
		t.Fatalf("node[1] = %v，期望 财务审批", rows[1]["key"])
	}
	expectNum(t, "财务审批 avg", rows[1]["avgDurationSeconds"], 31800) // T203=8h50m

	ap := statRows(t, statCall(t, f, "processInstance/stats/group", "dimension", "approver",
		"start", "2026-08-01 00:00:00", "end", "2026-08-02 23:59:59"))
	if ap[0]["key"] != "u5" {
		t.Fatalf("approver[0] = %v，期望 u5", ap[0]["key"])
	}
	expectNum(t, "u5", ap[0]["count"], 2) // T202+T204 历史办结
}

func TestStatsGroupStuckRealtimeIgnoresRange(t *testing.T) {
	// stuckNode/stuckApprover 实时快照，忽略 start/end
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/group", "dimension", "stuckNode",
		"start", "2020-01-01 00:00:00", "end", "2020-01-02 00:00:00")
	rows := statRows(t, r)
	// T200（部门经理审批）/T201（人事确认）两条在办任务分属不同节点 → 2 行，各 count=1
	if len(rows) != 2 {
		t.Fatalf("stuckNode 行数 = %d，期望 2", len(rows))
	}
	nodeKeys := map[string]bool{}
	for _, m := range rows {
		nodeKeys[m["key"].(string)] = true
		expectNum(t, "stuckNode count "+m["key"].(string), m["count"], 1)
		if m["avgDurationSeconds"] != nil {
			t.Fatalf("stuckNode avgDurationSeconds 应为 nil")
		}
	}
	if !nodeKeys["部门经理审批"] || !nodeKeys["人事确认"] {
		t.Fatalf("stuckNode 节点缺失：%v", nodeKeys)
	}

	// 会签自证：stuckApprover 每 actor 一行不重复计（仅 T201 注册了 actors u9/u10）
	r2 := statCall(t, f, "processInstance/stats/group", "dimension", "stuckApprover",
		"start", "2020-01-01 00:00:00", "end", "2020-01-02 00:00:00")
	actors := statRows(t, r2)
	if len(actors) != 2 {
		t.Fatalf("stuckApprover 行数 = %d，期望 2", len(actors))
	}
	for _, m := range actors {
		expectNum(t, "stuckApprover count "+m["key"].(string), m["count"], 1)
		if m["avgDurationSeconds"] != nil {
			t.Fatalf("stuckApprover avgDurationSeconds 应为 nil")
		}
	}
}

func TestStatsGroupDurationBucketFixedOrder(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/group", "dimension", "durationBucket")
	rows := statRows(t, r)
	if len(rows) != 4 {
		t.Fatalf("durationBucket 行数 = %d，期望 4", len(rows))
	}
	wantKeys := []string{"sameDay", "1to3d", "3to7d", "over7d"}
	wantCounts := []float64{2, 0, 0, 0} // durationBucket 仅对 state=20 实例：I101=6h、I102=9h → sameDay=2
	for i, m := range rows {
		if m["key"] != wantKeys[i] {
			t.Fatalf("bucket[%d] key = %v，期望 %v", i, m["key"], wantKeys[i])
		}
		expectNum(t, wantKeys[i], m["count"], wantCounts[i])
	}
}

func TestStatsGroupInvalidDimension(t *testing.T) {
	f, _ := statsFacade()
	r := statCall(t, f, "processInstance/stats/group", "dimension", "bogus")
	if numOf(r["code"]) == 0 {
		t.Fatal("非法 dimension 应报错")
	}
}

func TestStatsGroupLimitTopN(t *testing.T) {
	f, repo := statsFacade()
	seedStats(t, repo)
	r := statCall(t, f, "processInstance/stats/group", "dimension", "state", "limit", 2)
	rows := statRows(t, r)
	if len(rows) != 2 {
		t.Fatalf("limit 行数 = %d，期望 2", len(rows))
	}
	if numOf(rows[0]["count"]) < numOf(rows[1]["count"]) {
		t.Fatalf("未按 count 降序：%v < %v", rows[0]["count"], rows[1]["count"])
	}
}

func TestStatsOverviewExpireAllNull(t *testing.T) {
	// 边界：expire 全 NULL → overdueTaskCount=0、onTimeRate=0（非错误）
	f, repo := statsFacade()
	ctx := context.Background()
	repo.AddDefine(&model.ProcessDefine{ID: 1, Name: "leave", DisplayName: "请假流程", Type: "approval", State: 1})
	if err := repo.SaveInstance(ctx, &model.ProcessInstance{ID: 100, DefineID: 1, State: model.InstanceStateDoing, Operator: "u1", CreateTime: time.Now().Add(-2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveTask(ctx, &model.ProcessTask{ID: 200, ProcessInstanceID: 100, TaskState: model.TaskStateDoing, DisplayName: "审批", ActorID: "u9",
		CreateTime: time.Now().Add(-1 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	d := statMap(t, statCall(t, f, "processInstance/stats/overview"))
	expectNum(t, "overdueTaskCount", d["overdueTaskCount"], 0)
	expectNum(t, "onTimeRate", d["onTimeRate"], 0)
}
