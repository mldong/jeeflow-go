// 任务时限动作 T0 用例（spec 04 · 任务时限动作；工作流时限方案 §8 的引擎侧判据）：
// 建行（autoExecute/reminderTime）、旁路覆盖（saveNewTask 主路/串行会签补建/回退）、
// 不继承档（协办/征询成员）、失效联动（办结/废弃/实例终态取消）、完成回调事实范围、
// 可注入时钟、实例级排他锁调用面。
//
// 夹具全部内联 JSON（不碰共享 flows 目录）；断言落在 fake sink 收到的动作与仓储读回值上。
package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── fake sink ───────────────────────────────────────────────────────────────

type tlFakeSink struct {
	mu                 sync.Mutex
	actions            []spi.TimeAction
	cancelledTasks     []int64
	cancelledInstances []int64
}

func (f *tlFakeSink) InsertTimeAction(_ context.Context, a *spi.TimeAction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions = append(f.actions, *a)
	return nil
}

func (f *tlFakeSink) CancelTaskTimeActions(_ context.Context, taskID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelledTasks = append(f.cancelledTasks, taskID)
	return nil
}

func (f *tlFakeSink) CancelInstanceTimeActions(_ context.Context, instanceID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelledInstances = append(f.cancelledInstances, instanceID)
	return nil
}

func (f *tlFakeSink) snapshot() []spi.TimeAction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]spi.TimeAction(nil), f.actions...)
}

func (f *tlFakeSink) hasTaskCancel(taskID int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.cancelledTasks {
		if id == taskID {
			return true
		}
	}
	return false
}

func (f *tlFakeSink) hasInstanceCancel(instanceID int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.cancelledInstances {
		if id == instanceID {
			return true
		}
	}
	return false
}

// ─── 夹具 ────────────────────────────────────────────────────────────────────

// tlFlow start→apply→review→final→end；reviewProps 控制审批节点的四键/会签配置。
func tlFlow(name, reviewProps string) string {
	r := ""
	if reviewProps != "" {
		r = "," + reviewProps
	}
	return `{"name":"` + name + `","displayName":"时限夹具","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"`, "发起申请"),
			asNode("review", model.TypeTask, `"form":"form-r","assignee":"userA","taskType":0,"performType":0`+r, "审批"),
			asNode("final", model.TypeTask, `"form":"form-f","assignee":"userF","taskType":0,"performType":0`, "终审"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "review"),
			asEdge("e3", "review", "final"), asEdge("e4", "final", "end")}, ",") + `]}`
}

type tlHarness struct {
	eng   *engine.EngineImpl
	repo  *memory.Repository
	sink  *tlFakeSink
	defID int64
	now   time.Time
}

func tlNewHarness(t *testing.T, name, reviewProps string, startAt time.Time) *tlHarness {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "时限夹具", Type: "approval",
		State: 1, Version: 1, Content: []byte(tlFlow(name, reviewProps))}
	repo.AddDefine(def)
	sink := &tlFakeSink{}
	h := &tlHarness{repo: repo, sink: sink, defID: def.ID, now: startAt}
	h.eng = engine.New(repo, asUserProv{}, &asIDGen{}, nil,
		engine.WithClock(func() time.Time { return h.now }),
		engine.WithTimeActionSink(sink))
	return h
}

// tlStartAndReachReview 发起并办结申请节点，返回 review 待办（任务创建时刻＝当时时钟）。
func (h *tlHarness) tlStartAndReachReview(t *testing.T) (*model.ProcessInstance, *model.ProcessTask) {
	t.Helper()
	inst, rows := h.tlReachReviewRows(t)
	if len(rows) != 1 {
		t.Fatalf("期望 1 条审批待办，实得 %d", len(rows))
	}
	return inst, rows[0]
}

// tlReachReviewRows 同上去掉"单条"假设（并行会签档用）。
func (h *tlHarness) tlReachReviewRows(t *testing.T) (*model.ProcessInstance, []*model.ProcessTask) {
	t.Helper()
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "applicant",
		map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起失败: %v", err)
	}
	apply := asSortedDoing(t, h.repo, inst.ID, "apply")
	if len(apply) != 1 {
		t.Fatalf("期望 1 条申请待办，实得 %d", len(apply))
	}
	if _, err := h.eng.ExecuteProcessTask(context.Background(), apply[0].ID, "applicant",
		map[string]interface{}{"submitType": asApply}); err != nil {
		t.Fatalf("办结申请失败: %v", err)
	}
	review := asSortedDoing(t, h.repo, inst.ID, "review")
	if len(review) == 0 {
		t.Fatalf("期望审批待办，实得 0")
	}
	return inst, review
}

func (h *tlHarness) tlDone(t *testing.T, taskID int64, submit int) {
	t.Helper()
	h.tlDoneAs(t, taskID, "userA", submit)
}

func (h *tlHarness) tlDoneAs(t *testing.T, taskID int64, operator string, submit int) {
	t.Helper()
	if _, err := h.eng.ExecuteProcessTask(context.Background(), taskID, operator,
		map[string]interface{}{"submitType": submit}); err != nil {
		t.Fatalf("办理任务 %d 失败: %v", taskID, err)
	}
}

// ─── 建行与时钟 ──────────────────────────────────────────────────────────────

func TestTimeActionsNotConfigured(t *testing.T) {
	h := tlNewHarness(t, "tl_none", "", time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	h.tlStartAndReachReview(t)
	if got := h.sink.snapshot(); len(got) != 0 {
		t.Fatalf("未配置四键不得插动作行，实得 %+v", got)
	}
}

func TestTimeActionsAutoAndReminderOnCreate(t *testing.T) {
	start := time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local)
	h := tlNewHarness(t, "tl_auto_remind",
		`"expireTime":"2h","autoExecute":"Y","reminderTime":"30m","reminderRepeat":"15m"`, start)
	inst, review := h.tlStartAndReachReview(t)

	acts := h.sink.snapshot()
	if len(acts) != 2 {
		t.Fatalf("期望 2 条动作行（自动+首条提醒），实得 %d: %+v", len(acts), acts)
	}
	var auto, remind *spi.TimeAction
	for i := range acts {
		switch acts[i].Type {
		case spi.TimeActionAutoComplete:
			auto = &acts[i]
		case spi.TimeActionRemind:
			remind = &acts[i]
		}
	}
	if auto == nil || remind == nil {
		t.Fatalf("动作类型不全: %+v", acts)
	}
	if !auto.DueAt.Equal(start.Add(2 * time.Hour)) {
		t.Fatalf("自动办理 dueAt 应为创建+2h，实得 %v", auto.DueAt)
	}
	if auto.OccurrenceNo != 0 || auto.IdempotencyKey != fmt.Sprintf("wf:auto:%d:0", review.ID) {
		t.Fatalf("自动办理行契约不符: %+v", auto)
	}
	if !remind.DueAt.Equal(start.Add(30 * time.Minute)) {
		t.Fatalf("首次提醒 dueAt 应为创建+30m，实得 %v", remind.DueAt)
	}
	if remind.OccurrenceNo != 1 || remind.IdempotencyKey != fmt.Sprintf("wf:remind:%d:1", review.ID) {
		t.Fatalf("提醒行契约不符: %+v", remind)
	}
	if auto.TaskID != review.ID || auto.InstanceID != inst.ID || auto.NodeKey != "review" {
		t.Fatalf("动作行身份字段不符: auto=%+v", auto)
	}
	if !review.ExpireTime.Equal(start.Add(2 * time.Hour)) {
		t.Fatalf("expire_time 应随注入时钟计算，实得 %v", review.ExpireTime)
	}
}

func TestTimeActionsAutoExecuteExactY(t *testing.T) {
	for _, v := range []string{"N", "n", "y", "true", ""} {
		props := `"expireTime":"2h"`
		if v != "" {
			props += fmt.Sprintf(`,"autoExecute":%q`, v)
		}
		h := tlNewHarness(t, "tl_auto_"+v, props, time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
		h.tlStartAndReachReview(t)
		for _, a := range h.sink.snapshot() {
			if a.Type == spi.TimeActionAutoComplete {
				t.Fatalf("autoExecute=%q 不得开启自动办理", v)
			}
		}
	}
}

func TestTimeActionsReminderTimeModes(t *testing.T) {
	// 严格 HH:mm：在 09:00 建单 ⇒ 当天 18:00
	h := tlNewHarness(t, "tl_hhmm_1", `"reminderTime":"18:00"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	_, review := h.tlStartAndReachReview(t)
	got := h.sink.snapshot()
	if len(got) != 1 || got[0].Type != spi.TimeActionRemind {
		t.Fatalf("HH:mm 应只插一条提醒: %+v", got)
	}
	if want := time.Date(2026, 11, 19, 18, 0, 0, 0, time.Local); !got[0].DueAt.Equal(want) {
		t.Fatalf("HH:mm 当天档不符：want %v got %v", want, got[0].DueAt)
	}

	// 已过创建时刻 ⇒ 次日同一时刻
	h2 := tlNewHarness(t, "tl_hhmm_2", `"reminderTime":"18:00"`,
		time.Date(2026, 11, 19, 19, 0, 0, 0, time.Local))
	h2.tlStartAndReachReview(t)
	got2 := h2.sink.snapshot()
	if len(got2) != 1 {
		t.Fatalf("HH:mm 次日档应只插一条提醒: %+v", got2)
	}
	if want := time.Date(2026, 11, 20, 18, 0, 0, 0, time.Local); !got2[0].DueAt.Equal(want) {
		t.Fatalf("HH:mm 次日档不符：want %v got %v", want, got2[0].DueAt)
	}
	_ = review

	// 变量档优先：实例变量 dueAt 覆盖相对档
	repo := memory.New()
	def := &model.ProcessDefine{Name: "tl_var", DisplayName: "时限夹具", Type: "approval",
		State: 1, Version: 1, Content: []byte(tlFlow("tl_var", `"reminderTime":"dueAt"`))}
	repo.AddDefine(def)
	sink := &tlFakeSink{}
	eng := engine.New(repo, asUserProv{}, &asIDGen{}, nil, engine.WithTimeActionSink(sink))
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant",
		map[string]interface{}{"dueAt": "2026-12-31 10:00:00"})
	if err != nil {
		t.Fatalf("发起失败: %v", err)
	}
	apply := asSortedDoing(t, repo, inst.ID, "apply")
	if _, err := eng.ExecuteProcessTask(context.Background(), apply[0].ID, "applicant",
		map[string]interface{}{"submitType": asApply}); err != nil {
		t.Fatalf("办结申请失败: %v", err)
	}
	acts := sink.snapshot()
	if len(acts) != 1 {
		t.Fatalf("变量档应插一条提醒: %+v", acts)
	}
	if want := time.Date(2026, 12, 31, 10, 0, 0, 0, time.Local); !acts[0].DueAt.Equal(want) {
		t.Fatalf("变量档不符：want %v got %v", want, acts[0].DueAt)
	}
}

func TestTimeActionsInvalidReminderDoesNotBreakFlow(t *testing.T) {
	h := tlNewHarness(t, "tl_bad_remind", `"reminderTime":"2.5h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	_, review := h.tlStartAndReachReview(t) // 不报错即通过
	if got := h.sink.snapshot(); len(got) != 0 {
		t.Fatalf("非法提醒不应插行: %+v", got)
	}
	h.tlDone(t, review.ID, asAgree)
}

func TestParseReminderRepeat(t *testing.T) {
	if r, ok := engine.ParseReminderRepeat("15m"); !ok || r.N != 15 || r.Unit != 'm' {
		t.Fatalf("15m 解析不符: %+v ok=%v", r, ok)
	}
	if r, ok := engine.ParseReminderRepeat("1d"); !ok || r.N != 1 || r.Unit != 'd' {
		t.Fatalf("1d 解析不符: %+v ok=%v", r, ok)
	}
	for _, bad := range []string{"", "0m", "-5m", "1.5h", "cron", "10"} {
		if _, ok := engine.ParseReminderRepeat(bad); ok {
			t.Fatalf("%q 不应解析成功", bad)
		}
	}
}

// ─── 旁路覆盖与不继承档 ──────────────────────────────────────────────────────

func TestTimeActionsParallelCountersignEveryMember(t *testing.T) {
	h := tlNewHarness(t, "tl_par_cs",
		`"performType":1,"countersignType":"PARALLEL","assignee":"userA,userB","reminderTime":"1h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	_, rows := h.tlReachReviewRows(t)
	if len(rows) != 2 {
		t.Fatalf("并行会签应 2 条成员行，实得 %d", len(rows))
	}
	seen := map[int64]bool{}
	for _, a := range h.sink.snapshot() {
		if a.Type == spi.TimeActionRemind {
			seen[a.TaskID] = true
		}
	}
	for _, r := range rows {
		if !seen[r.ID] {
			t.Fatalf("成员任务 %d 缺提醒动作行", r.ID)
		}
	}
}

func TestTimeActionsSequentialCountersignNextMember(t *testing.T) {
	h := tlNewHarness(t, "tl_seq_cs",
		`"performType":1,"countersignType":"SEQUENTIAL","assignee":"userA,userB","expireTime":"2h","autoExecute":"Y"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	inst, first := h.tlStartAndReachReview(t)
	if !tlHasAction(h.sink, spi.TimeActionAutoComplete, first.ID) {
		t.Fatalf("串行会签首位缺自动办理行")
	}
	// 首位办结 ⇒ 推进出第二位（新 taskId），同样要建动作行
	if _, err := h.eng.ExecuteProcessTask(context.Background(), first.ID, "userA",
		map[string]interface{}{"submitType": asAgree}); err != nil {
		t.Fatalf("办结首位失败: %v", err)
	}
	rows := asSortedDoing(t, h.repo, inst.ID, "review")
	if len(rows) != 1 || rows[0].ID == first.ID {
		t.Fatalf("期望推进出唯一新成员行: %+v", rows)
	}
	if !tlHasAction(h.sink, spi.TimeActionAutoComplete, rows[0].ID) {
		t.Fatalf("串行会签推进出的下一位缺自动办理行（旁路覆盖）")
	}
	if !h.sink.hasTaskCancel(first.ID) {
		t.Fatalf("办结的首位任务应取消 pending 动作行")
	}
}

func TestTimeActionsAidantRowNoActions(t *testing.T) {
	h := tlNewHarness(t, "tl_aidant", `"expireTime":"2h","autoExecute":"Y","reminderTime":"1h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	inst, review := h.tlStartAndReachReview(t)
	before := len(h.sink.snapshot())
	created, err := h.eng.AidantTask(context.Background(), review.ID, []string{"aid1"}, "", false, "userA")
	if err != nil {
		t.Fatalf("协办失败: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("期望 1 条协办行")
	}
	if after := len(h.sink.snapshot()); after != before {
		t.Fatalf("协办行不得继承时限动作（before=%d after=%d）", before, after)
	}
	_ = inst
}

func TestTimeActionsCollectMemberRowsNoActions(t *testing.T) {
	h := tlNewHarness(t, "tl_collect",
		`"dynamicCountersign":true,"expireTime":"2h","autoExecute":"Y","reminderTime":"1h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	inst, review := h.tlStartAndReachReview(t)
	before := len(h.sink.snapshot())

	def, _ := h.repo.FindDefineByID(context.Background(), h.defID)
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		t.Fatalf("解析流程失败: %v", err)
	}
	var node *model.FlowNode
	for i := range flow.Nodes {
		if flow.Nodes[i].ID == "review" {
			node = &flow.Nodes[i]
		}
	}
	if node == nil {
		t.Fatalf("找不到 review 节点")
	}
	agg, _ := h.repo.FindInstanceByID(context.Background(), inst.ID)
	created, err := h.eng.CreateCollectMemberRows(context.Background(), &flow, agg, node, review,
		"userA", []string{"m1", "m2"}, 1, "征询")
	if err != nil {
		t.Fatalf("建征询成员行失败: %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("期望 2 条成员行")
	}
	if after := len(h.sink.snapshot()); after != before {
		t.Fatalf("征询成员行不得继承时限动作（before=%d after=%d）", before, after)
	}
}

// ─── 完成回调与失效联动 ──────────────────────────────────────────────────────

func TestTimeActionsCallbackOnCompleteOnly(t *testing.T) {
	start := time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local)
	h := tlNewHarness(t, "tl_cb", `"callback":"demoHandler","reminderTime":"1h"`, start)
	inst, review := h.tlStartAndReachReview(t)
	h.tlDone(t, review.ID, asAgree)

	var cb *spi.TimeAction
	for _, a := range h.sink.snapshot() {
		if a.Type == spi.TimeActionCallback {
			cb = &a
			break
		}
	}
	if cb == nil {
		t.Fatalf("TASK_COMPLETE 应插 CALLBACK 行")
	}
	if cb.TaskID != review.ID || cb.IdempotencyKey != fmt.Sprintf("wf:callback:%d:0", review.ID) {
		t.Fatalf("回调行契约不符: %+v", cb)
	}
	if !h.sink.hasTaskCancel(review.ID) {
		t.Fatalf("完成的任务应取消 pending 提醒行")
	}
	_ = inst

	// 退回族（TASK_REJECT）不触发回调
	h2 := tlNewHarness(t, "tl_cb_reject", `"callback":"demoHandler"`, start)
	_, review2 := h2.tlStartAndReachReview(t)
	h2.tlDone(t, review2.ID, asReject)
	for _, a := range h2.sink.snapshot() {
		if a.Type == spi.TimeActionCallback {
			t.Fatalf("退回不得触发完成回调: %+v", a)
		}
	}
}

func TestTimeActionsInstanceTerminalCancelsAll(t *testing.T) {
	h := tlNewHarness(t, "tl_end", `"reminderTime":"1h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	inst, review := h.tlStartAndReachReview(t)
	h.tlDone(t, review.ID, asAgree)
	final := asSortedDoing(t, h.repo, inst.ID, "final")
	if len(final) != 1 {
		t.Fatalf("期望终审待办")
	}
	h.tlDoneAs(t, final[0].ID, "userF", asAgree) // 走到 end，实例终态
	if !h.sink.hasInstanceCancel(inst.ID) {
		t.Fatalf("实例终态应取消该实例全部 pending 动作行")
	}
}

func TestTimeActionsWithdrawCancelsAll(t *testing.T) {
	h := tlNewHarness(t, "tl_withdraw", `"reminderTime":"1h"`,
		time.Date(2026, 11, 19, 9, 0, 0, 0, time.Local))
	inst, _ := h.tlStartAndReachReview(t)
	// 引擎侧无撤回入口（门面才有）；这里用 jumpToEnd 拒绝验证"实例终态取消全部动作行"。
	final := asSortedDoing(t, h.repo, inst.ID, "review")
	if len(final) != 1 {
		t.Fatalf("期望审批待办")
	}
	if _, err := h.eng.ExecuteAndJumpToEnd(context.Background(), final[0].ID, "userA",
		map[string]interface{}{"submitType": asReject}); err != nil {
		t.Fatalf("拒绝结束失败: %v", err)
	}
	if !h.sink.hasInstanceCancel(inst.ID) {
		t.Fatalf("拒绝结束（实例终态）应取消该实例全部 pending 动作行")
	}
}

// tlHasAction sink 中是否有指定类型+任务的动作行。
func tlHasAction(sink *tlFakeSink, typ string, taskID int64) bool {
	for _, a := range sink.snapshot() {
		if a.Type == typ && a.TaskID == taskID {
			return true
		}
	}
	return false
}

// ─── 实例级排他锁调用面 ──────────────────────────────────────────────────────

type tlLockedRepo struct {
	*memory.Repository
	mu     sync.Mutex
	locked []int64
}

func (r *tlLockedRepo) LockInstance(_ context.Context, instanceID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.locked = append(r.locked, instanceID)
	return nil
}

func (r *tlLockedRepo) lockedFor(instanceID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range r.locked {
		if id == instanceID {
			return true
		}
	}
	return false
}

func TestTimeActionsLockInstanceCalled(t *testing.T) {
	repo := &tlLockedRepo{Repository: memory.New()}
	def := &model.ProcessDefine{Name: "tl_lock", DisplayName: "时限夹具", Type: "approval",
		State: 1, Version: 1, Content: []byte(tlFlow("tl_lock", ""))}
	repo.AddDefine(def)
	eng := engine.New(repo, asUserProv{}, &asIDGen{}, nil)
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant",
		map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起失败: %v", err)
	}
	apply := asSortedDoing(t, repo.Repository, inst.ID, "apply")
	if _, err := eng.ExecuteProcessTask(context.Background(), apply[0].ID, "applicant",
		map[string]interface{}{"submitType": asApply}); err != nil {
		t.Fatalf("办结申请失败: %v", err)
	}
	if !repo.lockedFor(inst.ID) {
		t.Fatalf("仓储实现 InstanceLocker 时，办理路径必须取得实例级排他保护")
	}
}
