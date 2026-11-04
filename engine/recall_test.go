// 「已办撤回（recall）」验收（issues/246，go 腿）——移植 jeeflow-java 参考实现 90469a2 的
// `RecallTest` 用例形状：复活三件套（原行留 FINISHED＋tf_recallHistory、下游 DOING 废弃、
// 同节点新行名册照旧）、窗口两档（宽口径放行／下游已办结拦零写入）、五条逐字文案、
// 后加签行连带废弃、并行分支全废弃、解析期禁配、实例非 DOING、码 10 TASK_RECALL 事件。
//
// 夹具走内联 JSON（asFlowJSON 同款纪律，**不碰共享 flows 目录**）；断言一律落在
// 读回的持久值（repo.FindDoingTasks／FindHistoryTasks／FindTaskByID）上。
package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

const (
	rcAgree = int(model.SubmitTypeAgree)
	rcApply = int(model.SubmitTypeApply)
	rcOn    = `"allowRecall":true`
)

// rcDeepFlow 六节点链：start→apply→review→final→pay→end，供"下游已办结"窗口档
// （逐字照 java RecallTest.chainDeep）。
func rcDeepFlow(name, reviewProps string) string {
	r := ""
	if reviewProps != "" {
		r = "," + reviewProps
	}
	return `{"name":"` + name + `","displayName":"已办撤回深链","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"`, "发起申请"),
			asNode("review", model.TypeTask, `"form":"form-r","taskType":0,"performType":0,"assignee":"userA"`+r, "审批"),
			asNode("final", model.TypeTask, `"form":"form-f","assignee":"userF","taskType":0,"performType":0`, "终审"),
			asNode("pay", model.TypeTask, `"form":"form-p","assignee":"userP","taskType":0,"performType":0`, "支付"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "review"),
			asEdge("e3", "review", "final"), asEdge("e4", "final", "pay"), asEdge("e5", "pay", "end")}, ",") + `]}`
}

// rcForkFlow 并行双支：review 出两条边到 payA/payB，各自汇入 end；供"分支全废弃"档
// （逐字照 java RecallTest.chainFork）。
func rcForkFlow(name, reviewProps string) string {
	r := ""
	if reviewProps != "" {
		r = "," + reviewProps
	}
	return `{"name":"` + name + `","displayName":"已办撤回并行","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"`, "发起申请"),
			asNode("review", model.TypeTask, `"form":"form-r","taskType":0,"performType":0,"assignee":"userA"`+r, "审批"),
			asNode("payA", model.TypeTask, `"form":"form-a","assignee":"userPA","taskType":0,"performType":0`, "支付甲"),
			asNode("payB", model.TypeTask, `"form":"form-b","assignee":"userPB","taskType":0,"performType":0`, "支付乙"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "review"),
			asEdge("e3", "review", "payA"), asEdge("e4", "review", "payB"),
			asEdge("e5", "payA", "end"), asEdge("e6", "payB", "end")}, ",") + `]}`
}

// rcHarness 任意内容 JSON 的发起夹具（与 asHarness 同形，供深链/并行夹具用）。
func rcHarness(t *testing.T, content string) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: fmt.Sprintf("recall_%d", asSeq.Add(1)), DisplayName: "已办撤回夹具",
		Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return engine.New(repo, asUserProv{}, &asIDGen{}, nil), repo, def.ID
}

// rcAnyRow 不按状态滤——撤回后的下游行是 ABANDON(99)，不走 FINISHED（java anyRow 同形）。
func rcAnyRow(t *testing.T, repo *memory.Repository, instID int64, name string) *model.ProcessTask {
	t.Helper()
	rows, err := repo.FindHistoryTasks(context.Background(), instID)
	if err != nil {
		t.Fatalf("读全部任务行: %v", err)
	}
	for _, r := range rows {
		if r.TaskName == name {
			return r
		}
	}
	return nil
}

// rcRecallLedger tf_recallHistory 账本（两形都收）。
func rcRecallLedger(t *testing.T, repo *memory.Repository, taskID int64) []map[string]interface{} {
	t.Helper()
	row := asMustRow(t, repo, taskID)
	switch v := row.Variables[engine.KeyRecallHistory].(type) {
	case []map[string]interface{}:
		return v
	case []interface{}:
		return readLedgerIface(v)
	}
	return nil
}

func readLedgerIface(v []interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(v))
	for _, item := range v {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

// ═══ 用例（照 java RecallTest 9 格＋码 10 事件格） ═════════════════════════════

// 格 1（java test1）正向：办结→撤回→原行留账本、复活行名册照旧、下游废弃、重办后再推进。
func TestRecallRevivesAbandonsDownstreamAndAllowsRedo(t *testing.T) {
	eng, repo, iid := asStartToReview(t, rcOn)
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("userA 办结 review: %v", err)
	}
	oldReview := rcAnyRow(t, repo, iid, "review")
	if oldReview == nil || !oldReview.IsFinished() {
		t.Fatal("前置：review 应有一条 FINISHED 行")
	}
	if n := asDoingCount(repo, iid, "final"); n != 1 {
		t.Fatalf("前置：final 应有 1 条待办，实得 %d", n)
	}

	resurrected, err := eng.RecallTask(context.Background(), oldReview.ID, "userA")
	if err != nil {
		t.Fatalf("撤回: %v", err)
	}

	if resurrected.TaskName != "review" || !resurrected.IsDoing() {
		t.Errorf("复活行应为 review 且 DOING，实得 %s/%d", resurrected.TaskName, resurrected.TaskState)
	}
	if !containsActor(resurrected.ActorIDs, "userA") {
		t.Errorf("复活行名册应照原行（userA），实得 %v", resurrected.ActorIDs)
	}
	if resurrected.ParentTaskID == nil || oldReview.ParentTaskID == nil || *resurrected.ParentTaskID != *oldReview.ParentTaskID {
		t.Errorf("复活行 parent 应＝原行 parent，实得 %v（原 %v）", resurrected.ParentTaskID, oldReview.ParentTaskID)
	}
	// 下游废弃
	finalRow := rcAnyRow(t, repo, iid, "final")
	if finalRow == nil || finalRow.TaskState != model.TaskStateAbandoned {
		t.Errorf("下游 final 行应废弃（99），实得 %v", finalRow)
	}
	if n := asDoingCount(repo, iid, "final"); n != 0 {
		t.Errorf("下游不应残留待办，实得 %d", n)
	}
	// 原行不改写＋账本指向复活行
	reloaded := asMustRow(t, repo, oldReview.ID)
	if !reloaded.IsFinished() {
		t.Error("原 FINISHED 行不得被改写")
	}
	ledger := rcRecallLedger(t, repo, oldReview.ID)
	if len(ledger) != 1 {
		t.Fatalf("原行应有 1 条 tf_recallHistory，实得 %d", len(ledger))
	}
	if ledger[0]["operator"] != "userA" {
		t.Errorf("账本 operator 应为 userA，实得 %v", ledger[0]["operator"])
	}
	if id, ok := ledger[0]["recalledTaskId"].(int64); !ok || id != resurrected.ID {
		t.Errorf("账本 recalledTaskId 应指向复活行 %d，实得 %#v", resurrected.ID, ledger[0]["recalledTaskId"])
	}
	if ts, _ := ledger[0]["time"].(string); len(ts) != 19 {
		t.Errorf("账本时间须是全生态格式串，实得 %q", ts)
	}
	// 实例仍进行中，重办后再推进
	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst.State != model.InstanceStateDoing {
		t.Errorf("实例应仍进行中（10），实得 %d", inst.State)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), resurrected.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("复活行重办: %v", err)
	}
	if asDoing(t, repo, iid, "final") == nil {
		t.Error("重办后应推进到 final")
	}
	if n := asRowCount(t, repo, iid, "final"); n != 2 {
		t.Errorf("final 应共两行（废弃＋新办），实得 %d", n)
	}
}

// 格 2（java test2）窗口：下游已办结 ⇒ 逐字文案报错且零写入。
func TestRecallBlockedWhenDownstreamFinishedWithZeroWrite(t *testing.T) {
	eng, repo, defID := rcHarness(t, rcDeepFlow("rcdeep", rcOn))
	inst := asStart(t, eng, defID)
	for _, tk := range asSortedDoing(t, repo, inst.ID, "") {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant", asPlain(rcApply)); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}
	review := asDoing(t, repo, inst.ID, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	finalTask := asDoing(t, repo, inst.ID, "final")
	if _, err := eng.ExecuteProcessTask(context.Background(), finalTask.ID, "userF", asPlain(rcAgree)); err != nil {
		t.Fatalf("final 办结: %v", err)
	}
	oldReview := rcAnyRow(t, repo, inst.ID, "review")
	if oldReview == nil || !oldReview.IsFinished() || asDoing(t, repo, inst.ID, "pay") == nil {
		t.Fatal("前置：review 已办结、pay 应有待办")
	}

	_, err := eng.RecallTask(context.Background(), oldReview.ID, "userA")
	if err == nil || err.Error() != "任务[review]的后续节点已办理，不能撤回" {
		t.Fatalf("窗口拦必须逐字报错，实得 %v", err)
	}
	// 零写入：无账本、无复活行、下游原样
	if len(rcRecallLedger(t, repo, oldReview.ID)) != 0 {
		t.Error("报错零写入：原行不得出现账本")
	}
	if f := rcAnyRow(t, repo, inst.ID, "final"); f == nil || !f.IsFinished() {
		t.Error("报错零写入：final 行不得被改写")
	}
	if asDoing(t, repo, inst.ID, "pay") == nil {
		t.Error("报错零写入：pay 待办不得被废弃")
	}
	if n := asDoingCount(repo, inst.ID, "review"); n != 0 {
		t.Errorf("报错零写入：review 不得出现复活行，实得 %d", n)
	}
}

// 格 3（java test3）开关未开 ⇒ 逐字文案。
func TestRecallBlockedWhenSwitchOff(t *testing.T) {
	eng, repo, iid := asStartToReview(t, "")
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	oldReview := rcAnyRow(t, repo, iid, "review")
	_, err := eng.RecallTask(context.Background(), oldReview.ID, "userA")
	if err == nil || err.Error() != "节点[review]未开启 allowRecall，不能撤回" {
		t.Fatalf("开关未开必须逐字报错，实得 %v", err)
	}
}

// 格 4（java test4）非参与者拦、特权放行；进行中行拦。
func TestRecallPermissionAndNotFinishedGuards(t *testing.T) {
	eng, repo, iid := asStartToReview(t, rcOn)
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	oldReview := rcAnyRow(t, repo, iid, "review")
	// 非参与者
	_, err := eng.RecallTask(context.Background(), oldReview.ID, "userZ")
	if err == nil || err.Error() != "无权限撤回该任务" {
		t.Fatalf("非参与者必须逐字报错，实得 %v", err)
	}
	// flow.admin 特权放行
	resurrected, err := eng.RecallTask(context.Background(), oldReview.ID, "flow.admin")
	if err != nil {
		t.Fatalf("flow.admin 应特权放行: %v", err)
	}
	if !resurrected.IsDoing() {
		t.Error("复活行应为 DOING")
	}
	// 进行中行拦（复活行还没办结）
	_, err = eng.RecallTask(context.Background(), resurrected.ID, "userA")
	if err == nil || err.Error() != "任务非已完成状态，不可撤回" {
		t.Fatalf("目标行未办结必须逐字报错，实得 %v", err)
	}
}

// 格 5（java test5）实例已走完 ⇒ 实例非进行中档。
func TestRecallBlockedWhenInstanceFinished(t *testing.T) {
	eng, repo, iid := asStartToReview(t, rcOn)
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	finalTask := asDoing(t, repo, iid, "final")
	if _, err := eng.ExecuteProcessTask(context.Background(), finalTask.ID, "userF", asPlain(rcAgree)); err != nil {
		t.Fatalf("final 办结: %v", err)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("前置：实例应已办结（20），实得 %d", inst.State)
	}
	oldReview := rcAnyRow(t, repo, iid, "review")
	_, err := eng.RecallTask(context.Background(), oldReview.ID, "userA")
	if err == nil || err.Error() != "流程实例非进行中状态，不可撤回" {
		t.Fatalf("实例非进行中必须逐字报错，实得 %v", err)
	}
}

// 格 6（java test6）后加签行属下游 DOING：撤回连带废弃，不留孤儿待办；复活后可正常推进。
func TestRecallAbandonsAddSignRowsToo(t *testing.T) {
	eng, repo, iid := asStartToReview(t, `"allowRecall":true,"allowAddSign":true`)
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA",
		asAgreeOps("userX")); err != nil {
		t.Fatalf("userA 加签给 userX: %v", err)
	}
	addSignRow := asDoing(t, repo, iid, "review")
	if addSignRow == nil || !containsActor(addSignRow.ActorIDs, "userX") {
		t.Fatal("前置：加签行 userX 应在 review 节点续办")
	}
	oldReview := rcAnyRow(t, repo, iid, "review")

	if _, err := eng.RecallTask(context.Background(), oldReview.ID, "userA"); err != nil {
		t.Fatalf("撤回: %v", err)
	}

	// 加签行废弃、复活行是节点上唯一 DOING
	if row := asMustRow(t, repo, addSignRow.ID); row.TaskState != model.TaskStateAbandoned {
		t.Errorf("加签行应连带废弃（99），实得 %d", row.TaskState)
	}
	if n := asDoingCount(repo, iid, "review"); n != 1 {
		t.Fatalf("review 应只剩复活行 1 条待办，实得 %d", n)
	}
	// 复活行照常推进（无兄弟待办 ⇒ merged 出边）
	resurrected := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), resurrected.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("复活行推进: %v", err)
	}
	if asDoing(t, repo, iid, "final") == nil {
		t.Error("复活行办结后应推进到 final")
	}
}

// 格 7（java test7）并行分支：撤回后两支全部废弃。
func TestRecallAbandonsBothForkBranches(t *testing.T) {
	eng, repo, defID := rcHarness(t, rcForkFlow("rcfork", rcOn))
	inst := asStart(t, eng, defID)
	for _, tk := range asSortedDoing(t, repo, inst.ID, "") {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant", asPlain(rcApply)); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}
	review := asDoing(t, repo, inst.ID, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	if asDoing(t, repo, inst.ID, "payA") == nil || asDoing(t, repo, inst.ID, "payB") == nil {
		t.Fatal("前置：payA/payB 两支应各有一条待办")
	}
	oldReview := rcAnyRow(t, repo, inst.ID, "review")

	if _, err := eng.RecallTask(context.Background(), oldReview.ID, "userA"); err != nil {
		t.Fatalf("撤回: %v", err)
	}

	if row := rcAnyRow(t, repo, inst.ID, "payA"); row == nil || row.TaskState != model.TaskStateAbandoned {
		t.Errorf("payA 行应废弃（99），实得 %v", row)
	}
	if row := rcAnyRow(t, repo, inst.ID, "payB"); row == nil || row.TaskState != model.TaskStateAbandoned {
		t.Errorf("payB 行应废弃（99），实得 %v", row)
	}
	if n := asDoingCount(repo, inst.ID, "review"); n != 1 {
		t.Errorf("review 应只剩复活行，实得 %d", n)
	}
}

// 格 8（java test8）解析期禁配：会签节点/申请节点配 allowRecall 直接报错；非法布尔值同拦。
func TestRecallParseBans(t *testing.T) {
	asAssertParseFails(t, asApplyProps, rcOn+`,"performType":1,"countersignType":"PARALLEL"`, "会签节点")
	asAssertParseFails(t, asApplyProps, rcOn+`,"performType":1,"countersignType":"PARALLEL"`, "不允许配置 allowRecall")
	asAssertParseFails(t, asApplyProps+","+rcOn, rcOn, "首任务节点")
	asAssertParseFails(t, asApplyProps+","+rcOn, rcOn, "allowRecall")
	// 非布尔值解析期报配置错误（不静默当缺省）
	asAssertParseFails(t, asApplyProps, `"allowRecall":"yes"`, "allowRecall 配置非法")
}

// 格 9（java test9）消息常量与 spec 06 逐字文案一致性自证（防常量被改后测试仍绿）。
func TestRecallPinnedMessages(t *testing.T) {
	pinned := map[string]string{
		engine.MsgRecallWindow:         "任务[%s]的后续节点已办理，不能撤回",
		engine.MsgRecallSwitchOff:      "节点[%s]未开启 allowRecall，不能撤回",
		engine.MsgRecallNotFinished:    "任务非已完成状态，不可撤回",
		engine.MsgRecallNoPermission:   "无权限撤回该任务",
		engine.MsgRecallInstanceNotDoing: "流程实例非进行中状态，不可撤回",
	}
	for got, want := range pinned {
		if got != want {
			t.Errorf("文案漂移：%q 应为 %q", got, want)
		}
	}
	if engine.PropAllowRecall != "allowRecall" || engine.KeyRecallHistory != "tf_recallHistory" {
		t.Errorf("属性/协议键漂移：%q/%q", engine.PropAllowRecall, engine.KeyRecallHistory)
	}
}

// 格 10（go 补格，对齐 spec 11 码 10）：撤回落库后 fire 码 10 TASK_RECALL（sourceId=旧行），
// 复活行待办另走码 3；只发 10 不补发 5/6（撤回不是办结也不是拒绝）。
func TestRecallFiresTaskRecallEvent(t *testing.T) {
	eng, repo, iid := asStartToReview(t, rcOn)
	review := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), review.ID, "userA", asPlain(rcAgree)); err != nil {
		t.Fatalf("review 办结: %v", err)
	}
	oldReview := rcAnyRow(t, repo, iid, "review")

	type evtRec struct {
		typ  engine.EventType
		task int64
		inst int64
		op   string
	}
	var events []evtRec
	eng.SetExtensions(&engine.Extensions{Listeners: []engine.ProcessEventListener{
		func(_ context.Context, e engine.ProcessEvent) {
			events = append(events, evtRec{typ: e.Type, task: e.TaskID, inst: e.InstanceID, op: e.Operator})
		},
	}})
	if _, err := eng.RecallTask(context.Background(), oldReview.ID, "userA"); err != nil {
		t.Fatalf("撤回: %v", err)
	}
	var starts, completes, rejects, recalls int
	for _, e := range events {
		switch e.typ {
		case engine.EventProcessTaskStart:
			starts++
		case engine.EventTaskComplete:
			completes++
		case engine.EventTaskReject:
			rejects++
		case engine.EventTaskRecall:
			recalls++
			if e.task != oldReview.ID {
				t.Errorf("码 10 sourceId 应＝被撤旧行 %d，实得 %d", oldReview.ID, e.task)
			}
			if e.inst != iid || e.op != "userA" {
				t.Errorf("码 10 载荷应为 instanceId=%d/operator=userA，实得 %d/%s", iid, e.inst, e.op)
			}
		}
	}
	if recalls != 1 {
		t.Errorf("撤回应恰好 fire 一次码 10，实得 %d", recalls)
	}
	if completes != 0 || rejects != 0 {
		t.Errorf("撤回不得补发 5/6，实得 complete=%d reject=%d", completes, rejects)
	}
	if starts != 1 {
		t.Errorf("复活行待办应走既有建单腿 fire 一次码 3，实得 %d", starts)
	}
}
