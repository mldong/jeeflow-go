// 「动态会签（征询模式 dynamicCountersign）」验收（issues/247，go 腿）——移植 jeeflow-java
// 参考实现 ffe2749 的 `DynamicCountersignTest` 用例形状：发起不办结主办行＋响应 marker、
// 成员行三戳（parent/轮次/reason）、账本含 reason、闸门逐字文案挡全部 submitType、
// 成员行禁拉人、重复/给自己拦、撤回接口恢复闸门且已交意见保留、多轮征询跨轮计数、
// detail 投影 active 翻转、三条解析期硬校验＋非法布尔值、征询档推进闸
// （成员办结不流转、主办拍板才流转）、撤回动态会签非参与者文案。
//
// 发起/闸门/撤回在门面分流——测试经 facade.Flow 驱动（与 java 同层）；夹具内联 JSON，
// 不碰共享 flows 目录。
package facade_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

var dcSeq int

func dcSeqAdd(v int) int { dcSeq += v; return dcSeq }

const dcCollectOn = `"dynamicCountersign":true`

// dcFlow 五节点链：start→apply→review→final→end（逐字照 java DynamicCountersignTest.chain）。
func dcFlow(name, reviewProps string) string {
	r := ""
	if reviewProps != "" {
		r = "," + reviewProps
	}
	return `{"name":"` + name + `","displayName":"动态会签夹具","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","x":0,"y":0,"properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","x":0,"y":0,"properties":{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"},"text":{"value":"发起申请"}},` +
		`{"id":"review","type":"snaker:task","x":0,"y":0,"properties":{"form":"form-r","taskType":0,"performType":0,"assignee":"userA"` + r + `},"text":{"value":"审批"}},` +
		`{"id":"final","type":"snaker:task","x":0,"y":0,"properties":{"form":"form-f","assignee":"userF","taskType":0,"performType":0},"text":{"value":"终审"}},` +
		`{"id":"end","type":"snaker:end","x":0,"y":0,"properties":{},"text":{"value":"结束"}}` +
		`],"edges":[` +
		`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e2","sourceNodeId":"apply","targetNodeId":"review","properties":{}},` +
		`{"id":"e3","sourceNodeId":"review","targetNodeId":"final","properties":{}},` +
		`{"id":"e4","sourceNodeId":"final","targetNodeId":"end","properties":{}}]}`
}

// dcSetup 部署夹具并走 startAndExecute（自动办结申请节点），返回停在 review 的（门面、仓储、实例 id）。
func dcSetup(t *testing.T, reviewProps string) (*facadeHelper, *memory.Repository, int64) {
	t.Helper()
	f, repo, _ := setupFacade()
	name := fmt.Sprintf("collect_%d", dcSeqAdd(1))
	defID := dcDeploy(t, f, name, dcFlow(name, reviewProps))
	r := f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": fmt.Sprintf("%d", defID), "operator": "applicant",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("发起失败: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	idStr, _ := data["processInstanceId"].(string)
	var iid int64
	fmt.Sscanf(idStr, "%d", &iid)
	if iid == 0 {
		t.Fatalf("startAndExecute 未返回实例 id: %v", r)
	}
	return &facadeHelper{f}, repo, iid
}

func dcDeploy(t *testing.T, f facadeFlow, name, content string) int64 {
	t.Helper()
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content, "operator": "system"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("部署失败: %v", r)
	}
	data, _ := r["data"].(map[string]interface{})
	idStr, _ := data["processDefineId"].(string)
	var id int64
	fmt.Sscanf(idStr, "%d", &id)
	if id == 0 {
		t.Fatalf("deploy 未返回定义 id: %v", r)
	}
	return id
}

// facadeFlow / facadeHelper：dcDeploy 与 dcSetup 各自需要的最小接口（不与既有测试助手重名）。
type facadeFlow interface {
	Flow(action string, args map[string]interface{}) map[string]interface{}
}

type facadeHelper struct {
	f facadeFlow
}

// exec processTask/execute 信封。
func (h *facadeHelper) exec(taskID int64, operator string, submitType int, extra map[string]interface{}) map[string]interface{} {
	a := map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", taskID),
		"operator":      operator,
		"submitType":    submitType,
	}
	for k, v := range extra {
		a[k] = v
	}
	return h.f.Flow("processTask/execute", a)
}

// collect 经门面发起动态会签（带 tf_addSignOperators 的 AGREE 提交）。
func (h *facadeHelper) collect(host *model.ProcessTask, operator string, operators []interface{}, reason string) map[string]interface{} {
	a := map[string]interface{}{
		"processTaskId":        fmt.Sprintf("%d", host.ID),
		"operator":             operator,
		"submitType":           int(model.SubmitTypeAgree),
		engine.KeyAddSignOperators: operators,
	}
	if reason != "" {
		a[engine.KeyAddSignReason] = reason
	}
	return h.f.Flow("processTask/execute", a)
}

func dcData(resp map[string]interface{}) map[string]interface{} {
	m, _ := resp["data"].(map[string]interface{})
	return m
}

func dcDoing(t *testing.T, repo *memory.Repository, instID int64, name string) *model.ProcessTask {
	t.Helper()
	rows, err := repo.FindDoingTasks(context.Background(), instID, []string{name})
	if err != nil {
		t.Fatalf("读 DOING 行: %v", err)
	}
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func dcDoingCount(t *testing.T, repo *memory.Repository, instID int64, name string) int {
	t.Helper()
	rows, err := repo.FindDoingTasks(context.Background(), instID, []string{name})
	if err != nil {
		t.Fatalf("读 DOING 行: %v", err)
	}
	return len(rows)
}

// dcMemberRows 经仓储读回成员行（parent＝主办行且同节点同名；断言只认读回的持久值）。
func dcMemberRows(t *testing.T, repo *memory.Repository, instID int64, host *model.ProcessTask) []*model.ProcessTask {
	t.Helper()
	rows, err := repo.FindHistoryTasks(context.Background(), instID)
	if err != nil {
		t.Fatalf("读全部任务行: %v", err)
	}
	var out []*model.ProcessTask
	for _, r := range rows {
		if r.ParentTaskID != nil && *r.ParentTaskID == host.ID && r.TaskName == host.TaskName {
			out = append(out, r)
		}
	}
	return out
}

// dcRound 成员行变量里的轮次（键名逐字钉住 addSignRound_{节点名}）。
func dcRound(task *model.ProcessTask, nodeID string) int {
	if task == nil || task.Variables == nil {
		return 0
	}
	switch v := task.Variables["addSignRound_"+nodeID].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

// dcLedger 主办行账本（tf_addSignHistory，两形都收）。
func dcLedger(t *testing.T, repo *memory.Repository, taskID int64) []map[string]interface{} {
	t.Helper()
	row, err := repo.FindTaskByID(context.Background(), taskID)
	if err != nil || row == nil {
		t.Fatalf("读任务行 %d: %v", taskID, err)
	}
	switch v := row.Variables[engine.KeyAddSignHistory].(type) {
	case []map[string]interface{}:
		return v
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// ═══ 用例（照 java DynamicCountersignTest 8 格） ══════════════════════════════

// 格 1（java test1）发起：不办结主办行、成员行三戳齐、账本含 reason、响应 marker。
func TestCollectSubmitKeepsHostDoingAndStampsMemberRows(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	resp := f.collect(host, "userA", []interface{}{"userX", "userY"}, "涉及地规划需两部门意见")
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("发起应成功: %v", resp)
	}
	dc := dcData(resp)["dynamicCountersign"].(map[string]interface{})
	if dc["round"] != 1 || dc["total"] != 2 || dc["submitted"] != 0 {
		t.Errorf("marker 应为 round=1 total=2 submitted=0，实得 %v", dc)
	}
	// 主办行仍 DOING（读回的持久值）
	hostRow, _ := repo.FindTaskByID(context.Background(), host.ID)
	if !hostRow.IsDoing() {
		t.Errorf("主办行不得被办结，实得 %d", hostRow.TaskState)
	}
	// 成员行三戳
	members := dcMemberRows(t, repo, iid, host)
	if len(members) != 2 {
		t.Fatalf("应建两条成员行，实得 %d", len(members))
	}
	for _, m := range members {
		if !m.IsDoing() {
			t.Errorf("成员行应 DOING，实得 %d", m.TaskState)
		}
		if m.ParentTaskID == nil || *m.ParentTaskID != host.ID {
			t.Errorf("成员行 parent 应＝主办行 %d，实得 %v", host.ID, m.ParentTaskID)
		}
		if got := dcRound(m, "review"); got != 1 {
			t.Errorf("成员行轮次应为 1，实得 %d", got)
		}
		if got, _ := m.Variables[engine.KeyAddSignReason].(string); got != "涉及地规划需两部门意见" {
			t.Errorf("成员行应带 reason，实得 %q", got)
		}
	}
	// 账本含 reason、只一条
	lg := dcLedger(t, repo, host.ID)
	if len(lg) != 1 {
		t.Fatalf("主办行应有 1 条账本，实得 %d", len(lg))
	}
	if lg[0]["reason"] != "涉及地规划需两部门意见" {
		t.Errorf("账本应含 reason，实得 %v", lg[0]["reason"])
	}
	if lg[0]["fromActor"] != "userA" {
		t.Errorf("账本 fromActor 应为 userA，实得 %v", lg[0]["fromActor"])
	}
}

// 格 2（java test2）闸门：未收齐时主办按任意 submitType 办结 ⇒ 逐字文案报错且零写入。
func TestCollectGateBlocksAllSubmitTypesUntilCollected(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	f.collect(host, "userA", []interface{}{"userX", "userY"}, "")
	for _, submitType := range []int{int(model.SubmitTypeAgree), int(model.SubmitTypeReject)} {
		resp := f.exec(host.ID, "userA", submitType, nil)
		if code, _ := resp["code"].(int); code != 99999999 {
			t.Fatalf("submitType=%d 应被闸门拦下: %v", submitType, resp)
		}
		if msg, _ := resp["msg"].(string); msg != "节点[review]动态会签意见未收齐（已提交 0/2），不能办理" {
			t.Fatalf("闸门文案必须逐字，实得 %q", msg)
		}
	}
	// 零写入：主办仍 DOING、成员行未被办结、行数不增
	hostRow, _ := repo.FindTaskByID(context.Background(), host.ID)
	if !hostRow.IsDoing() {
		t.Error("报错零写入：主办行不得被办结")
	}
	if n := dcDoingCount(t, repo, iid, "review"); n != 3 {
		t.Errorf("review 应为主办＋两成员共 3 条 DOING，实得 %d", n)
	}
}

// 格 3（java test3）收齐后主办拍板推进；投影 active 随提交翻转为 false。
func TestCollectHostProceedsAfterAllSubmittedWithProjection(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	f.collect(host, "userA", []interface{}{"userX", "userY"}, "")
	members := dcMemberRows(t, repo, iid, host)
	// 逐个提交意见（意见落各自行的 tf_approvalComment）
	for i, m := range members {
		resp := f.exec(m.ID, m.ActorIDs[0], int(model.SubmitTypeAgree),
			map[string]interface{}{"tf_approvalComment": fmt.Sprintf("意见%d", i+1)})
		if code, _ := resp["code"].(int); code != 0 {
			t.Fatalf("成员办结应成功: %v", resp)
		}
		if dcDoing(t, repo, iid, "final") != nil {
			t.Fatal("成员办结不得流转（主办还没拍板）")
		}
	}
	// 投影：submitted=2、active=false
	detail := f.f.Flow("processTask/detail", map[string]interface{}{"id": fmt.Sprintf("%d", host.ID)})
	ext, _ := dcData(detail)["ext"].(map[string]interface{})
	dc, _ := ext["dynamicCountersign"].(map[string]interface{})
	if dc == nil {
		t.Fatal("detail 投影应有 ext.dynamicCountersign")
	}
	if dc["active"] != false {
		t.Errorf("投影 active 应为 false，实得 %v", dc["active"])
	}
	if dc["total"] != 2 || dc["submitted"] != 2 {
		t.Errorf("投影应为 total=2 submitted=2，实得 %v", dc)
	}
	actors, _ := dc["actors"].([]interface{})
	if len(actors) != 2 {
		t.Errorf("投影 actors 应有 2 条，实得 %d", len(actors))
	}
	// 主办拍板 → 推进
	resp := f.exec(host.ID, "userA", int(model.SubmitTypeAgree), nil)
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("主办拍板应成功: %v", resp)
	}
	if dcDoing(t, repo, iid, "final") == nil {
		t.Error("收齐后主办拍板应推进到 final")
	}
}

// 格 4（java test4）成员行携带拉人参数 ⇒ 报错；重复拉同一人 ⇒ 报错；加给自己 ⇒ 报错。
func TestCollectMemberPullDuplicateAndSelfGuards(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	f.collect(host, "userA", []interface{}{"userX"}, "")
	members := dcMemberRows(t, repo, iid, host)
	// 成员行拉人
	resp := f.exec(members[0].ID, "userX", int(model.SubmitTypeAgree),
		map[string]interface{}{engine.KeyAddSignOperators: "userZ"})
	if msg, _ := resp["msg"].(string); msg != "动态会签成员任务不能发起加签" {
		t.Fatalf("成员拉人必须逐字报错，实得 %q", msg)
	}
	// 重复拉 pending 中的人
	resp2 := f.collect(host, "userA", []interface{}{"userX"}, "")
	if msg, _ := resp2["msg"].(string); msg != "操作人[userX]已在节点[review]的动态会签中" {
		t.Fatalf("重复拉人必须逐字报错，实得 %q", msg)
	}
	// 加给自己
	resp3 := f.collect(host, "userA", []interface{}{"userA"}, "")
	if msg, _ := resp3["msg"].(string); msg != "不能把任务加签给自己" {
		t.Fatalf("加给自己必须逐字报错，实得 %q", msg)
	}
}

// 格 5（java test5）撤回：未答废弃、已交保留、账本记撤回、闸门恢复；重复撤回归其逐字文案。
func TestCollectRevokeRestoresGateAndKeepsSubmittedOpinions(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	f.collect(host, "userA", []interface{}{"userX", "userY"}, "")
	members := dcMemberRows(t, repo, iid, host)
	var xRow, yRow *model.ProcessTask
	for _, m := range members {
		if containsStr2(m.ActorIDs, "userX") {
			xRow = m
		} else {
			yRow = m
		}
	}
	if resp := f.exec(xRow.ID, "userX", int(model.SubmitTypeAgree),
		map[string]interface{}{"tf_approvalComment": "甲部门意见"}); codeOf(resp) != 0 {
		t.Fatalf("userX 交意见: %v", resp)
	}

	resp := f.f.Flow("processTask/revokeDynamicCountersign", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA", "reason": "部门选错重新拉",
	})
	if codeOf(resp) != 0 {
		t.Fatalf("撤回应成功: %v", resp)
	}
	if got := dcData(resp)["revoked"]; got != 1 {
		t.Errorf("应只撤未答的 1 条，实得 %v", got)
	}
	// 已交保留（FINISHED）、未答废弃（99）
	if row, _ := repo.FindTaskByID(context.Background(), xRow.ID); !row.IsFinished() {
		t.Errorf("已交意见行应保留，实得 %d", row.TaskState)
	}
	if row, _ := repo.FindTaskByID(context.Background(), yRow.ID); row.TaskState != model.TaskStateAbandoned {
		t.Errorf("未答成员行应废弃（99），实得 %d", row.TaskState)
	}
	// 账本两条（发起＋撤回，撤回条带 revoked 标记）
	lg := dcLedger(t, repo, host.ID)
	if len(lg) != 2 {
		t.Fatalf("账本应有 2 条，实得 %d", len(lg))
	}
	if lg[1]["revoked"] != true {
		t.Errorf("撤回条应带 revoked=true，实得 %v", lg[1]["revoked"])
	}
	if lg[1]["reason"] != "部门选错重新拉" {
		t.Errorf("撤回条应带 reason，实得 %v", lg[1]["reason"])
	}
	// 闸门恢复：主办可直接拍板推进
	goResp := f.exec(host.ID, "userA", int(model.SubmitTypeAgree), nil)
	if codeOf(goResp) != 0 {
		t.Fatalf("撤回后闸门应恢复: %v", goResp)
	}
	if dcDoing(t, repo, iid, "final") == nil {
		t.Error("撤回后主办拍板应推进")
	}
	// 撤回已无待答 → 逐字文案
	again := f.f.Flow("processTask/revokeDynamicCountersign", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA",
	})
	if msg, _ := again["msg"].(string); msg != "节点[review]没有进行中的动态会签" {
		t.Fatalf("无待答撤回必须逐字报错，实得 %q", msg)
	}
}

// 格 6（java test6）多轮征询：跨轮意见互不覆盖，轮次递增，收齐判据跨轮计数。
func TestCollectMultiRoundCollectCountsAcrossRounds(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	r1 := f.collect(host, "userA", []interface{}{"userX"}, "")
	if dcData(r1)["dynamicCountersign"].(map[string]interface{})["round"] != 1 {
		t.Fatalf("第一轮应为 1: %v", r1)
	}
	xRow := dcMemberRows(t, repo, iid, host)[0]
	if resp := f.exec(xRow.ID, "userX", int(model.SubmitTypeAgree), nil); codeOf(resp) != 0 {
		t.Fatalf("userX 交意见: %v", resp)
	}
	// 第二轮（闸门已开时仍可再拉）
	r2 := f.collect(host, "userA", []interface{}{"userY"}, "")
	if dcData(r2)["dynamicCountersign"].(map[string]interface{})["round"] != 2 {
		t.Fatalf("第二轮应为 2: %v", r2)
	}
	// 跨轮收齐判据：x 已交、y 未交 ⇒ 1/2
	resp := f.exec(host.ID, "userA", int(model.SubmitTypeAgree), nil)
	if msg, _ := resp["msg"].(string); msg != "节点[review]动态会签意见未收齐（已提交 1/2），不能办理" {
		t.Fatalf("跨轮计数文案必须逐字，实得 %q", msg)
	}
	// 剩余未答成员行逐个办结
	for _, m := range dcMemberRows(t, repo, iid, host) {
		if !m.IsDoing() {
			continue // 第 1 轮已提交的行不再重复办结
		}
		if resp := f.exec(m.ID, m.ActorIDs[0], int(model.SubmitTypeAgree), nil); codeOf(resp) != 0 {
			t.Fatalf("成员办结: %v", resp)
		}
	}
	goResp := f.exec(host.ID, "userA", int(model.SubmitTypeAgree), nil)
	if codeOf(goResp) != 0 {
		t.Fatalf("收齐后拍板: %v", goResp)
	}
	if dcDoing(t, repo, iid, "final") == nil {
		t.Error("收齐后应推进到 final")
	}
}

// 格 7（java test7）三条解析期硬校验：双开／会签节点／申请节点＋非法布尔值。
func TestCollectParseBansMutexCountersignAndApplyNodes(t *testing.T) {
	f, _, _ := setupFacade()
	assertDcDeployFails(t, f, dcCollectOn+`,"allowAddSign":true`, "不允许同时配置 allowAddSign 与 dynamicCountersign")
	assertDcDeployFails(t, f, dcCollectOn+`,"performType":1,"countersignType":"PARALLEL"`, "会签节点[review]不允许配置 dynamicCountersign")
	// 申请节点（start 直接后继）配 dynamicCountersign ⇒ 解析期报错
	bad := strings.Replace(dcFlow("collect_apply_bad", dcCollectOn),
		`"assignee":"applicant"}`, `"assignee":"applicant","dynamicCountersign":true}`, 1)
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": bad, "operator": "system"})
	// 部署腿不跑解析校验（go 的解析校验挂在发起/执行入口），拦截应发生在 startAndExecute
	if code, _ := r["code"].(int); code != 0 {
		if msg, _ := r["msg"].(string); strings.Contains(msg, "申请节点[apply]不允许配置 dynamicCountersign") {
			return
		}
	}
	start := f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": dcData(r)["processDefineId"], "operator": "applicant",
	})
	if msg, _ := start["msg"].(string); !strings.Contains(msg, "申请节点[apply]不允许配置 dynamicCountersign") {
		t.Fatalf("申请节点禁配必须解析期报错，实得 %v", start)
	}
	// 非布尔值解析期报配置错误
	f2, _, _ := setupFacade()
	assertDcDeployFails(t, f2, `"dynamicCountersign":"yes"`, "dynamicCountersign 配置非法")
}

func assertDcDeployFails(t *testing.T, f facadeFlow, reviewProps, wantFragment string) {
	t.Helper()
	name := fmt.Sprintf("collect_bad_%d", dcSeqAdd(1))
	defID := dcDeploy(t, f, name, dcFlow(name, reviewProps))
	start := f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": fmt.Sprintf("%d", defID), "operator": "applicant",
	})
	if msg, _ := start["msg"].(string); !strings.Contains(msg, wantFragment) {
		t.Fatalf("解析期禁配应报含 %q 的错，实得 %v", wantFragment, start)
	}
}

// 格 8（java test8）消息常量与 spec 06 逐字文案一致性自证（防常量被改后测试仍绿）。
func TestCollectPinnedMessages(t *testing.T) {
	pinned := map[string]string{
		engine.MsgCollectPending:          "节点[%s]动态会签意见未收齐（已提交 %d/%d），不能办理",
		engine.MsgCollectNoPending:        "节点[%s]没有进行中的动态会签",
		engine.MsgCollectNoPermissionRevoke: "无权限撤回该动态会签",
		engine.MsgCollectMemberPull:       "动态会签成员任务不能发起加签",
		engine.MsgCollectDuplicate:        "操作人[%s]已在节点[%s]的动态会签中",
	}
	for got, want := range pinned {
		if got != want {
			t.Errorf("文案漂移：%q 应为 %q", got, want)
		}
	}
	if engine.PropDynamicCountersign != "dynamicCountersign" || engine.KeyAddSignReason != "tf_addSignReason" {
		t.Errorf("属性/协议键漂移：%q/%q", engine.PropDynamicCountersign, engine.KeyAddSignReason)
	}
}

// 格 9（go 补格）：revokeDynamicCountersign 非参与者 ⇒ 逐字「无权限撤回该动态会签」；
// 撤回后轮次不消耗（下一轮 round 续接，不被撤回重置）。
func TestCollectRevokePermissionAndRoundKept(t *testing.T) {
	f, repo, iid := dcSetup(t, dcCollectOn)
	host := dcDoing(t, repo, iid, "review")
	f.collect(host, "userA", []interface{}{"userX"}, "")
	resp := f.f.Flow("processTask/revokeDynamicCountersign", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userZ",
	})
	if msg, _ := resp["msg"].(string); msg != "无权限撤回该动态会签" {
		t.Fatalf("非参与者撤回必须逐字报错，实得 %q", msg)
	}
	// 撤回后（userA 撤、待答清零）再拉：轮次续接为 2（撤回不消耗）
	rv := f.f.Flow("processTask/revokeDynamicCountersign", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA",
	})
	if codeOf(rv) != 0 {
		t.Fatalf("userA 撤回应成功: %v", rv)
	}
	r2 := f.collect(host, "userA", []interface{}{"userY"}, "")
	if dcData(r2)["dynamicCountersign"].(map[string]interface{})["round"] != 2 {
		t.Errorf("撤回后轮次不消耗，应续接为 2: %v", r2)
	}
}

func codeOf(resp map[string]interface{}) int {
	code, _ := resp["code"].(int)
	return code
}

