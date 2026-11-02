// 「后加签（可重复）」验收（go 腿）——移植 jeeflow-java 参考实现的 `AddSignAfterTest` 16 格语义：
// 链式续办、多选等同圈兄弟、四类写入口报错且零写入、三条解析期硬校验、拒绝/退回发起人废弃
// 未完成加签、回归（不开开关逐字不变）、并发只放行一次、根因守卫（tf_ 不进实例变量）、
// 共享夹具 19 跑通链。
//
// 夹具走内联 JSON（asFlowJSON），**不碰共享 flows 目录**——只有第 16 格例外，它必须真读
// flows/19-add-sign-after.json（七语言仓 flows/ 是 java 编辑源的入库副本，没有生产者核对过的
// 夹具会在传播轮里各语言各自"改一份能跑的"）。
//
// 断言一律落在**读回的持久值**（repo.FindDoingTasks／FindHistoryTasks／FindTaskByID）上，
// 不是引擎返回的聚合对象（issues/113 教训：只有读回值能证明那一行真进了库）。
package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/internal/flowsutil"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// ─── 测试 SPI 与夹具 ──────────────────────────────────────────────────────────

type asUserProv struct{}

func (asUserProv) GetUser(string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: "u", RealName: "用户u"}, nil
}

type asIDGen struct{ n int64 }

func (g *asIDGen) NextID() int64 { g.n++; return g.n }

const (
	asAgree      = int(model.SubmitTypeAgree)
	asApply      = int(model.SubmitTypeApply)
	asReject     = int(model.SubmitTypeReject)
	asReturn     = int(model.SubmitTypeRollbackToOperator)
	asOn         = `"allowAddSign":true`
	asApplyProps = `"assignee":"applicant"`
)

// asNode／asEdge：与 java 测试的 node()/edge() 同形的片段生成器。
func asNode(id, typ, props, text string) string {
	return fmt.Sprintf(`{"id":"%s","type":"%s","x":0,"y":0,"properties":{%s},"text":{"value":"%s"}}`, id, typ, props, text)
}

func asEdge(id, from, to string) string {
	return fmt.Sprintf(`{"id":"%s","type":"snaker:transition","sourceNodeId":"%s","targetNodeId":"%s","properties":{}}`, id, from, to)
}

// asFlowJSON start → apply → review → final → end；两参数分别控制申请节点与审批节点的
// properties（逐字照 java 的 chain(applyProps, reviewProps)）。
func asFlowJSON(name, applyProps, reviewProps string) string {
	a := applyProps
	if a == "" {
		a = asApplyProps
	}
	r := ""
	if reviewProps != "" {
		r = "," + reviewProps
	}
	return `{"name":"` + name + `","displayName":"后加签夹具","type":"approval","nodes":[` +
		strings.Join([]string{
			asNode("start", model.TypeStart, "", "开始"),
			asNode("apply", model.TypeTask, `"form":"apply-form","taskType":0,"performType":0,`+a, "发起申请"),
			asNode("review", model.TypeTask, `"form":"form-r","assignee":"userA","taskType":0,"performType":0`+r, "审批"),
			asNode("final", model.TypeTask, `"form":"form-f","assignee":"userF","taskType":0,"performType":0`, "终审"),
			asNode("end", model.TypeEnd, "", "结束"),
		}, ",") + `],"edges":[` +
		strings.Join([]string{asEdge("e1", "start", "apply"), asEdge("e2", "apply", "review"),
			asEdge("e3", "review", "final"), asEdge("e4", "final", "end")}, ",") + `]}`
}

func asHarness(t *testing.T, name, content string) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "后加签夹具", Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return engine.New(repo, asUserProv{}, &asIDGen{}, nil), repo, def.ID
}

func asStart(t *testing.T, eng *engine.EngineImpl, defID int64) *model.ProcessInstance {
	t.Helper()
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起流程失败: %v", err)
	}
	return inst
}

// asSortedDoing 某节点（nodeID 为空＝全部节点）的 DOING 行，按 id 升序（内存仓遍历 map 无序）
func asSortedDoing(t *testing.T, repo *memory.Repository, instID int64, nodeID string) []*model.ProcessTask {
	t.Helper()
	var names []string
	if nodeID != "" {
		names = []string{nodeID}
	}
	rows, err := repo.FindDoingTasks(context.Background(), instID, names)
	if err != nil {
		t.Fatalf("读 DOING 行: %v", err)
	}
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j].ID < rows[j-1].ID; j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
	return rows
}

func asDoing(t *testing.T, repo *memory.Repository, instID int64, nodeID string) *model.ProcessTask {
	t.Helper()
	rows := asSortedDoing(t, repo, instID, nodeID)
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func asDoingCount(repo *memory.Repository, instID int64, nodeID string) int {
	rows, _ := repo.FindDoingTasks(context.Background(), instID, []string{nodeID})
	return len(rows)
}

// asRowCount 该节点的**全部**任务行数（含已办结与废弃，＝java 的 rows()）
func asRowCount(t *testing.T, repo *memory.Repository, instID int64, nodeID string) int {
	t.Helper()
	rows, err := repo.FindHistoryTasks(context.Background(), instID)
	if err != nil {
		t.Fatalf("读全部任务行: %v", err)
	}
	n := 0
	for _, r := range rows {
		if r.TaskName == nodeID {
			n++
		}
	}
	return n
}

// asFirstActor 进行中任务的 actor_id 列恒空是家族不变量（办结才写），取办理人要用参与者列表
func asFirstActor(task *model.ProcessTask) string {
	if len(task.ActorIDs) == 0 {
		return ""
	}
	return task.ActorIDs[0]
}

func asAgreeOps(operators interface{}) map[string]interface{} {
	return map[string]interface{}{"submitType": asAgree, engine.KeyAddSignOperators: operators}
}

func asPlain(submitType int) map[string]interface{} {
	return map[string]interface{}{"submitType": submitType}
}

// asRound 任务变量里的加签轮次（键名逐字钉住 addSignRound_{节点名}；缺键＝0）
func asRound(task *model.ProcessTask, nodeID string) int {
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

// asHistory 账本（tf_addSignHistory）——两形都收（内存 []map、仓储往返 []interface{}），
// 键名逐字钉在 engine.KeyAddSignHistory 上。
func asHistory(t *testing.T, repo *memory.Repository, taskID int64) []map[string]interface{} {
	t.Helper()
	task, err := repo.FindTaskByID(context.Background(), taskID)
	if err != nil || task == nil {
		t.Fatalf("读任务行 %d: %v", taskID, err)
	}
	switch v := task.Variables[engine.KeyAddSignHistory].(type) {
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

func asMustRow(t *testing.T, repo *memory.Repository, taskID int64) *model.ProcessTask {
	t.Helper()
	row, err := repo.FindTaskByID(context.Background(), taskID)
	if err != nil || row == nil {
		t.Fatalf("读任务行 %d: %v", taskID, err)
	}
	return row
}

// asStartToReview 发起并办结申请节点，返回停在 review 的（引擎、仓储、实例 id）
func asStartToReview(t *testing.T, reviewProps string) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	name := fmt.Sprintf("addsign_%d", asSeq.Add(1))
	eng, repo, defID := asHarness(t, name, asFlowJSON(name, asApplyProps, reviewProps))
	inst := asStart(t, eng, defID)
	for _, tk := range asSortedDoing(t, repo, inst.ID, "") {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant", asPlain(asApply)); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}
	return eng, repo, inst.ID
}

var asSeq = &counter{}

type counter struct{ n int }

func (c *counter) Add(v int) int { c.n += v; return c.n }

// ═══ 正向 ══════════════════════════════════════════════════════════════════

// 格 1（java test01_chainThenStop）：链式续办 A→B→C→不点人即流出；
// 血缘串成链、轮次递增、账本落在已完成行。
func TestAddSignChainThenStop(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if a == nil {
		t.Fatal("审批节点应有待办")
	}

	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB")); err != nil {
		t.Fatalf("A 加签给 B: %v", err)
	}
	b := asDoing(t, repo, iid, "review")
	if b == nil {
		t.Fatal("被加签人应在同一节点收到新待办")
	}
	if b.ParentTaskID == nil || *b.ParentTaskID != a.ID {
		t.Errorf("血缘 parent＝刚办结那条，实得 %v", b.ParentTaskID)
	}
	if got := asRound(b, "review"); got != 1 {
		t.Errorf("轮次应为 1，实得 %d", got)
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("令牌不得流出本节点")
	}
	if n := asDoingCount(repo, iid, "review"); n != 1 {
		t.Errorf("本节点只应有 1 条待办，实得 %d", n)
	}

	histA := asHistory(t, repo, a.ID)
	if len(histA) != 1 {
		t.Fatalf("已完成行应有 1 条账本，实得 %d", len(histA))
	}
	e0 := histA[0]
	if e0["fromActor"] != "userA" || e0["operator"] != "userA" {
		t.Errorf("账本 fromActor/operator 应都是办理人 userA，实得 %v/%v", e0["fromActor"], e0["operator"])
	}
	if got, ok := e0["toActors"].([]string); !ok || len(got) != 1 || got[0] != "userB" {
		t.Errorf("账本 toActors 应为 [userB]，实得 %#v", e0["toActors"])
	}
	if r, ok := e0["round"].(int); !ok || r != 1 {
		t.Errorf("账本 round 应为 1，实得 %#v", e0["round"])
	}
	ts, _ := e0["time"].(string)
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`).MatchString(ts) {
		t.Errorf("时间须是全生态格式串 yyyy-MM-dd HH:mm:ss，实得 %q", ts)
	}

	if _, err := eng.ExecuteProcessTask(context.Background(), b.ID, "userB", asAgreeOps("userC")); err != nil {
		t.Fatalf("B 加签给 C: %v", err)
	}
	c := asDoing(t, repo, iid, "review")
	if c == nil {
		t.Fatal("C 应有待办")
	}
	if got := asRound(c, "review"); got != 2 {
		t.Errorf("轮次应递增到 2，实得 %d", got)
	}
	if c.ParentTaskID == nil || *c.ParentTaskID != b.ID {
		t.Errorf("血缘应串成链，实得 %v", c.ParentTaskID)
	}

	// C 不再点人 → 停止条件成立，流出本节点
	if _, err := eng.ExecuteProcessTask(context.Background(), c.ID, "userC", asPlain(asAgree)); err != nil {
		t.Fatalf("C 正常办结: %v", err)
	}
	if asDoing(t, repo, iid, "review") != nil {
		t.Error("不点人后不得再有本节点待办")
	}
	fin := asDoing(t, repo, iid, "final")
	if fin == nil {
		t.Fatal("应流转到下一审批节点")
	}
	if n := asRowCount(t, repo, iid, "review"); n != 3 {
		t.Errorf("review 共三行，实得 %d", n)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), fin.ID, "userF", asPlain(asAgree)); err != nil {
		t.Fatalf("终审办结: %v", err)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst.State != model.InstanceStateDone {
		t.Errorf("实例应办结（20），实得 %d", inst.State)
	}
}

// 格 2（java test02_multiSelectWaitsSiblings）：数组两形同判据（spec 06 §2.11）——
// 一次点两人 ⇒ 同圈建两行，最后一位办结且没点人才放行。
func TestAddSignMultiSelectWaitsSiblings(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")

	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA",
		asAgreeOps([]interface{}{"userB", "userC"})); err != nil {
		t.Fatalf("一次点两人: %v", err)
	}
	if n := asDoingCount(repo, iid, "review"); n != 2 {
		t.Fatalf("同圈应建两行待办，实得 %d", n)
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Fatal("同圈还有兄弟待办时不得放行")
	}

	b := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), b.ID, asFirstActor(b), asPlain(asAgree)); err != nil {
		t.Fatalf("第一位办结: %v", err)
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("兄弟待办未完成时不得放行")
	}
	if n := asDoingCount(repo, iid, "review"); n != 1 {
		t.Errorf("应剩 1 条兄弟待办，实得 %d", n)
	}

	c := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), c.ID, asFirstActor(c), asPlain(asAgree)); err != nil {
		t.Fatalf("最后一位办结: %v", err)
	}
	if asDoing(t, repo, iid, "final") == nil {
		t.Fatal("最后一位办结且没点人才放行")
	}
	if n := asDoingCount(repo, iid, "final"); n != 1 {
		t.Errorf("放行只能发生一次，实得 %d 条后续待办", n)
	}
}

// 格 3（java test03）：加给已办过的人合法（A→B→A 乒乓），轮次继续计数。
func TestAddSignReAddPreviouslyHandledIsLegal(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB")); err != nil {
		t.Fatalf("A→B: %v", err)
	}
	b := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), b.ID, "userB", asAgreeOps("userA")); err != nil {
		t.Fatalf("B→A: %v", err)
	}
	back := asDoing(t, repo, iid, "review")
	if back == nil {
		t.Fatal("A 应再收到一条待办")
	}
	if !containsActor(back.ActorIDs, "userA") {
		t.Errorf("加给已办过的人应合法，实得参与者 %v", back.ActorIDs)
	}
	if got := asRound(back, "review"); got != 2 {
		t.Errorf("乒乓轮次应继续计到 2，实得 %d", got)
	}
}

// ═══ 负向：写入口报错且零写入 ═══════════════════════════════════════════════

// 格 4（java test04）：与 tf_nextNodeOperator 同传 ⇒ 报错，任务行/实例零改动。
func TestAddSignMutexWithNextNodeOperatorRejected(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	before := asRowCount(t, repo, iid, "review")

	args := asAgreeOps("userB")
	args[engine.KeyNextNodeOperator] = "userF"
	_, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", args)
	if err == nil || err.Error() != engine.MsgAddSignMutex {
		t.Fatalf("同传两键必须逐字报错 %q，实得 %v", engine.MsgAddSignMutex, err)
	}
	if row := asMustRow(t, repo, a.ID); !row.IsDoing() {
		t.Error("报错时任务行不得被办结")
	}
	if got := asRowCount(t, repo, iid, "review"); got != before {
		t.Errorf("报错时不得新增行：%d → %d", before, got)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst.State != model.InstanceStateDoing {
		t.Errorf("实例状态不得变化，实得 %d", inst.State)
	}
}

// 格 5（java test05）：节点未开开关却传键 ⇒ 报错（不得静默忽略），零写入。
func TestAddSignSwitchOffRejected(t *testing.T) {
	eng, repo, iid := asStartToReview(t, "") // review 不开 allowAddSign
	a := asDoing(t, repo, iid, "review")
	_, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB"))
	if err == nil || !strings.Contains(err.Error(), "未开启 allowAddSign") {
		t.Fatalf("开关没开却传键必须报错，实得 %v", err)
	}
	if !asMustRow(t, repo, a.ID).IsDoing() {
		t.Error("报错时任务行不得被办结")
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("报错时不得流出本节点")
	}
}

// 格 6（java test06）：加给自己 ⇒ 报错（死循环入口），零写入。
func TestAddSignSelfRejected(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	_, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userA"))
	if err == nil || err.Error() != engine.MsgAddSignSelf {
		t.Fatalf("加给自己必须逐字报错 %q，实得 %v", engine.MsgAddSignSelf, err)
	}
	if !asMustRow(t, repo, a.ID).IsDoing() {
		t.Error("报错时任务行不得被办结")
	}
	if n := asDoingCount(repo, iid, "review"); n != 1 {
		t.Errorf("报错时不得新增待办，实得 %d", n)
	}
}

// 格 7（java test07）：超上限 ⇒ 报错不强制放行（放行＝伪造"审批完成"），零写入；
// 但上一轮那位不点人可以正常流出。
func TestAddSignMaxRoundsRejected(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn+`,"addSignMaxRounds":1`)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB")); err != nil {
		t.Fatalf("第 1 轮应合法: %v", err)
	}
	b := asDoing(t, repo, iid, "review")
	_, err := eng.ExecuteProcessTask(context.Background(), b.ID, "userB", asAgreeOps("userC"))
	if err == nil || !strings.Contains(err.Error(), "已达上限 1") {
		t.Fatalf("超过 addSignMaxRounds 必须报错，实得 %v", err)
	}
	if !asMustRow(t, repo, b.ID).IsDoing() {
		t.Error("越界那次不得办结任务行")
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("越界那次不得流出本节点")
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), b.ID, "userB", asPlain(asAgree)); err != nil {
		t.Fatalf("不点人应能正常流出: %v", err)
	}
	if asDoing(t, repo, iid, "final") == nil {
		t.Error("不点人那位办结后应放行")
	}
}

// ═══ 解析期硬校验 ═══════════════════════════════════════════════════════════

// 格 8（java test08）：会签节点（performType=1）配 allowAddSign ⇒ 解析期报错。
func TestAddSignCountersignNodeRejectedAtParse(t *testing.T) {
	asAssertParseFails(t, asApplyProps, asOn+`,"performType":1,"countersignType":"PARALLEL"`, "会签节点")
}

// 格 9（java test09）：首任务节点配 allowAddSign ⇒ 解析期报错。
func TestAddSignFirstTaskNodeRejectedAtParse(t *testing.T) {
	asAssertParseFails(t, asApplyProps+","+asOn, asOn, "首任务节点")
}

// 格 10（java test10）：两开关非法值 ⇒ 解析期报错，不得静默当缺省。
func TestAddSignInvalidConfigRejectedAtParse(t *testing.T) {
	asAssertParseFails(t, asApplyProps, `"allowAddSign":"yes"`, "allowAddSign 配置非法")
	asAssertParseFails(t, asApplyProps, asOn+`,"addSignMaxRounds":-1`, "不得为负")
	asAssertParseFails(t, asApplyProps, asOn+`,"addSignMaxRounds":"1.5"`, "必须是整数")
}

// asAssertParseFails 解析期判点＝发起腿（定义 JSON → FlowModel 那一跳，go 的等价解析期）。
// 除发起外，执行公共序言与 loadFlowForTask 两个落点也各自带同一枚校验（见 add_sign.go 头注），
// 所以配了脏键的定义在任何入口都过不去。
func asAssertParseFails(t *testing.T, applyProps, reviewProps, wantFragment string) {
	t.Helper()
	name := fmt.Sprintf("addsign_bad_%d", asSeq.Add(1))
	eng, _, defID := asHarness(t, name, asFlowJSON(name, applyProps, reviewProps))
	_, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", map[string]interface{}{})
	if err == nil {
		t.Fatalf("应报解析期配置错误（%s），却发起了", wantFragment)
	}
	if !strings.Contains(err.Error(), wantFragment) {
		t.Fatalf("报错应含 %q，实得 %q", wantFragment, err.Error())
	}
}

// ═══ 废弃未完成加签任务 ═════════════════════════════════════════════════════

// 格 11（java test11）：不同意(2) 结束实例，本节点未完成的兄弟加签任务全部废弃，不留孤儿待办。
func TestAddSignRejectAbandonsSiblings(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA",
		asAgreeOps([]interface{}{"userB", "userC"})); err != nil {
		t.Fatalf("一次点两人: %v", err)
	}
	if n := asDoingCount(repo, iid, "review"); n != 2 {
		t.Fatalf("应有两条兄弟待办，实得 %d", n)
	}
	b := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteAndJumpToEnd(context.Background(), b.ID, asFirstActor(b), asPlain(asReject)); err != nil {
		t.Fatalf("拒绝: %v", err)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst.State != model.InstanceStateReject {
		t.Errorf("实例应已拒绝（45），实得 %d", inst.State)
	}
	if n := asDoingCount(repo, iid, "review"); n != 0 {
		t.Errorf("未完成的加签兄弟不得留待办，实得 %d", n)
	}
	rows, _ := repo.FindHistoryTasks(context.Background(), iid)
	abandoned := 0
	for _, r := range rows {
		if r.TaskName == "review" && r.TaskState == model.TaskStateAbandoned {
			abandoned++
		}
	}
	if abandoned != 1 {
		t.Errorf("被废弃的那条应留废弃态（99），实得 %d 条", abandoned)
	}
}

// 格 12（java test12）：退回发起人(6) ⇒ 生成申请待办，本节点未完成加签任务废弃。
func TestAddSignReturnToApplicantAbandonsSiblings(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA",
		asAgreeOps([]interface{}{"userB", "userC"})); err != nil {
		t.Fatalf("一次点两人: %v", err)
	}
	b := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteAndJumpToFirstTaskNode(context.Background(), b.ID, asFirstActor(b), asPlain(asReturn)); err != nil {
		t.Fatalf("退回发起人: %v", err)
	}
	if asDoing(t, repo, iid, "apply") == nil {
		t.Error("应生成申请待办")
	}
	if n := asDoingCount(repo, iid, "review"); n != 0 {
		t.Errorf("本节点未完成加签任务应被废弃，实得 %d 条待办", n)
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("退回发起人不得点亮后续节点")
	}
}

// ═══ 语义边界补格（spec 04 边界表第一行，基准 java 靠既有守卫零改动兑现、未单列格子） ═══

// 加签链上「退回上一步」(submitType=3) 必须是**禁**档：加签那条行的血缘 parent 与自己同节点，
// 模型里没有自环边 ⇒ 既有 canRejected 守卫不过 ⇒ 报"无法驳回至上一步处理"那一档（与会签同档），
// 要退用退回发起人(6)或跳转(4)。本格钉住"禁"这一档不是靠巧合，而是守卫真的拦住了。
//
// ⚠️ 本格**只断言报错档，不断言零写入**——这一档的守卫位在 prepareExecuteTask 之后
// （engine_impl.go executeAndJumpTaskInner：先完成当前任务、再解析血缘），所以报错时那条
// 加签待办已被办结、本节点没有新行。这是**本栈既有的形状、不是加签带进来的**：串行会签推进位
// 的行血缘 parent 也在同节点，今天对它按 submitType=3 就是同一结果（java 同序，
// ProcessInstance.rollback 的守卫也在 completeTask 之后）。要把"报错且零写入"抬到这一档，
// 得把血缘校验前置到状态变更之前——那是回退腿自己的 issue，不塞进本轮移植（已按当轮回报记给 owner）。
func TestAddSignRollbackOnChainIsForbidden(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB")); err != nil {
		t.Fatalf("A→B: %v", err)
	}
	b := asDoing(t, repo, iid, "review")
	_, err := eng.ExecuteAndJumpTask(context.Background(), b.ID, asFirstActor(b), asPlain(int(model.SubmitTypeRollback)), "")
	if err == nil {
		t.Fatal("加签链上退回上一步必须被守卫拦下（同节点血缘、模型无自环边）")
	}
	if !strings.Contains(err.Error(), "无法驳回至上一步处理") {
		t.Errorf("应落到既有 20010008 那一档文案，实得 %q", err.Error())
	}
	// 守卫不过 ⇒ 绝不给本节点点亮新行（同节点续办不走"退回"这条路）
	if asDoing(t, repo, iid, "review") != nil {
		t.Error("被拒的退回不得在本节点再建一条待办")
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Error("被拒的退回不得流出本节点")
	}
}

// ═══ 回归与并发 ═════════════════════════════════════════════════════════════

// 格 13（java test13）：不开开关的节点行为逐字不变——同意即流出，账本一格都不写。
func TestAddSignPlainNodeUnchanged(t *testing.T) {
	eng, repo, iid := asStartToReview(t, "")
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asPlain(asAgree)); err != nil {
		t.Fatalf("普通节点办结: %v", err)
	}
	if asDoing(t, repo, iid, "final") == nil {
		t.Fatal("同意即流出")
	}
	if n := asRowCount(t, repo, iid, "review"); n != 1 {
		t.Errorf("不得多出任何本节点行，实得 %d", n)
	}
	if h := asHistory(t, repo, a.ID); len(h) != 0 {
		t.Errorf("不开开关不得写账本，实得 %d 条", len(h))
	}
}

// 格 14（java test14）：两个同圈加签任务并发办结 ⇒ 只放行一次（后续节点恰好一条待办）。
// 同实例互斥照 reapply 轮的 instLocks，「复验本节点等待 → 建后继任务」在同一临界区里。
func TestAddSignConcurrentFinishReleasesOnce(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA",
		asAgreeOps([]interface{}{"userB", "userC"})); err != nil {
		t.Fatalf("一次点两人: %v", err)
	}
	sibs := asSortedDoing(t, repo, iid, "review")
	if len(sibs) != 2 {
		t.Fatalf("应有两条同圈待办，实得 %d", len(sibs))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := make([]error, 0, 2)
	for _, s := range sibs {
		wg.Add(1)
		go func(taskID int64, actor string) {
			defer wg.Done()
			_, err := eng.ExecuteProcessTask(context.Background(), taskID, actor, asPlain(asAgree))
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(s.ID, asFirstActor(s))
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("并发办结不应报错: %v", errs)
	}
	if n := asDoingCount(repo, iid, "final"); n != 1 {
		t.Errorf("后续节点只应有一条待办（放行一次），实得 %d", n)
	}
	if n := asDoingCount(repo, iid, "review"); n != 0 {
		t.Errorf("本节点不应留待办，实得 %d", n)
	}
}

// 格 15（java test15，根因守卫）：控制类 tf_* 参数不得进实例变量（跨请求复活的病根），
// 但任务行留痕必须完整——两者是同一枚判据的两面。
func TestAddSignControlKeysNeverEnterInstanceVariables(t *testing.T) {
	eng, repo, iid := asStartToReview(t, asOn)
	a := asDoing(t, repo, iid, "review")
	args := asAgreeOps("userB")
	args["tf_approvalComment"] = "请 B 复核"
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", args); err != nil {
		t.Fatalf("A 加签给 B: %v", err)
	}

	inst, _ := repo.FindInstanceByID(context.Background(), iid)
	if v, ok := inst.Variables[engine.KeyAddSignOperators]; ok {
		t.Errorf("tf_addSignOperators 不得进实例变量（跨请求复活），实得 %v", v)
	}
	if v, ok := inst.Variables["tf_approvalComment"]; ok {
		t.Errorf("tf_approvalComment 不得进实例变量，实得 %v", v)
	}
	for k := range inst.Variables {
		if strings.HasPrefix(k, "tf_") {
			t.Errorf("实例变量里不应有任何 tf_ 前缀键，实得 %q", k)
		}
	}

	// 留痕仍在任务行上（修根因不得把审计信息一起改掉）
	row := asMustRow(t, repo, a.ID)
	if got, _ := row.Variables["tf_approvalComment"].(string); got != "请 B 复核" {
		t.Errorf("任务行留痕应完整，tf_approvalComment 实得 %q", got)
	}
	if got, _ := row.Variables[engine.KeyAddSignOperators].(string); got != "userB" {
		t.Errorf("任务行留痕应完整，tf_addSignOperators 实得 %q", got)
	}
	if h := asHistory(t, repo, a.ID); len(h) != 1 {
		t.Errorf("任务行应有 1 条账本，实得 %d", len(h))
	}
}

// ═══ 共享夹具 ═══════════════════════════════════════════════════════════════

// 格 16（java test16）：共享夹具 flows/19-add-sign-after.json 必须真被本栈消费——
// 七语言仓 flows/ 是 java 编辑源的入库副本，各语言移植后加签读的就是这一份。
func TestAddSignSharedFixtureChainRuns(t *testing.T) {
	content := asReadSharedFlow(t, "19-add-sign-after.json")
	name := "addsign-shared-19"
	eng, repo, defID := asHarness(t, name, content)
	inst := asStart(t, eng, defID)
	iid := inst.ID
	for _, tk := range asSortedDoing(t, repo, iid, "") {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant", asPlain(asApply)); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}

	a := asDoing(t, repo, iid, "review")
	if a == nil {
		t.Fatal("共享夹具的申请节点办结后应到审批节点")
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), a.ID, "userA", asAgreeOps("userB,userC")); err != nil {
		t.Fatalf("逗号串两形: %v", err)
	}
	if n := asDoingCount(repo, iid, "review"); n != 2 {
		t.Fatalf("逗号串两形应建两行，实得 %d", n)
	}
	if asDoing(t, repo, iid, "final") != nil {
		t.Fatal("同圈兄弟未办结不得放行")
	}

	// 先取快照再逐条办结：办结会改写仓储里的进行中集合，边遍历边办是并发修改
	sibs := asSortedDoing(t, repo, iid, "review")
	for _, s := range sibs {
		if _, err := eng.ExecuteProcessTask(context.Background(), s.ID, asFirstActor(s), asPlain(asAgree)); err != nil {
			t.Fatalf("办结同圈兄弟 %d: %v", s.ID, err)
		}
	}
	fin := asDoing(t, repo, iid, "final")
	if fin == nil {
		t.Fatal("同圈兄弟全部办结后应放行")
	}
	if n := asRowCount(t, repo, iid, "final"); n != 1 {
		t.Errorf("放行只能发生一次，实得 %d 行", n)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), fin.ID, "userF", asPlain(asAgree)); err != nil {
		t.Fatalf("终审办结: %v", err)
	}
	inst2, _ := repo.FindInstanceByID(context.Background(), iid)
	if inst2.State != model.InstanceStateDone {
		t.Errorf("实例应办结（20），实得 %d", inst2.State)
	}
}

func asReadSharedFlow(t *testing.T, file string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(flowsutil.Dir(), file))
	if err != nil {
		t.Fatalf("读共享夹具 %s: %v", file, err)
	}
	return string(raw)
}

func containsActor(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
