// 「动态协办（aidant）」验收（issues/253，go 腿）——移植 jeeflow-java 参考实现
// `AidantTest`（34 例）的用例形状：派生形状（task_type=1／parent＝主办行／同节点同名／
// performType=NORMAL／expire_time 同尺／委托并入／码 3 既有腿）、推进闸（协办行办结不驱动，
// execute／jump／reject／退发起人四入口同闸）、等待闸两档（waitCompletion=true 逐字文案＋
// 零写入／缺省 false 随时推进＋离开节点清场记账）、两跳账本存活、detail 投影形状、
// 协办行能力边界（可加/摘人、可被转办、禁递归）、负向六文案＋越权＋非 DOING、
// 回归（会签票齐即推进不被协办挡／与征询并存两闸各数／撤回清场／transfer·surrogate·addCandidate 不变）。
//
// 夹具内联 JSON（不碰共享 flows/ 目录，与 collect_247_test.go／recall 同款纪律）；
// 五节点链与部署/读数助手复用同包 collect_247_test.go 的 dcFlow／dcDeploy／dcDoing 等
// ——既有测试期望值一字未改。
package facade_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// adFixture 一套协办验收夹具：门面＋内存仓＋扩展仓＋引擎＋事件码捕获。
type adFixture struct {
	f      *facade.Facade
	repo   *memory.Repository
	ext    *memory.ExtRepository
	eng    *engine.EngineImpl
	events []int
	iid    int64
}

// adSetup 部署五节点链（start→apply→review→final→end）并走 startAndExecute
// （自动办结申请节点），返回停在 review 的夹具。reviewProps 控制 review 的额外 properties。
func adSetup(t *testing.T, reviewProps string) *adFixture {
	t.Helper()
	repo := memory.New()
	ext := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	fx := &adFixture{repo: repo, ext: ext, eng: eng}
	eng.SetExtensions(&engine.Extensions{Listeners: []engine.ProcessEventListener{
		func(_ context.Context, evt engine.ProcessEvent) { fx.events = append(fx.events, int(evt.Type)) },
	}})
	fx.f = facade.New(eng, repo, ext)
	name := fmt.Sprintf("aidant_%d", dcSeqAdd(1))
	defID := dcDeploy(t, fx.f, name, dcFlow(name, reviewProps))
	fx.startDefine(t, defID)
	return fx
}

// adSetupFlow 用自定义流程 JSON 建夹具（会签链等）。
func adSetupFlow(t *testing.T, content string) *adFixture {
	t.Helper()
	repo := memory.New()
	ext := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	fx := &adFixture{repo: repo, ext: ext, eng: eng}
	eng.SetExtensions(&engine.Extensions{Listeners: []engine.ProcessEventListener{
		func(_ context.Context, evt engine.ProcessEvent) { fx.events = append(fx.events, int(evt.Type)) },
	}})
	fx.f = facade.New(eng, repo, ext)
	fx.startDefine(t, dcDeploy(t, fx.f, fmt.Sprintf("aidant_cs_%d", dcSeqAdd(1)), content))
	return fx
}

func (fx *adFixture) startDefine(t *testing.T, defID int64) {
	t.Helper()
	r := fx.f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": fmt.Sprintf("%d", defID), "operator": "applicant",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("发起失败: %v", r)
	}
	idStr, _ := dcData(r)["processInstanceId"].(string)
	var iid int64
	fmt.Sscanf(idStr, "%d", &iid)
	if iid == 0 {
		t.Fatalf("startAndExecute 未返回实例 id: %v", r)
	}
	fx.iid = iid
}

// adCsFlow 并行会签链：start→apply→cs（userA,userB）→end（照 java countersignChain）。
func adCsFlow(name string) string {
	return `{"name":"` + name + `","displayName":"协办×会签夹具","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","x":0,"y":0,"properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","x":0,"y":0,"properties":{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"},"text":{"value":"发起申请"}},` +
		`{"id":"cs","type":"snaker:task","x":0,"y":0,"properties":{"form":"form-c","taskType":0,"performType":1,"assignee":"userA,userB","countersignType":"PARALLEL"},"text":{"value":"会签审批"}},` +
		`{"id":"end","type":"snaker:end","x":0,"y":0,"properties":{},"text":{"value":"结束"}}` +
		`],"edges":[` +
		`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e2","sourceNodeId":"apply","targetNodeId":"cs","properties":{}},` +
		`{"id":"e3","sourceNodeId":"cs","targetNodeId":"end","properties":{}}]}`
}

// ─── 驱动助手 ─────────────────────────────────────────────────────────────────

// aidant 经门面派生协办。actorIds 传 nil ⇒ 不带该键；wait 传 nil ⇒ 不带该键。
func (fx *adFixture) aidant(host *model.ProcessTask, operator string, actorIds interface{},
	reason string, wait interface{}) map[string]interface{} {
	a := map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID),
		"operator":      operator,
	}
	if actorIds != nil {
		a["actorIds"] = actorIds
	}
	if reason != "" {
		a["reason"] = reason
	}
	if wait != nil {
		a["waitCompletion"] = wait
	}
	return fx.f.Flow("processTask/aidant", a)
}

func (fx *adFixture) exec(row *model.ProcessTask, operator string, submitType int) map[string]interface{} {
	return fx.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", row.ID),
		"operator":      operator,
		"submitType":    submitType,
	})
}

func (fx *adFixture) task(id int64) *model.ProcessTask {
	row, err := fx.repo.FindTaskByID(context.Background(), id)
	if err != nil || row == nil {
		return nil
	}
	return row
}

func (fx *adFixture) allTasks() []*model.ProcessTask {
	rows, _ := fx.repo.FindHistoryTasks(context.Background(), fx.iid)
	return rows
}

// rows 协办行（经引擎判据读回的持久值）。**按 id 升序**＝建单顺序：本栈仓储
// FindHistoryTasks 遍历 map，行序随机，不排序会让"rows[0] 是 userX"这类断言随运行漂。
func (fx *adFixture) rows(host *model.ProcessTask) []*model.ProcessTask {
	out := engine.AidantRows(fx.allTasks(), fx.task(host.ID))
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (fx *adFixture) ledger(host *model.ProcessTask) []map[string]interface{} {
	return engine.AidantHistoryOf(fx.task(host.ID))
}

func (fx *adFixture) state() int {
	inst, _ := fx.repo.FindInstanceByID(context.Background(), fx.iid)
	if inst == nil {
		return -1
	}
	return int(inst.State)
}

func (fx *adFixture) detailExt(id int64) map[string]interface{} {
	r := fx.f.Flow("processTask/detail", map[string]interface{}{"id": fmt.Sprintf("%d", id)})
	if code, _ := r["code"].(int); code != 0 {
		return nil
	}
	ext, _ := dcData(r)["ext"].(map[string]interface{})
	return ext
}

func adMsg(t *testing.T, resp map[string]interface{}, want string) {
	t.Helper()
	if code, _ := resp["code"].(int); code != 99999999 {
		t.Fatalf("应失败 code=99999999，实得 %v", resp)
	}
	if msg, _ := resp["msg"].(string); msg != want {
		t.Fatalf("文案应逐字 %q，实得 %q", want, msg)
	}
}

func adOK(t *testing.T, resp map[string]interface{}) {
	t.Helper()
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("应成功 code=0，实得 %v", resp)
	}
}

// adInt 数值读数归一（int/int64/float64/model.TaskState 经 JSON 往返形态不一）。
func adInt(v interface{}) int {
	n, _ := strconv.Atoi(fmt.Sprintf("%v", v))
	return n
}

func adActors(v interface{}) []string {
	var out []string
	switch a := v.(type) {
	case []string:
		out = append(out, a...)
	case []interface{}:
		for _, x := range a {
			out = append(out, fmt.Sprintf("%v", x))
		}
	}
	return out
}

// adMaps 对象数组读数归一：内存直传是 []map[string]interface{}，经门面/JSON 往返是
// []interface{}——两形都收（与本仓账本读取腿 readAddSignHistory 同款）。
func adMaps(v interface{}) []map[string]interface{} {
	switch a := v.(type) {
	case []map[string]interface{}:
		return a
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(a))
		for _, x := range a {
			if m, ok := x.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// ═══ 正向：派生形状 ═══════════════════════════════════════════════════════════

// 派生：每人一行、task_type=1、parent＝主办行、同节点同名、performType=NORMAL、
// 主办保留 DOING、账本六键、只走既有码 3。
func TestAidantDeriveShapesSecondaryRowsAndKeepsHostDoing(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.events = nil

	resp := fx.aidant(host, "userA", []string{"userX", "userY"}, "需两部门协办", nil)
	adOK(t, resp)
	if resp["data"] != nil {
		t.Errorf("成功 data → null（spec 06），实得 %v", resp["data"])
	}

	hostRow := fx.task(host.ID)
	if !hostRow.IsDoing() {
		t.Errorf("主办行应保留 DOING，实得 %d", hostRow.TaskState)
	}
	if strings.Join(hostRow.ActorIDs, ",") != "userA" {
		t.Errorf("主办行名册不动，实得 %v", hostRow.ActorIDs)
	}
	if hostRow.ActorID != "" {
		t.Errorf("严禁覆写任务 actor_id 列（转办那条 ⚠️ 同尺），实得 %q", hostRow.ActorID)
	}

	rows := fx.rows(host)
	if len(rows) != 2 {
		t.Fatalf("多人每人一行，应 2 实得 %d", len(rows))
	}
	var actors []string
	for _, r := range rows {
		if !r.IsDoing() {
			t.Errorf("协办行应 DOING，实得 %d", r.TaskState)
		}
		if r.TaskType != int(model.TaskTypeSecondary) {
			t.Errorf("task_type 应=1，实得 %d", r.TaskType)
		}
		if r.ParentTaskID == nil || *r.ParentTaskID != host.ID {
			t.Errorf("parent 应＝主办行 id，实得 %v", r.ParentTaskID)
		}
		if r.TaskName != host.TaskName || r.DisplayName != host.DisplayName {
			t.Errorf("应同节点同名，实得 %s/%s", r.TaskName, r.DisplayName)
		}
		if r.PerformType != int(model.PerformTypeNormal) {
			t.Errorf("performType 应=NORMAL，实得 %d", r.PerformType)
		}
		if r.Variables[engine.KeyAidantReason] != "需两部门协办" {
			t.Errorf("行戳 tf_aidantReason 缺失: %v", r.Variables)
		}
		if _, ok := r.Variables["tf_approvalComment"]; ok {
			t.Errorf("协办行不占 tf_approvalComment")
		}
		if r.ActorID != "" {
			t.Errorf("进行中任务 actor_id 列恒无值，实得 %q", r.ActorID)
		}
		actors = append(actors, r.ActorIDs[0])
		ra, _ := fx.repo.FindTaskActors(context.Background(), r.ID)
		if len(ra) != 1 {
			t.Errorf("参与者表应真落库 1 人，实得 %v", ra)
		}
	}
	if strings.Join(actors, ",") != "userX,userY" {
		t.Errorf("协办人顺序应为 userX,userY，实得 %v", actors)
	}

	lg := fx.ledger(host)
	if len(lg) != 1 {
		t.Fatalf("账本应 1 条，实得 %d", len(lg))
	}
	e0 := lg[0]
	for _, k := range []string{"round", "fromActor", "toActors", "reason", "time", "operator"} {
		if _, ok := e0[k]; !ok {
			t.Errorf("账本六键缺 %s: %v", k, e0)
		}
	}
	if len(e0) != 6 {
		t.Errorf("账本条目应恰六键，实得 %d: %v", len(e0), e0)
	}
	if adInt(e0["round"]) != 1 || e0["fromActor"] != "userA" || e0["operator"] != "userA" {
		t.Errorf("账本 round/fromActor/operator 不符: %v", e0)
	}
	if strings.Join(adActors(e0["toActors"]), ",") != "userX,userY" {
		t.Errorf("账本 toActors 不符: %v", e0["toActors"])
	}
	if e0["reason"] != "需两部门协办" {
		t.Errorf("账本 reason 不符: %v", e0["reason"])
	}
	if tm, _ := e0["time"].(string); !strings.HasPrefix(tm, "20") || len(tm) != 19 {
		t.Errorf("time 一律 yyyy-MM-dd HH:mm:ss，实得 %v", e0["time"])
	}
	if strings.Join(adInts(fx.events), ",") != "3,3" {
		t.Errorf("只走既有码 3（逐协办行一次），实得 %v", fx.events)
	}
}

func adInts(in []int) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strconv.Itoa(v))
	}
	return out
}

// reason 选填：空串兜底进账本，协办行不盖空戳。
func TestAidantReasonOptionalFallsBackToEmptyString(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.aidant(host, "userA", "userX", "", nil))
	if fx.ledger(host)[0]["reason"] != "" {
		t.Errorf("reason 应空串兜底，实得 %v", fx.ledger(host)[0]["reason"])
	}
	if _, ok := fx.rows(host)[0].Variables[engine.KeyAidantReason]; ok {
		t.Errorf("reason 空时协办行不盖戳")
	}
	if _, ok := fx.task(host.ID).Variables["tf_approvalComment"]; ok {
		t.Errorf("主办行也不占审批意见")
	}
}

// actorIds 逗号串与数组两形同归一（§2.11 单点）；空白元素丢弃。
func TestAidantActorIdsAcceptsCommaStringAndArray(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.aidant(host, "userA", " userX , userY ,", "", nil))
	if len(fx.rows(host)) != 2 {
		t.Fatalf("逗号串应归一成 2 人，实得 %d", len(fx.rows(host)))
	}
}

// expire_time 同尺重算（节点表达式），委托照既有腿并入协办行名册。
func TestAidantExpireTimeRecomputedAndSurrogateMerged(t *testing.T) {
	fx := adSetup(t, `"expireTime":"2d"`)
	st := time.Now().Add(-time.Hour)
	et := time.Now().Add(time.Hour)
	if err := fx.ext.SaveSurrogate(context.Background(), &model.ProcessSurrogate{
		ID: 1, Operator: "userX", Surrogate: "userS", ProcessName: "",
		StartTime: &st, EndTime: &et, Enabled: 1,
	}); err != nil {
		t.Fatalf("存委托: %v", err)
	}
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.aidant(host, "userA", "userX", "", nil))
	child := fx.rows(host)[0]
	if child.ExpireTime == nil {
		t.Fatalf("expire_time 应按节点表达式同尺重算")
	}
	if !child.ExpireTime.After(time.Now().Add(47 * time.Hour)) {
		t.Errorf("expire_time 应≈+2d，实得 %v", child.ExpireTime)
	}
	joined := strings.Join(child.ActorIDs, ",")
	if !strings.Contains(joined, "userX") || !strings.Contains(joined, "userS") {
		t.Errorf("委托并入不摘原人，实得 %v", child.ActorIDs)
	}
	ra, _ := fx.repo.FindTaskActors(context.Background(), child.ID)
	if len(ra) != 2 {
		t.Errorf("落库参与者表也真带上，应 2 实得 %v", ra)
	}
}

// ═══ 正向：推进闸（四入口同闸） ═══════════════════════════════════════════════

// 协办行办结不产生执行对象、不跑输出变迁；主办行仍 DOING；只 fire 既有码 5。
func TestAidantCompletionDoesNotDrive(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	child := fx.rows(host)[0]
	fx.events = nil

	created, err := fx.eng.ExecuteProcessTask(context.Background(), child.ID, "userX",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeAgree)})
	if err != nil {
		t.Fatalf("协办办结应成功: %v", err)
	}
	if len(created) != 0 {
		t.Errorf("协办办结不产生执行对象（无新任务），实得 %d", len(created))
	}
	if !fx.task(child.ID).IsFinished() {
		t.Errorf("协办行应已办结")
	}
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("主办行保留 DOING")
	}
	if dcDoing(t, fx.repo, fx.iid, "final") != nil {
		t.Errorf("不跑输出变迁：下游节点不得有待办")
	}
	if fx.state() != int(model.InstanceStateDoing) {
		t.Errorf("实例应仍 DOING，实得 %d", fx.state())
	}
	if strings.Join(adInts(fx.events), ",") != "5" {
		t.Errorf("只走既有码 5，不新增事件码，实得 %v", fx.events)
	}
}

// 四入口同闸：JUMP／ROLLBACK（executeAndJumpTask）对协办行同样不驱动。
func TestAidantJumpPathAlsoGated(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", []string{"userX", "userY"}, "", nil)
	rows := fx.rows(host)

	// JUMP 路（门面 submitType=4）
	adOK(t, fx.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", rows[0].ID),
		"operator":      "userX",
		"submitType":    int(model.SubmitTypeJump),
		"taskName":      "final",
	}))
	if !fx.task(rows[0].ID).IsFinished() {
		t.Errorf("JUMP 路协办行应已办结")
	}
	if dcDoing(t, fx.repo, fx.iid, "final") != nil {
		t.Errorf("跳转路同闸：不得凭空建出 final 待办")
	}

	// ROLLBACK 路（引擎直调，targetTaskName 空）
	created, err := fx.eng.ExecuteAndJumpTask(context.Background(), rows[1].ID, "userY",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeRollback)}, "")
	if err != nil {
		t.Fatalf("ROLLBACK 路应成功: %v", err)
	}
	if len(created) != 0 {
		t.Errorf("ROLLBACK 路不产生执行对象，实得 %d", len(created))
	}
	if !fx.task(rows[1].ID).IsFinished() {
		t.Errorf("ROLLBACK 路协办行应已办结")
	}
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("主办行仍在")
	}
	if dcDoingCount(t, fx.repo, fx.iid, "review") != 1 {
		t.Errorf("review 应只剩主办一行")
	}
}

// 协办行拒绝／退回发起人同样不驱动（任何 submitType 都只办结自己那一行）。
func TestAidantRejectAndReturnDoNotDrive(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", []string{"userX", "userY"}, "", nil)
	rows := fx.rows(host)

	adOK(t, fx.exec(rows[0], "userX", int(model.SubmitTypeReject)))
	if fx.state() != int(model.InstanceStateDoing) {
		t.Errorf("拒绝不得结束整单，实得 %d", fx.state())
	}
	adOK(t, fx.exec(rows[1], "userY", int(model.SubmitTypeRollbackToOperator)))
	if fx.state() != int(model.InstanceStateDoing) {
		t.Errorf("退回发起人不得结束整单，实得 %d", fx.state())
	}
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("主办行仍 DOING")
	}
	if dcDoing(t, fx.repo, fx.iid, "apply") != nil {
		t.Errorf("协办行退回不得复活申请节点")
	}
}

// 主办再办结照常驱动（协办已办结）。
func TestAidantHostCompletionAfterAidantDrives(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	fx.exec(fx.rows(host)[0], "userX", int(model.SubmitTypeAgree))
	adOK(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("主办办结才推进")
	}
	if !fx.task(host.ID).IsFinished() {
		t.Errorf("主办行应已办结")
	}
}

// ═══ 正向／负向：等待闸两档 ═══════════════════════════════════════════════════

// waitCompletion=true：未齐 ⇒ 逐字文案＋零写入；齐了 ⇒ 放行推进。
func TestAidantWaitTrueBlocksHostUntilDone(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.aidant(host, "userA", []string{"userX", "userY"}, "", true))
	rows := fx.rows(host)
	ledgerBefore := len(fx.ledger(host))

	adMsg(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)),
		"节点[review]协办未办结（已办结 0/2），不能办理")
	// 零写入：主办仍 DOING、无下游待办、账本不增、协办行不动
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("零写入：主办仍 DOING")
	}
	if dcDoing(t, fx.repo, fx.iid, "final") != nil {
		t.Errorf("零写入：无下游待办")
	}
	if len(fx.ledger(host)) != ledgerBefore {
		t.Errorf("零写入：账本不增")
	}
	for _, r := range fx.rows(host) {
		if !r.IsDoing() {
			t.Errorf("零写入：协办行不动")
		}
	}

	fx.exec(rows[0], "userX", int(model.SubmitTypeAgree))
	adMsg(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)),
		"节点[review]协办未办结（已办结 1/2），不能办理")

	fx.exec(rows[1], "userY", int(model.SubmitTypeAgree))
	adOK(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("全办结后应放行推进")
	}
}

// 等待闸挡全部 submitType（含拒绝／退回），不只挡同意。
func TestAidantWaitGateBlocksEverySubmitType(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", true)
	for _, st := range []int{int(model.SubmitTypeAgree), int(model.SubmitTypeReject),
		int(model.SubmitTypeRollbackToOperator)} {
		adMsg(t, fx.exec(host, "userA", st), "节点[review]协办未办结（已办结 0/1），不能办理")
		if !fx.task(host.ID).IsDoing() {
			t.Errorf("submitType=%d 被拦后主办仍 DOING", st)
		}
	}
	if fx.state() != int(model.InstanceStateDoing) {
		t.Errorf("实例应仍 DOING")
	}
}

// waitCompletion 缺省 false：主办可随时推进，未办结协办行随废弃腿清场＋账本记废弃。
func TestAidantWaitDefaultFalseCleansPendingOnLeave(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", []string{"userX", "userY"}, "", nil)
	rows := fx.rows(host)
	fx.exec(rows[0], "userX", int(model.SubmitTypeAgree))

	adOK(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("缺省 false 主办可随时推进")
	}
	if fx.task(rows[1].ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("未办结协办行清场为废弃(99)，实得 %d", fx.task(rows[1].ID).TaskState)
	}
	if fx.task(rows[0].ID).TaskState != model.TaskStateDone {
		t.Errorf("已办结那条不改写，实得 %d", fx.task(rows[0].ID).TaskState)
	}
	lg := fx.ledger(host)
	if len(lg) != 2 {
		t.Fatalf("账本应 2 条（派生＋清场），实得 %d", len(lg))
	}
	last := lg[1]
	if last["abandoned"] != true {
		t.Errorf("清场条目应带 abandoned=true，实得 %v", last)
	}
	if strings.Join(adActors(last["toActors"]), ",") != "userY" {
		t.Errorf("清场 toActors＝被废弃的人，实得 %v", last["toActors"])
	}
	if adInt(last["round"]) != 1 {
		t.Errorf("清场不消耗轮次，实得 %v", last["round"])
	}
	if dcDoingCount(t, fx.repo, fx.iid, "review") != 0 {
		t.Errorf("不留孤儿待办")
	}
}

// waitCompletion=false 显式传值同缺省；字符串 "true" 两形也认；非法值按缺省 false。
func TestAidantWaitCompletionStringAndInvalidForms(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.aidant(host, "userA", "userX", "", "true"))
	adMsg(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)),
		"节点[review]协办未办结（已办结 0/1），不能办理")

	fx2 := adSetup(t, "")
	h2 := dcDoing(t, fx2.repo, fx2.iid, "review")
	adOK(t, fx2.aidant(h2, "userA", "userX", "", "不是布尔"))
	adOK(t, fx2.exec(h2, "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx2.repo, fx2.iid, "final") == nil {
		t.Errorf("非法值按缺省 false，不新造文案")
	}
}

// 拒绝／退回发起人离开节点：协办行同样清场（废弃腿覆盖）。
func TestAidantRejectAndReturnToOperatorClean(t *testing.T) {
	fx := adSetup(t, "")
	h1 := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(h1, "userA", "userX", "", nil)
	c1 := fx.rows(h1)[0]
	adOK(t, fx.exec(h1, "userA", int(model.SubmitTypeReject)))
	if fx.task(c1.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("拒绝离开节点应清场协办行")
	}
	if fx.ledger(h1)[1]["abandoned"] != true {
		t.Errorf("拒绝清场应记账")
	}

	fx2 := adSetup(t, "")
	h2 := dcDoing(t, fx2.repo, fx2.iid, "review")
	fx2.aidant(h2, "userA", "userX", "", nil)
	c2 := fx2.rows(h2)[0]
	adOK(t, fx2.exec(h2, "userA", int(model.SubmitTypeRollbackToOperator)))
	if fx2.task(c2.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("退回发起人离开节点应清场协办行")
	}
	if dcDoing(t, fx2.repo, fx2.iid, "apply") == nil {
		t.Errorf("退回发起人后申请节点复活")
	}
}

// ═══ 正向：账本与投影 ═════════════════════════════════════════════════════════

// 两跳账本＝2，且主办办结后仍在（变量合并序保证存活）。
func TestAidantTwoRoundsLedgerSurvivesHostFinish(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "第一轮", false)
	fx.exec(fx.rows(host)[0], "userX", int(model.SubmitTypeAgree))
	fx.aidant(host, "userA", "userY", "第二轮", false)

	lg := fx.ledger(host)
	if len(lg) != 2 {
		t.Fatalf("两跳账本＝2，实得 %d", len(lg))
	}
	if adInt(lg[0]["round"]) != 1 || adInt(lg[1]["round"]) != 2 {
		t.Errorf("轮次应 1/2，实得 %v %v", lg[0]["round"], lg[1]["round"])
	}
	if lg[0]["reason"] != "第一轮" || lg[1]["reason"] != "第二轮" {
		t.Errorf("两轮 reason 各自留痕")
	}
	if strings.Join(adActors(lg[0]["toActors"]), ",") != "userX" ||
		strings.Join(adActors(lg[1]["toActors"]), ",") != "userY" {
		t.Errorf("两轮 toActors 各自留痕")
	}

	fx.exec(fx.rows(host)[1], "userY", int(model.SubmitTypeAgree))
	adOK(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)))
	if !fx.task(host.ID).IsFinished() {
		t.Errorf("主办应已办结")
	}
	if len(fx.ledger(host)) != 2 {
		t.Errorf("办结后账本仍在，实得 %d", len(fx.ledger(host)))
	}
}

// 投影 ext.aidant＝{active, round, total, submitted, actors, wait}，从 parent＋task_type 现算。
func TestAidantDetailProjectionShape(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", []string{"userX", "userY"}, "协办原因", true)
	rows := fx.rows(host)

	ext := fx.detailExt(host.ID)
	p, _ := ext["aidant"].(map[string]interface{})
	if p == nil {
		t.Fatalf("主办行应输出 ext.aidant，实得 %v", ext)
	}
	for _, k := range []string{"active", "round", "total", "submitted", "actors", "wait"} {
		if _, ok := p[k]; !ok {
			t.Errorf("投影缺键 %s: %v", k, p)
		}
	}
	if len(p) != 6 {
		t.Errorf("投影应恰六键，实得 %d", len(p))
	}
	if p["active"] != true || adInt(p["round"]) != 1 || adInt(p["total"]) != 2 ||
		adInt(p["submitted"]) != 0 || p["wait"] != true {
		t.Errorf("投影读数不符: %v", p)
	}
	actors := adMaps(p["actors"])
	if len(actors) != 2 {
		t.Fatalf("actors 应 2 条，实得 %v", p["actors"])
	}
	for _, k := range []string{"actor", "taskId", "state"} {
		if _, ok := actors[0][k]; !ok {
			t.Errorf("actors 条目缺键 %s", k)
		}
	}
	if actors[0]["actor"] != "userX" || adInt(actors[0]["taskId"]) != int(rows[0].ID) ||
		adInt(actors[0]["state"]) != int(model.TaskStateDoing) {
		t.Errorf("actors[0] 不符: %v", actors[0])
	}

	// 办结一条 ⇒ submitted 现算推进
	fx.exec(rows[0], "userX", int(model.SubmitTypeAgree))
	p2, _ := fx.detailExt(host.ID)["aidant"].(map[string]interface{})
	if adInt(p2["submitted"]) != 1 || p2["active"] != true {
		t.Errorf("submitted 应现算推进到 1，实得 %v", p2)
	}

	// 无协办行的普通任务不输出该投影
	fx2 := adSetup(t, "")
	plain := dcDoing(t, fx2.repo, fx2.iid, "review")
	if fx2.detailExt(plain.ID)["aidant"] != nil {
		t.Errorf("无协办行不输出 ext.aidant")
	}
}

// ═══ 负向：六文案＋越权＋非 DOING ═════════════════════════════════════════════

func TestAidantNegativeActorIdsRequired(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adMsg(t, fx.aidant(host, "userA", nil, "", nil), "actorIds 必填")
	adMsg(t, fx.aidant(host, "userA", []string{}, "", nil), "actorIds 必填")
	adMsg(t, fx.aidant(host, "userA", "  ,  ", "", nil), "actorIds 必填")
	if len(fx.rows(host)) != 0 {
		t.Errorf("零写入")
	}
}

func TestAidantNegativeOperatorRequired(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adMsg(t, fx.aidant(host, "", "userX", "", nil), "operator 必填")
	adMsg(t, fx.aidant(host, "   ", "userX", "", nil), "operator 必填")
}

func TestAidantNegativeTaskNotFound(t *testing.T) {
	fx := adSetup(t, "")
	adMsg(t, fx.f.Flow("processTask/aidant", map[string]interface{}{
		"processTaskId": "999999", "operator": "userA", "actorIds": "userX",
	}), "任务不存在")
}

func TestAidantNegativeUnauthorizedOperator(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adMsg(t, fx.aidant(host, "userZ", "userX", "", nil), "无权限派生协办任务")
	if len(fx.rows(host)) != 0 {
		t.Errorf("零写入")
	}
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("主办行不动")
	}
}

// 名册内参与人（含加签进来的）可派生；flow.auto／flow.admin 例外放行。
func TestAidantRosterMemberAndPrivilegedAllowed(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "actorIds": []string{"userB"},
	}))
	adOK(t, fx.aidant(host, "userB", "userX", "", nil))
	adOK(t, fx.aidant(host, "flow.auto", "userY", "", nil))
	adOK(t, fx.aidant(host, "flow.admin", "userZ", "", nil))
	if len(fx.rows(host)) != 3 {
		t.Errorf("三次派生应 3 行，实得 %d", len(fx.rows(host)))
	}
}

func TestAidantNegativeHostNotDoing(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.exec(host, "userA", int(model.SubmitTypeAgree))
	if !fx.task(host.ID).IsFinished() {
		t.Fatalf("主办应已办结")
	}
	adMsg(t, fx.aidant(host, "userA", "userX", "", nil), "任务非进行中，不可派生协办")
}

func TestAidantNegativeDuplicateActor(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	adMsg(t, fx.aidant(host, "userA", "userX", "", nil), "该人已有未办结的协办任务")
	if len(fx.rows(host)) != 1 {
		t.Errorf("零写入：不重复建行")
	}
	if len(fx.ledger(host)) != 1 {
		t.Errorf("零写入：账本不增")
	}
	// 已办结的那位可以再派生（去重只认未办结）
	fx.exec(fx.rows(host)[0], "userX", int(model.SubmitTypeAgree))
	adOK(t, fx.aidant(host, "userA", "userX", "", nil))
	if len(fx.rows(host)) != 2 {
		t.Errorf("已办结后可再派生，实得 %d", len(fx.rows(host)))
	}
}

func TestAidantNegativeRecursionForbidden(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	child := fx.rows(host)[0]
	adMsg(t, fx.aidant(child, "userX", "userY", "", nil), "协办任务不能再派生协办")
	if len(fx.rows(host)) != 1 {
		t.Errorf("零写入")
	}
	if len(engine.AidantRows(fx.allTasks(), fx.task(child.ID))) != 0 {
		t.Errorf("协办行下不得有协办行")
	}
}

// ═══ 协办行能力边界 ═══════════════════════════════════════════════════════════

// 协办行可加/摘参与人、可被转办（走既有 actor 写腿／transfer，不造特例）。
func TestAidantRowSupportsActorWritesAndTransfer(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	child := fx.rows(host)[0]
	cid := child.ID

	adOK(t, fx.f.Flow("processTask/surrogate", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", cid), "actorIds": []string{"userK"},
	}))
	if ra, _ := fx.repo.FindTaskActors(context.Background(), cid); !containsStr2(ra, "userK") {
		t.Errorf("surrogate 应追加 userK，实得 %v", ra)
	}
	adOK(t, fx.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", cid), "actorIds": []string{"userL"},
	}))
	if ra, _ := fx.repo.FindTaskActors(context.Background(), cid); !containsStr2(ra, "userL") {
		t.Errorf("addCandidate 应追加 userL，实得 %v", ra)
	}
	adOK(t, fx.f.Flow("processTask/removeTaskActor", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", cid), "operator": "userK", "actorIds": []string{"userK"},
	}))
	if ra, _ := fx.repo.FindTaskActors(context.Background(), cid); containsStr2(ra, "userK") {
		t.Errorf("removeTaskActor 应摘掉自己那一票，实得 %v", ra)
	}

	adOK(t, fx.f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", cid), "operator": "userX",
		"fromActor": "userX", "toActor": "userT", "reason": "协办转办",
	}))
	ra, _ := fx.repo.FindTaskActors(context.Background(), cid)
	if containsStr2(ra, "userX") || !containsStr2(ra, "userT") {
		t.Errorf("转办应摘原人换新参与人，实得 %v", ra)
	}
	after := fx.task(cid)
	if after.ID != cid || !after.IsDoing() || after.TaskType != int(model.TaskTypeSecondary) {
		t.Errorf("转办不新建任务行、不改 task_type，实得 %+v", after)
	}
	adOK(t, fx.exec(after, "userT", int(model.SubmitTypeAgree)))
	if !fx.task(host.ID).IsDoing() {
		t.Errorf("转办后新办理人办结仍不驱动流程")
	}
	if dcDoing(t, fx.repo, fx.iid, "final") != nil {
		t.Errorf("下游不得有待办")
	}
}

// ═══ 回归：会签合并 ═══════════════════════════════════════════════════════════

// 会签票齐即推进，不被协办行挡（合并判据显式排除 task_type=1）。
func TestAidantCountersignMergeNotBlocked(t *testing.T) {
	fx := adSetupFlow(t, adCsFlow(fmt.Sprintf("aidant_cs_%d", dcSeqAdd(1))))
	members, _ := fx.repo.FindDoingTasks(context.Background(), fx.iid, nil)
	if len(members) != 2 {
		t.Fatalf("会签应预建 2 行，实得 %d", len(members))
	}
	var rowA, rowB *model.ProcessTask
	for _, m := range members {
		if containsStr2(m.ActorIDs, "userA") {
			rowA = m
		}
		if containsStr2(m.ActorIDs, "userB") {
			rowB = m
		}
	}
	if rowA == nil || rowB == nil {
		t.Fatalf("会签两成员行未找齐")
	}
	adOK(t, fx.aidant(rowA, "userA", "userX", "", nil))
	child := engine.AidantRows(fx.allTasks(), fx.task(rowA.ID))[0]
	if !child.IsDoing() || child.TaskName != "cs" {
		t.Errorf("协办行应 DOING 且同节点 cs")
	}

	adOK(t, fx.exec(rowA, "userA", int(model.SubmitTypeAgree)))
	adOK(t, fx.exec(rowB, "userB", int(model.SubmitTypeAgree)))
	if fx.state() != int(model.InstanceStateDone) {
		t.Errorf("票齐即推进：实例应办结，实得 %d", fx.state())
	}
	if fx.task(child.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("协办行随离开节点清场，实得 %d", fx.task(child.ID).TaskState)
	}
	if left, _ := fx.repo.FindDoingTasks(context.Background(), fx.iid, nil); len(left) != 0 {
		t.Errorf("不留孤儿待办，实得 %d", len(left))
	}
}

// 会签未齐时协办行办结也不得推进（推进闸＋合并判据两头都守住）。
func TestAidantOnCountersignDoesNotAdvanceEarly(t *testing.T) {
	fx := adSetupFlow(t, adCsFlow(fmt.Sprintf("aidant_cs2_%d", dcSeqAdd(1))))
	members, _ := fx.repo.FindDoingTasks(context.Background(), fx.iid, nil)
	rowA := members[0]
	if !containsStr2(rowA.ActorIDs, "userA") {
		rowA = members[1]
	}
	fx.aidant(rowA, "userA", "userX", "", nil)
	child := engine.AidantRows(fx.allTasks(), fx.task(rowA.ID))[0]
	fx.exec(child, "userX", int(model.SubmitTypeAgree))
	if fx.state() != int(model.InstanceStateDoing) {
		t.Errorf("协办办结不推进，实得 %d", fx.state())
	}
	if dcDoingCount(t, fx.repo, fx.iid, "cs") != 2 {
		t.Errorf("会签两成员行仍在，实得 %d", dcDoingCount(t, fx.repo, fx.iid, "cs"))
	}
}

// ═══ 回归：与征询并存（两闸各数各的行） ═══════════════════════════════════════

func TestAidantCoexistWithCollectTwoGatesCountOwnRows(t *testing.T) {
	fx := adSetup(t, dcCollectOn)
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA",
		"submitType": int(model.SubmitTypeAgree), engine.KeyAddSignOperators: []interface{}{"userC"},
	}))
	adOK(t, fx.aidant(host, "userA", "userX", "", true))

	hostRow := fx.task(host.ID)
	all := fx.allTasks()
	if total, _ := engine.CollectMemberCounts(all, hostRow); total != 1 {
		t.Errorf("征询收齐闸只数成员行，应 1 实得 %d", total)
	}
	if total, _ := engine.CountAidant(all, hostRow); total != 1 {
		t.Errorf("协办等待闸只数协办行，应 1 实得 %d", total)
	}
	if len(engine.CollectMemberRows(all, hostRow)) != 1 || len(engine.AidantRows(all, hostRow)) != 1 {
		t.Errorf("两族各 1 行")
	}
	aidantRow := engine.AidantRows(all, hostRow)[0]
	if engine.IsCollectMemberRow(all, aidantRow) {
		t.Errorf("协办行不是征询成员行")
	}

	// 协办先办结 ⇒ 征询闸仍拦（各数各的行），文案是征询那一族
	fx.exec(aidantRow, "userX", int(model.SubmitTypeAgree))
	adMsg(t, fx.exec(hostRow, "userA", int(model.SubmitTypeAgree)),
		"节点[review]动态会签意见未收齐（已提交 0/1），不能办理")

	member := engine.CollectMemberRows(fx.allTasks(), fx.task(host.ID))[0]
	adOK(t, fx.exec(member, "userC", int(model.SubmitTypeAgree)))
	adOK(t, fx.exec(fx.task(host.ID), "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("两闸齐过后主办推进")
	}
}

// 反向：征询已收齐、协办未办结 ⇒ 报协办那一族文案（证明两闸不串味）。
func TestAidantCollectDoneButAidantPendingReportsAidantMessage(t *testing.T) {
	fx := adSetup(t, dcCollectOn)
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA",
		"submitType": int(model.SubmitTypeAgree), engine.KeyAddSignOperators: []interface{}{"userC"},
	})
	fx.aidant(host, "userA", "userX", "", true)
	member := engine.CollectMemberRows(fx.allTasks(), fx.task(host.ID))[0]
	fx.exec(member, "userC", int(model.SubmitTypeAgree))
	adMsg(t, fx.exec(fx.task(host.ID), "userA", int(model.SubmitTypeAgree)),
		"节点[review]协办未办结（已办结 0/1），不能办理")
}

// 征询节点上 waitCompletion=false：主办收齐后可推进，未办结协办行清场（加签闸不被协办行挡）。
func TestAidantCollectNodeWaitFalseNotBlocked(t *testing.T) {
	fx := adSetup(t, dcCollectOn)
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", host.ID), "operator": "userA",
		"submitType": int(model.SubmitTypeAgree), engine.KeyAddSignOperators: []interface{}{"userC"},
	})
	fx.aidant(host, "userA", "userX", "", false)
	member := engine.CollectMemberRows(fx.allTasks(), fx.task(host.ID))[0]
	fx.exec(member, "userC", int(model.SubmitTypeAgree))
	adOK(t, fx.exec(fx.task(host.ID), "userA", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("缺省不等协办：收齐即推进")
	}
	aidantRow := engine.AidantRows(fx.allTasks(), fx.task(host.ID))[0]
	if fx.task(aidantRow.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("未办结协办行应清场，实得 %d", fx.task(aidantRow.ID).TaskState)
	}
}

// ═══ 回归：撤回清场 ═══════════════════════════════════════════════════════════

// 已办撤回：下游 DOING 协办行一并废弃（血缘自然覆盖），复活行是唯一活待办。
func TestAidantRecallCleansRows(t *testing.T) {
	fx := adSetup(t, `"allowRecall":true`)
	host := dcDoing(t, fx.repo, fx.iid, "review")
	adOK(t, fx.exec(host, "userA", int(model.SubmitTypeAgree)))
	finalRow := dcDoing(t, fx.repo, fx.iid, "final")
	if finalRow == nil {
		t.Fatalf("应推进到 final")
	}
	adOK(t, fx.aidant(finalRow, "userF", "userX", "", nil))
	child := engine.AidantRows(fx.allTasks(), fx.task(finalRow.ID))[0]
	if !child.IsDoing() {
		t.Fatalf("协办行应 DOING")
	}

	resurrected, err := fx.eng.RecallTask(context.Background(), host.ID, "userA")
	if err != nil {
		t.Fatalf("撤回应成功: %v", err)
	}
	if !resurrected.IsDoing() || resurrected.TaskName != "review" {
		t.Errorf("复活行应 DOING 且在 review")
	}
	if fx.task(child.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("下游协办行废弃，实得 %d", fx.task(child.ID).TaskState)
	}
	if fx.task(finalRow.ID).TaskState != model.TaskStateAbandoned {
		t.Errorf("下游主办行废弃，实得 %d", fx.task(finalRow.ID).TaskState)
	}
	if left, _ := fx.repo.FindDoingTasks(context.Background(), fx.iid, nil); len(left) != 1 {
		t.Errorf("唯一活待办，实得 %d", len(left))
	}
}

// 实例撤回：DOING 协办行随整单置 WITHDRAW(30)，不留孤儿待办。
func TestAidantInstanceWithdrawCleansRows(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	fx.aidant(host, "userA", "userX", "", nil)
	child := fx.rows(host)[0]
	adOK(t, fx.f.Flow("processInstance/withdraw", map[string]interface{}{
		"id": fmt.Sprintf("%d", fx.iid), "operator": "applicant",
	}))
	if fx.task(child.ID).TaskState != model.TaskStateWithdraw {
		t.Errorf("协办行应随整单 WITHDRAW(30)，实得 %d", fx.task(child.ID).TaskState)
	}
	if left, _ := fx.repo.FindDoingTasks(context.Background(), fx.iid, nil); len(left) != 0 {
		t.Errorf("不留孤儿待办，实得 %d", len(left))
	}
}

// ═══ 回归：既有 actor 写腿不变 ═══════════════════════════════════════════════

// transfer／surrogate／addCandidate 在主办行上行为不变（协办存在也不影响）。
func TestAidantTransferSurrogateAddCandidateUnchanged(t *testing.T) {
	fx := adSetup(t, "")
	host := dcDoing(t, fx.repo, fx.iid, "review")
	hid := host.ID
	fx.aidant(host, "userA", "userX", "", nil)

	adOK(t, fx.f.Flow("processTask/surrogate", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", hid), "actorIds": []string{"userB"},
	}))
	if ra, _ := fx.repo.FindTaskActors(context.Background(), hid); strings.Join(ra, ",") != "userA,userB" {
		t.Errorf("surrogate 只追加原人保留，实得 %v", ra)
	}
	adOK(t, fx.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", hid), "actorIds": []string{"userC"},
	}))
	if ra, _ := fx.repo.FindTaskActors(context.Background(), hid); strings.Join(ra, ",") != "userA,userB,userC" {
		t.Errorf("addCandidate 同一腿，实得 %v", ra)
	}
	adOK(t, fx.f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", hid), "operator": "userA",
		"fromActor": "userA", "toActor": "userD", "reason": "转办原因",
	}))
	ra, _ := fx.repo.FindTaskActors(context.Background(), hid)
	if containsStr2(ra, "userA") || !containsStr2(ra, "userD") {
		t.Errorf("transfer 摘原人换新参与人，实得 %v", ra)
	}
	after := fx.task(hid)
	if after.ID != hid {
		t.Errorf("转办不新建任务行")
	}
	if adInt(after.Variables[engine.KeySubmitType]) != int(model.SubmitTypeTransfer) {
		t.Errorf("转办留痕 submitType=7，实得 %v", after.Variables[engine.KeySubmitType])
	}
	if after.Variables["tf_transferHistory"] == nil {
		t.Errorf("转办留痕 tf_transferHistory 缺失")
	}
	if after.ActorID != "" {
		t.Errorf("转办不写 actor_id 列")
	}
	if len(fx.rows(after)) != 1 || !fx.rows(after)[0].IsDoing() {
		t.Errorf("协办行不受主办转办影响，仍 DOING")
	}
	adOK(t, fx.exec(after, "userD", int(model.SubmitTypeAgree)))
	if dcDoing(t, fx.repo, fx.iid, "final") == nil {
		t.Errorf("新办理人办结照常推进")
	}
}

// 权限码复用 wf:processTask:execute（OR 规则，照 246/247 B 案惯例，不建按钮种子）。
func TestAidantPermissionCodeRidesExecute(t *testing.T) {
	got := spi.PermissionCodes("processTask/aidant")
	if len(got) != 1 || got[0] != "wf:processTask:execute" {
		t.Errorf("aidant 权限码应复用 wf:processTask:execute，实得 %v", got)
	}
	// 哨兵：没进表的 action 仍走默认派生
	sentinel := spi.PermissionCodes("processTask/transfer")
	if len(sentinel) != 1 || sentinel[0] != "wf:processTask:transfer" {
		t.Errorf("transfer 应走默认派生，实得 %v", sentinel)
	}
}

// 节点级 taskType=1 仍解析期禁配（issues/252 不因本轮运行期派生而放宽）。
// 判据落在**解析期**＝发起/执行时 parse 模型那一刀（java ModelParser.parse、本栈
// validateAddSignFlow），deploy 只存内容不解析 ⇒ 断言点在 startAndExecute。
func TestAidantNodeLevelSecondaryStillBanned(t *testing.T) {
	fx := adSetup(t, "")
	name := fmt.Sprintf("aidant_ban_%d", dcSeqAdd(1))
	// review 节点的 taskType 由 0 改 1（只替换 review 那一处，apply 的 form 键不同名不会误伤）
	content := strings.Replace(dcFlow(name, ""), `"form":"form-r","taskType":0`,
		`"form":"form-r","taskType":1`, 1)
	defID := dcDeploy(t, fx.f, name, content)
	r := fx.f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": fmt.Sprintf("%d", defID), "operator": "applicant",
	})
	if code, _ := r["code"].(int); code == 0 {
		t.Fatalf("节点级协办配置必须解析期报错")
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "不允许配置协办任务类型") {
		t.Errorf("禁配文案不符，实得 %q", msg)
	}
}