// 抄送写侧判重＝幂等空操作（issues/141 G2 · Go 栈，内存仓一路 ＋ 引擎/门面三条腿）。
//
// 立法逐字依据＝spec 06-facade.md §4「写侧判重＝幂等空操作（owner 2026-09-29 拍）」＋
// spec 11.2 原则 1「码值表达发生了什么事实」。同一 (instanceID, actorID) 已存在 cc 行时，
// 再次抄送必须：①不新增行 ②不重置未读状态 ③不更新原行时间 ④**不 fire CC_CREATE（码 4）**。
// 逐人 fire 的入参换成**实际新建的子集**，子集为空整支不 fire（不空转、也不照旧全量 fire）。
// 查询侧不引入 DISTINCT、历史重复行不清理（owner 明确接受既成事实），故这里只钉写侧。
//
// 判重义务覆盖三条入口（spec §11.7「同一支」）：发起 f_ccActors、办理 tf_ccActors
// （与发起腿同走引擎 HandleCcActors）、门面手动 processInstance/createCCInstance。
// SQL 仓一路见 repository/jdbc/cc_ownership_dedup_141_sqlite_test.go，两仓必须同答案
// （issues/117 场景 27 那把尺子）。
//
// ⚠️ 本仓改前的红：②③两档原先在内存仓**照不出来**（cc 存成 actor id 切片、没有 state/时间），
// 故内存仓升级为 [memory.CcRow] 行模型；①④与"子集 fire"三支的旧形状是无条件 append ＋
// 按原始请求全量 fire。
package facade_test

import (
	"context"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// setupCc141 门面＋引擎＋内存仓 ＋ 只收 CC_CREATE 的事件 sink。
func setupCc141(t *testing.T) (*facade.Facade, *memory.Repository, *[]engine.ProcessEvent) {
	t.Helper()
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	events := &[]engine.ProcessEvent{}
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{func(evt engine.ProcessEvent) {
			if evt.Type == engine.EventCCCreate {
				*events = append(*events, evt)
			}
		}},
	})
	return facade.New(eng, repo, memory.NewExt()), repo, events
}

// defineID141 部署 01-simple.json（start→apply→task1(leader)→end），办理腿要有这一条待办。
func defineID141(t *testing.T, f *facade.Facade) int64 {
	t.Helper()
	r := f.Flow("processDefine/deploy", map[string]interface{}{
		"content": string(flowContent(t, "01-simple.json")),
	})
	mustOk(t, r)
	return mustI64(r["data"].(map[string]interface{})["processDefineId"])
}

// start141 发起一条实例（可带发起腿 f_ccActors），返回实例 id。
func start141(t *testing.T, f *facade.Facade, defineID int64, ccActors interface{}) int64 {
	t.Helper()
	args := map[string]interface{}{"processDefineId": defineID, "operator": "zhangsan"}
	if ccActors != nil {
		args["f_ccActors"] = ccActors
	}
	r := f.Flow("processInstance/startAndExecute", args)
	mustOk(t, r)
	return mustI64(r["data"].(map[string]interface{})["processInstanceId"])
}

// execute141 办理 task1（可带办理腿 tf_ccActors），submitType=1 AGREE。
func execute141(t *testing.T, f *facade.Facade, repo *memory.Repository, instanceID int64, ccActors interface{}) {
	t.Helper()
	taskID := doingTaskID(t, repo, instanceID, "task1")
	if taskID == 0 {
		t.Fatalf("前置条件：实例 %d 应有 task1 待办", instanceID)
	}
	args := map[string]interface{}{
		"processTaskId": taskID, "operator": "leader", "submitType": int(model.SubmitTypeAgree),
	}
	if ccActors != nil {
		args[engine.KeyCcActors] = ccActors
	}
	mustOk(t, f.Flow("processTask/execute", args))
}

// manualCc141 门面手动腿。
func manualCc141(t *testing.T, f *facade.Facade, instanceID int64, actorIDs ...string) {
	t.Helper()
	mustOk(t, f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "zhangsan", "actorIds": actorIDs,
	}))
}

// markCcRead141 门面已读腿（②档的前置：先把 state 置成 1）。
func markCcRead141(t *testing.T, f *facade.Facade, instanceID int64, actorID string) {
	t.Helper()
	mustOk(t, f.Flow("processInstance/updateCCStatus", map[string]interface{}{
		"processInstanceId": instanceID, "operator": actorID,
	}))
}

func ccActors141(repo *memory.Repository, instanceID int64) []string {
	return repo.CcActorsForTest(instanceID)
}

func ccRow141(t *testing.T, repo *memory.Repository, instanceID int64, actorID string) *memory.CcRow {
	t.Helper()
	for _, row := range repo.CcRowsForTest(instanceID) {
		if row.ActorID == actorID {
			return row
		}
	}
	return nil
}

func ccEvtActors(events []engine.ProcessEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.CcActorID)
	}
	return out
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// tick141 让"原行时间被刷新"与"没被刷新"在断言上分得开（内存仓逐次取 time.Now，纳秒精度）。
func tick141() { time.Sleep(5 * time.Millisecond) }

// ═══ 正向对照：全新的一次抄送照旧建行＋逐人 fire ═══

// TestIssue141FirstCcStillCreatesRowsAndFiresPerActor 判重不许把正常抄送走掉：
// 两个新人 ⇒ 两行、逐人 fire 码 4（顺序与入参一致、sourceId 指向实例）、新行未读 state=0。
func TestIssue141FirstCcStillCreatesRowsAndFiresPerActor(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil
	tick141()

	manualCc141(t, f, instanceID, "6101", "6102")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6101", "6102"}) {
		t.Fatalf("全新抄送应逐人落行，实得 %v", got)
	}
	if len(*events) != 2 {
		t.Fatalf("全新抄送应逐人 fire 码 4，实得 %d 支", len(*events))
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"6101", "6102"}) {
		t.Fatalf("ccActorId 顺序须与入参一致，实得 %v", got)
	}
	for _, e := range *events {
		if e.InstanceID != instanceID {
			t.Fatalf("码 4 的 sourceId 应为 instanceID %d，实得 %d", instanceID, e.InstanceID)
		}
	}
	row := ccRow141(t, repo, instanceID, "6101")
	if row == nil {
		t.Fatalf("6101 的 cc 行没建出来")
	}
	if row.State != 0 {
		t.Fatalf("新行应是未读（state=0），实得 %d", row.State)
	}
}

// ═══ 四档：重复抄送是幂等空操作 ═══

// TestIssue141RepeatCcAddsNoRowAndFiresNothing ①不新增行 ＋ ④不 fire 码 4。
// 改前红：内存仓无条件 append（第二行）＋ 按原始 actors 全量 fire（一支空火）。
func TestIssue141RepeatCcAddsNoRowAndFiresNothing(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)

	manualCc141(t, f, instanceID, "6201")
	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6201"}) {
		t.Fatalf("首次抄送应落 1 行，实得 %v", got)
	}
	if len(*events) != 1 {
		t.Fatalf("首次抄送应 fire 1 次，实得 %d", len(*events))
	}

	*events = nil
	tick141()
	manualCc141(t, f, instanceID, "6201")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6201"}) {
		t.Fatalf("①重复抄送不得新增行，实得 %v", got)
	}
	if n := len(repo.CcRowsForTest(instanceID)); n != 1 {
		t.Fatalf("①重复抄送后行数仍是 1，实得 %d", n)
	}
	if len(*events) != 0 {
		t.Fatalf("④没发生创建就不得发码 4（spec 11.2 原则 1「码=事实」），实得 %v", ccEvtActors(*events))
	}
}

// TestIssue141RepeatCcDoesNotResetUnreadState ②不重置未读：先置已读，再重复抄送，
// state 必须仍是已读（owner 明确"不需要重置"，不产生"再提醒一次"语义）。
// 改前内存仓 cc 只存 actor id、UpdateCcStatus 是 no-op ⇒ 这一档根本照不出来（行模型升级后才成立）。
func TestIssue141RepeatCcDoesNotResetUnreadState(t *testing.T) {
	f, repo, _ := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)

	manualCc141(t, f, instanceID, "6301")
	markCcRead141(t, f, instanceID, "6301")
	row := ccRow141(t, repo, instanceID, "6301")
	if row == nil || row.State != 1 {
		t.Fatalf("置读后 state 应为 1，实得 %+v", row)
	}

	tick141()
	manualCc141(t, f, instanceID, "6301")

	after := ccRow141(t, repo, instanceID, "6301")
	if after == nil {
		t.Fatalf("②前置失效：cc 行不见了")
	}
	if after.State != 1 {
		t.Fatalf("②重复抄送不得把已读抹回未读，实得 state=%d", after.State)
	}
}

// TestIssue141RepeatCcDoesNotTouchOriginalRowTimes ③不更新原行时间：
// createTime 与 updateTime 逐字不变（也没有"删旧插新"）。
func TestIssue141RepeatCcDoesNotTouchOriginalRowTimes(t *testing.T) {
	f, repo, _ := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)

	manualCc141(t, f, instanceID, "6401")
	before := ccRow141(t, repo, instanceID, "6401")
	if before == nil || before.CreateTime.IsZero() {
		t.Fatalf("③前置失效：cc 行应带 createTime，实得 %+v", before)
	}
	createTime, updateTime := before.CreateTime, before.UpdateTime

	tick141()
	manualCc141(t, f, instanceID, "6401")

	after := ccRow141(t, repo, instanceID, "6401")
	if after == nil {
		t.Fatalf("③前置失效：cc 行不见了")
	}
	if !after.CreateTime.Equal(createTime) {
		t.Fatalf("③重复抄送不得刷新原行 createTime，实得 %v want %v", after.CreateTime, createTime)
	}
	if !after.UpdateTime.Equal(updateTime) {
		t.Fatalf("③重复抄送不得刷新原行 updateTime，实得 %v want %v", after.UpdateTime, updateTime)
	}
}

// TestIssue141RepeatCcFiresOnlyNewlyCreatedSubset ④的子集档：第二次同时给
// 「已知人＋新人」⇒ 只为新人建行、只为新人 fire（逐人 fire 的入参＝实际新建的子集）。
func TestIssue141RepeatCcFiresOnlyNewlyCreatedSubset(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)

	manualCc141(t, f, instanceID, "6501", "6502")
	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6501", "6502"}) {
		t.Fatalf("首轮应落两行，实得 %v", got)
	}
	if len(*events) != 2 {
		t.Fatalf("首轮应 fire 2 次，实得 %d", len(*events))
	}

	*events = nil
	tick141()
	manualCc141(t, f, instanceID, "6501", "6503")

	if got := ccEvtActors(*events); !eqStrs(got, []string{"6503"}) {
		t.Fatalf("逐人 fire 的入参应是实际新建的子集，实得 %v want [6503]", got)
	}
	if len(*events) != 1 {
		t.Fatalf("子集只有 1 人 ⇒ 只 fire 1 次，实得 %d 次", len(*events))
	}
	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6501", "6502", "6503"}) {
		t.Fatalf("实际新建的 cc 行也应只有 6503 那一行，实得 %v", got)
	}
}

// TestIssue141DuplicateWithinOneCallCollapses 同一次调用里重复给同一个人 ⇒ 也按幂等处理
// （一行一次提醒；子集里也不该出现两次）。
func TestIssue141DuplicateWithinOneCallCollapses(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil

	manualCc141(t, f, instanceID, "6601", "6601")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"6601"}) {
		t.Fatalf("同一次调用内的重复不应新增第二行，实得 %v", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"6601"}) {
		t.Fatalf("同一次调用内的重复只 fire 一次，实得 %v", got)
	}
}

// ═══ 引擎腿（f_ccActors ／ tf_ccActors）同一条判据 ═══

// TestIssue141EngineCcLegsShareTheSameDedupRule 办理腿与发起腿重叠的那个人不得再建行、
// 不得再 fire；新人照旧（spec §11.7「三条入口同一支」）。
func TestIssue141EngineCcLegsShareTheSameDedupRule(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	instanceID := start141(t, f, defineID, "7001")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7001"}) {
		t.Fatalf("发起腿应落 1 行，实得 %v", got)
	}
	if len(*events) != 1 {
		t.Fatalf("发起腿应 fire 1 次，实得 %d", len(*events))
	}

	*events = nil
	tick141()
	execute141(t, f, repo, instanceID, "7001,7002")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7001", "7002"}) {
		t.Fatalf("办理腿只为新人 7002 建行（7001 已有行），实得 %v", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7002"}) {
		t.Fatalf("办理腿只 fire 实际新建的子集，实得 %v want [7002]", got)
	}
}

// TestIssue141CcLegsFireNothingWhenAllDuplicated 三条入口的"整支不发"档：
// 发起腿抄过的人，办理腿与手动腿各自重复抄一次 ⇒ 两支都不发码 4（不空转）。
func TestIssue141CcLegsFireNothingWhenAllDuplicated(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	instanceID := start141(t, f, defineID, "7201")

	*events = nil
	tick141()
	execute141(t, f, repo, instanceID, "7201")
	if len(*events) != 0 {
		t.Fatalf("办理腿全重复 ⇒ 整支不得 fire，实得 %v", ccEvtActors(*events))
	}

	*events = nil
	manualCc141(t, f, instanceID, "7201")
	if len(*events) != 0 {
		t.Fatalf("手动腿全重复 ⇒ 整支不得 fire，实得 %v", ccEvtActors(*events))
	}
	if n := len(repo.CcRowsForTest(instanceID)); n != 1 {
		t.Fatalf("两次全重复抄送后仍应只有 1 行，实得 %d", n)
	}
}

// TestIssue141StringAndCollectionFormsShareTheDedupRule 两形态入参（逗号串 / 数组）共用
// 同一条判重腿：发起腿给数组、办理腿给逗号串，重叠的人仍只有一行、只 fire 新人。
func TestIssue141StringAndCollectionFormsShareTheDedupRule(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	instanceID := start141(t, f, defineID, []interface{}{"7101", "7102"})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7101", "7102"}) {
		t.Fatalf("集合形态照旧逐人建行，实得 %v", got)
	}
	if len(*events) != 2 {
		t.Fatalf("集合形态照旧逐人 fire，实得 %d 支", len(*events))
	}

	*events = nil
	tick141()
	execute141(t, f, repo, instanceID, "7101,7103")

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7101", "7102", "7103"}) {
		t.Fatalf("逗号串形态与集合形态判重同一条，实得 %v", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7103"}) {
		t.Fatalf("两形态混用也只为新人 fire，实得 %v want [7103]", got)
	}
}

// ═══ 反向哨兵 ＋ 仓储 SPI ═══

// TestIssue141CcDedupIsScopedToInstanceNotGlobal 判重不得把"没抄送过的人"也吃掉：
// 不同实例上的同一个人各自建行、各 fire（作用域是按实例，不是全局）。
func TestIssue141CcDedupIsScopedToInstanceNotGlobal(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	first := start141(t, f, defineID, nil)
	second := start141(t, f, defineID, nil)
	if first == second {
		t.Fatalf("夹具失效：两个实例 id 必须不同（判重作用域是按实例）")
	}
	*events = nil

	manualCc141(t, f, first, "6701")
	tick141()
	manualCc141(t, f, second, "6701")

	if got := ccActors141(repo, first); !eqStrs(got, []string{"6701"}) {
		t.Fatalf("实例一应有自己的 cc 行，实得 %v", got)
	}
	if got := ccActors141(repo, second); !eqStrs(got, []string{"6701"}) {
		t.Fatalf("实例二不受实例一影响，同一个人照样建行，实得 %v", got)
	}
	if len(*events) != 2 {
		t.Fatalf("两个实例各 fire 一次，实得 %d 次", len(*events))
	}
}

// TestIssue141CreateCcInstanceIfAbsentReturnsActualSubset 仓储 SPI 本体：
// 返回**实际新建**的子集（顺序与入参一致、重复折叠），全重复时返回空子集；
// FindCcActorIDs 读回真实行集。这两支是三条入口"拿子集去 fire"的根据。
func TestIssue141CreateCcInstanceIfAbsentReturnsActualSubset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	inst := &model.ProcessInstance{DefineID: 1, State: 10, BusinessNo: "CC141-SPI", Operator: "zhangsan"}
	if err := repo.SaveInstance(ctx, inst); err != nil {
		t.Fatalf("建实例: %v", err)
	}

	ids, err := repo.FindCcActorIDs(ctx, inst.ID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("空实例没有 cc 行，实得 %v err=%v", ids, err)
	}

	created, err := repo.CreateCcInstanceIfAbsent(ctx, inst.ID, "zhangsan", "9001", "9002", "9001")
	if err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if !eqStrs(created, []string{"9001", "9002"}) {
		t.Fatalf("首轮应返回实际新建的子集 [9001 9002]（同一次调用内重复折叠），实得 %v", created)
	}

	created, err = repo.CreateCcInstanceIfAbsent(ctx, inst.ID, "zhangsan", "9001", "9003")
	if err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if !eqStrs(created, []string{"9003"}) {
		t.Fatalf("第二轮子集应只剩新人 [9003]，实得 %v", created)
	}

	created, err = repo.CreateCcInstanceIfAbsent(ctx, inst.ID, "zhangsan", "9001", "9002")
	if err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("全重复时子集必须为空（空 ⇒ 整支不 fire），实得 %v", created)
	}

	ids, err = repo.FindCcActorIDs(ctx, inst.ID)
	if err != nil || !eqStrs(ids, []string{"9001", "9002", "9003"}) {
		t.Fatalf("FindCcActorIDs 应读回真实行集，实得 %v err=%v", ids, err)
	}
	// 直连 CreateCcInstance（不走 IfAbsent）也必须同一条判据——两仓写侧判重都收在写点本身
	if err := repo.CreateCcInstance(ctx, inst.ID, "zhangsan", "9001"); err != nil {
		t.Fatalf("直连建 cc: %v", err)
	}
	if n := len(repo.CcRowsForTest(inst.ID)); n != 3 {
		t.Fatalf("直连 CreateCcInstance 同样判重 ⇒ 仍是 3 行，实得 %d", n)
	}
}
