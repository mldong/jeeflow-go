package facade_test

import (
	"context"
	"os"
	"reflect"
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

// ─── 测试 stub（与根目录 engine_test 等价） ───────────────────────────────────

type testUserProv struct{}

func (p *testUserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID, DeptID: "D01", DeptName: "测试部门", PostID: "P01", PostName: "测试岗位"}, nil
}

type testIDGen struct{ n int64 }

func (g *testIDGen) NextID() int64 { g.n++; return g.n }

type testExprEval struct{}

func (e *testExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	if v, ok := vars["amount"]; ok {
		amt := toFloat(v)
		if expr == "amount > 1000" {
			return amt > 1000, nil
		}
		if expr == "amount <= 1000" {
			return amt <= 1000, nil
		}
	}
	return false, nil
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	}
	return 0
}

var _ spi.UserProvider = (*testUserProv)(nil)
var _ spi.IDGenerator = (*testIDGen)(nil)
var _ spi.ExpressionEvaluator = (*testExprEval)(nil)

// ─── 门面路由测试（v1.1.0，spec §12 #15） ─────────────────────────────────────

type testOrgUserProv struct{}

func (p *testOrgUserProv) FindDeptLeaders(deptID string) ([]string, error)     { return nil, nil }
func (p *testOrgUserProv) FindDeptMainLeaders(deptID string) ([]string, error) { return nil, nil }
func (p *testOrgUserProv) FindByRole(roleCode string) ([]string, error) {
	if roleCode == "finance" {
		return []string{"finA", "finB"}, nil
	}
	return nil, nil
}

var _ spi.OrgUserProvider = (*testOrgUserProv)(nil)

func setupFacade() (*facade.Facade, *memory.Repository, *memory.ExtRepository) {
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	return facade.New(eng, repo, extRepo), repo, extRepo
}

func flowContent(t *testing.T, name string) []byte {
	t.Helper()
	candidates := []string{
		"../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
		"../../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
		"../../../../jeeflow-java/jeeflow-core/src/test/resources/flows/" + name,
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err == nil {
			return data
		}
	}
	t.Fatalf("flow json not found: %s", name)
	return nil
}

func TestFacadeDeployVersion(t *testing.T) {
	f, repo, _ := setupFacade()
	content := string(flowContent(t, "01-simple.json"))

	// 首次部署：version=0
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r)
	}
	def, err := repo.FindDefineByName(context.Background(), "simple")
	if err != nil || def == nil {
		t.Fatalf("define not found: %v", err)
	}
	if def.Version != 0 {
		t.Fatalf("first deploy version = %d, want 0", def.Version)
	}

	// 再次部署：version+1
	r = f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy failed: %v", r)
	}
	latest, _ := repo.FindDefineByName(context.Background(), "simple")
	if latest.Version != 1 {
		t.Fatalf("second deploy version = %d, want 1", latest.Version)
	}

	// 启停 + 删除
	if r := f.Flow("processDefine/upAndDown", map[string]interface{}{"id": def.ID, "state": 0}); r["code"].(int) != 0 {
		t.Fatalf("upAndDown failed: %v", r)
	}
	if got, _ := repo.FindDefineByID(context.Background(), def.ID); got.State != 0 {
		t.Fatalf("state not updated: %d", got.State)
	}
	if r := f.Flow("processDefine/remove", map[string]interface{}{"id": def.ID}); r["code"].(int) != 0 {
		t.Fatalf("remove failed: %v", r)
	}
}

func TestFacadeInstanceTaskAndWithdraw(t *testing.T) {
	f, repo, _ := setupFacade()
	content := flowContent(t, "01-simple.json")
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(content)})
	defineID := int64(0)
	if data, ok := r["data"].(map[string]interface{}); ok {
		defineID = mustI64(data["processDefineId"])
	}

	// startAndExecute：发起并自动完成 apply → task1(leader)
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "amount": "1000",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])

	// execute（AGREE=1）：leader 完成任务 → 实例完成
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("want task1 doing, got %+v", doing)
	}
	r = f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": doing[0].ID, "operator": "leader", "submitType": 1,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("execute failed: %v", r)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), instanceID)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("instance state = %d, want done", inst.State)
	}

	// withdraw：新流程实例撤回（级联废弃 doing）
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	instanceID2 := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	r = f.Flow("processInstance/withdraw", map[string]interface{}{"id": instanceID2, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("withdraw failed: %v", r)
	}
	after, _ := repo.FindDoingTasks(context.Background(), instanceID2, nil)
	if len(after) != 0 {
		t.Fatalf("withdraw should abandon doing tasks, got %+v", after)
	}
}

func TestFacadeDesignAndSurrogate(t *testing.T) {
	f, _, extRepo := setupFacade()
	content := string(flowContent(t, "01-simple.json"))

	// 保存设计（含内容快照）
	r := f.Flow("processDesign/save", map[string]interface{}{
		"name": "leave", "displayName": "请假流程", "content": content, "operator": "zhangsan",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("design save failed: %v", r)
	}
	designID := mustI64(r["data"].(map[string]interface{})["id"])

	// detail：含历史 + jsonObject
	r = f.Flow("processDesign/detail", map[string]interface{}{"id": designID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("design detail failed: %v", r)
	}
	data := r["data"].(map[string]interface{})
	if data["jsonObject"] == nil {
		t.Fatalf("detail should include jsonObject")
	}
	if _, ok := data["his"]; !ok {
		t.Fatalf("detail should include his")
	}

	// 发布设计 → 生成 define + isDeployed=1
	r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("design deploy failed: %v", r)
	}
	d, _ := extRepo.FindDesignByID(context.Background(), designID)
	if d.IsDeployed != 1 {
		t.Fatalf("isDeployed = %d, want 1", d.IsDeployed)
	}

	// 委托：新增 + 生效查询 + 分页 + 删除
	r = f.Flow("processSurrogate/save", map[string]interface{}{
		"operator": "zhangsan", "surrogate": "lisi", "processName": "leave",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate save failed: %v", r)
	}
	surrogateID := mustI64(r["data"].(map[string]interface{})["id"])
	hit, _ := extRepo.GetSurrogate(context.Background(), "zhangsan", "leave", time.Now())
	if hit == nil || hit.Surrogate != "lisi" {
		t.Fatalf("getSurrogate = %+v, want lisi", hit)
	}
	r = f.Flow("processSurrogate/page", map[string]interface{}{"operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate page failed: %v", r)
	}
	rows := r["data"].(map[string]interface{})["rows"].([]*model.ProcessSurrogate)
	if len(rows) != 1 {
		t.Fatalf("surrogate page rows = %d, want 1", len(rows))
	}
	r = f.Flow("processSurrogate/remove", map[string]interface{}{"id": surrogateID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate remove failed: %v", r)
	}
}

func TestFacadeViewEndpoints(t *testing.T) {
	f, repo, _ := setupFacade()
	content := string(flowContent(t, "01-simple.json"))
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	// getLastByName
	r = f.Flow("processDefine/getLastByName", map[string]interface{}{"processDefineName": "simple"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("getLastByName failed: %v", r)
	}
	if name, _ := r["data"].(map[string]interface{})["name"].(string); name != "simple" {
		t.Fatalf("getLastByName name = %v", name)
	}

	// startAndExecute → 视图端点
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])

	r = f.Flow("processInstance/approvalRecord", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("approvalRecord failed: %v", r)
	}
	if len(r["data"].([]map[string]interface{})) != 2 { // apply + task1
		t.Fatalf("approvalRecord rows = %d, want 2", len(r["data"].([]map[string]interface{})))
	}

	r = f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	hl := r["data"].(map[string]interface{})
	if !containsStr2(hl["activeNodeNames"].([]string), "task1") {
		t.Fatalf("highLight active should contain task1: %v", hl)
	}

	r = f.Flow("processInstance/getAssigneeTextData", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("getAssigneeTextData failed: %v", r)
	}

	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	r = f.Flow("processTask/detail", map[string]interface{}{"id": doing[0].ID, "operator": "leader"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("taskDetail failed: %v", r)
	}
	if exec, _ := r["data"].(map[string]interface{})["executable"].(bool); !exec {
		t.Fatalf("taskDetail executable should be true")
	}

	r = f.Flow("processTask/latest", map[string]interface{}{"processInstanceId": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("taskLatest failed: %v", r)
	}
	if name, _ := r["data"].(map[string]interface{})["taskName"].(string); name != "task1" {
		t.Fatalf("taskLatest = %v, want task1", name)
	}

	// 抄送：创建 + 已读 + 列表（ccList v1.3.0 补齐）
	if err := f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "zhangsan", "actorIds": []string{"lisi"},
	})["code"].(int); err != 0 {
		t.Fatalf("createCCInstance failed")
	}
	r = f.Flow("processInstance/updateCCStatus", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "lisi",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("updateCCStatus failed: %v", r)
	}
	r = f.Flow("processInstance/ccList", map[string]interface{}{"operator": "lisi"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("ccList failed: %v", r)
	}
	ccData := r["data"].(map[string]interface{})
	ccRows := ccData["rows"].([]map[string]interface{})
	if len(ccRows) != 1 {
		t.Fatalf("ccList rows = %d, want 1", len(ccRows))
	}
	if _, ok := ccRows[0]["ext"]; !ok {
		t.Fatalf("ccList 行缺 ext: %v", ccRows[0])
	}

	// 加签/转交
	r = f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": doing[0].ID, "actorIds": []string{"zhaoliu"},
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("addCandidate failed: %v", r)
	}
	actors, _ := repo.FindTaskActors(context.Background(), doing[0].ID)
	if !containsStr2(actors, "zhaoliu") {
		t.Fatalf("addCandidate actors = %v", actors)
	}

	// candidatePage：无模型候选 → 未配置钩子报错
	r = f.Flow("processTask/candidatePage", map[string]interface{}{"processTaskId": doing[0].ID})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("candidatePage without hook should fail, got %v", r)
	}
	// 注入钩子后可用
	f.SetUserSearch(func(q map[string]interface{}) ([]map[string]interface{}, int, error) {
		return []map[string]interface{}{{"userId": "u1", "realName": "用户1"}}, 1, nil
	})
	r = f.Flow("processTask/candidatePage", map[string]interface{}{"processTaskId": doing[0].ID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("candidatePage with hook failed: %v", r)
	}
}

func containsStr2(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestCandidatePageDualSource(t *testing.T) {
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	f := facade.New(eng, repo, extRepo)
	f.SetOrgUserProvider(&testOrgUserProv{})

	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "12-candidate-page.json"))})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r0)
	}
	def, err := repo.FindDefineByName(context.Background(), "candidate-flow")
	if err != nil || def == nil {
		t.Fatalf("define not found: %v", err)
	}
	// 直接启动（不自动完成 apply）→ apply 任务 → candidatePage 查 review 候选
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(doing) != 1 || doing[0].TaskName != "apply" {
		t.Fatalf("want apply task, got %+v", doing)
	}
	r := f.Flow("processTask/candidatePage", map[string]interface{}{"processTaskId": doing[0].ID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("candidatePage failed: %v", r)
	}
	data := r["data"].(map[string]interface{})
	rows := data["rows"].([]map[string]interface{})
	if len(rows) != 4 {
		t.Fatalf("candidates = %d, want 4 (userA/userB + finA/finB)", len(rows))
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row["userId"].(string)] = true
	}
	for _, want := range []string{"userA", "userB", "finA", "finB"} {
		if !got[want] {
			t.Fatalf("candidate missing %s: %v", want, rows)
		}
	}
}

func TestStartAndExecutePreAssign(t *testing.T) {
	f, repo, _ := setupFacade()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r0)
	}
	def, err := repo.FindDefineByName(context.Background(), "simple")
	if err != nil || def == nil {
		t.Fatalf("define not found: %v", err)
	}

	// 预指派人：f_nextNodeOperator=userA → 自动完成 apply → task1 参与者 = userA
	r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": def.ID, "operator": "user1", "f_nextNodeOperator": "userA",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	inst1 := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	doing, _ := repo.FindDoingTasks(context.Background(), inst1, nil)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("want task1, got %+v", doing)
	}
	actors1, _ := repo.FindTaskActors(context.Background(), doing[0].ID)
	if len(actors1) != 1 || actors1[0] != "userA" {
		t.Fatalf("预指派后 task1 参与者应为 userA, got %v", actors1)
	}

	// 未指定 → task1 参与者 = leader
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": def.ID, "operator": "user1",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	inst2 := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	doing2, _ := repo.FindDoingTasks(context.Background(), inst2, nil)
	if len(doing2) != 1 || doing2[0].TaskName != "task1" {
		t.Fatalf("want task1, got %+v", doing2)
	}
	actors2, _ := repo.FindTaskActors(context.Background(), doing2[0].ID)
	if len(actors2) != 1 || actors2[0] != "leader" {
		t.Fatalf("未指定时 task1 参与者应为 leader, got %v", actors2)
	}
}

func TestFacadeErrors(t *testing.T) {
	f, _, _ := setupFacade()
	r := f.Flow("foo/bar", nil)
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("unknown action should fail, got %v", r)
	}

	// 未配置扩展仓储时设计 action 报错
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	fNoExt := facade.New(eng, repo, nil)
	r = fNoExt.Flow("processDesign/page", nil)
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("design action without ext should fail, got %v", r)
	}
}

// ═══ highLight 决策分支表达式过滤（issues/06）═══

func TestHighLightFiltersDecisionBranch(t *testing.T) {
	f, repo, _ := setupFacade()
	content := string(flowContent(t, "03-decision-expr.json"))
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	// amount=500 → 走「amount <= 1000」分支（task3），task2 分支未执行
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "amount": 500,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])

	// 推进：task1(leader) → decision → task3(director) → end
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName == "task1" {
			_ = repo.AddTaskActor(context.Background(), tk.ID, []string{"leader"})
			r = f.Flow("processTask/execute", map[string]interface{}{
				"processTaskId": tk.ID, "operator": "leader", "submitType": 1,
			})
			if code, _ := r["code"].(int); code != 0 {
				t.Fatalf("execute task1 failed: %v", r)
			}
		}
	}
	doing, _ = repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName == "task3" {
			_ = repo.AddTaskActor(context.Background(), tk.ID, []string{"director"})
			r = f.Flow("processTask/execute", map[string]interface{}{
				"processTaskId": tk.ID, "operator": "director", "submitType": 1,
			})
			if code, _ := r["code"].(int); code != 0 {
				t.Fatalf("execute task3 failed: %v", r)
			}
		}
	}

	r = f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	hl := r["data"].(map[string]interface{})
	histEdges := hl["historyEdgeNames"].([]string)
	histNodes := hl["historyNodeNames"].([]string)
	// 走过的分支：e4（amount<=1000 → task3）+ e6（task3→end）
	if !containsStr2(histEdges, "e4") || !containsStr2(histEdges, "e6") {
		t.Fatalf("应包含走过的边 e4/e6: %v", histEdges)
	}
	// 未走分支：e3/e5/task2 不得出现
	if containsStr2(histEdges, "e3") || containsStr2(histEdges, "e5") {
		t.Fatalf("未走分支边不应高亮: %v", histEdges)
	}
	if containsStr2(histNodes, "task2") {
		t.Fatalf("未走节点 task2 不应高亮: %v", histNodes)
	}
	if !containsStr2(histNodes, "task3") {
		t.Fatalf("应包含走过节点 task3: %v", histNodes)
	}
}

// ═══ 三个 detail 返回 jsonObject（issues/05-1）═══

func TestDetailJsonObject(t *testing.T) {
	f, repo, _ := setupFacade()
	content := string(flowContent(t, "01-simple.json"))
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content})
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	r = f.Flow("processDefine/detail", map[string]interface{}{"id": defineID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("defineDetail failed: %v", r)
	}
	if _, ok := r["data"].(map[string]interface{})["jsonObject"]; !ok {
		t.Fatalf("defineDetail 缺 jsonObject: %v", r["data"])
	}

	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	r = f.Flow("processInstance/detail", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("instanceDetail failed: %v", r)
	}
	if _, ok := r["data"].(map[string]interface{})["jsonObject"]; !ok {
		t.Fatalf("instanceDetail 缺 jsonObject: %v", r["data"])
	}

	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	r = f.Flow("processTask/detail", map[string]interface{}{"id": doing[0].ID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("taskDetail failed: %v", r)
	}
	if _, ok := r["data"].(map[string]interface{})["jsonObject"]; !ok {
		t.Fatalf("taskDetail 缺 jsonObject: %v", r["data"])
	}
}

func TestMQueryParams(t *testing.T) {
	// issues/05-5：m_ 前缀查询参数（m_LIKE_name / m_pd_LIKE_displayName / m_t_LIKE_displayName）
	f, _, _ := setupFacade()
	c1 := string(flowContent(t, "01-simple.json"))
	c2 := string(flowContent(t, "02-multi-task.json"))
	if r := f.Flow("processDefine/deploy", map[string]interface{}{"content": c1}); r["code"].(int) != 0 {
		t.Fatalf("deploy1: %v", r)
	}
	if r := f.Flow("processDefine/deploy", map[string]interface{}{"content": c2}); r["code"].(int) != 0 {
		t.Fatalf("deploy2: %v", r)
	}

	// 无别名 → 默认主表别名 t（t.name / t.display_name）
	r := f.Flow("processDefine/page", map[string]interface{}{"m_LIKE_name": "simple"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("definePage: %v", r)
	}
	rows := r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_LIKE_name 应过滤到 1 行: %v", r)
	}

	r = f.Flow("processDefine/page", map[string]interface{}{"m_LIKE_displayName": "简单"})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_LIKE_displayName 应过滤到 1 行: %v", r)
	}

	r = f.Flow("processDefine/page", map[string]interface{}{"m_LIKE_displayName": "流程"})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 2 {
		t.Fatalf("m_LIKE_displayName 应匹配全部: %v", r)
	}

	// 实例列表：m_pd_LIKE_displayName（别名 pd → pd.display_name）
	r = f.Flow("processDefine/getLastByName", map[string]interface{}{"processDefineName": "simple"})
	defineID := mustI64(r["data"].(map[string]interface{})["id"])
	if r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	}); r["code"].(int) != 0 {
		t.Fatalf("start: %v", r)
	}
	r = f.Flow("processInstance/page", map[string]interface{}{
		"operator": "zhangsan", "m_pd_LIKE_displayName": "简单",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_pd_LIKE_displayName 应命中: %v", r)
	}
	r = f.Flow("processInstance/page", map[string]interface{}{
		"operator": "zhangsan", "m_pd_LIKE_displayName": "zzz",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 0 {
		t.Fatalf("m_pd_LIKE_displayName 不应命中: %v", r)
	}

	// 任务列表：m_t_LIKE_displayName（别名 t → t.display_name）
	r = f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "leader", "m_t_LIKE_displayName": "审批",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_t_LIKE_displayName 应命中待办: %v", r)
	}
	r = f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "leader", "m_t_LIKE_displayName": "zzz",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if len(rows) != 0 {
		t.Fatalf("m_t_LIKE_displayName 不应命中: %v", r)
	}

	// 设计列表：无别名 m_LIKE_name（process-design 页）
	if r := f.Flow("processDesign/save", map[string]interface{}{
		"name": "leave", "displayName": "请假流程", "content": c1, "operator": "zhangsan",
	}); r["code"].(int) != 0 {
		t.Fatalf("design save: %v", r)
	}
	r = f.Flow("processDesign/page", map[string]interface{}{"m_LIKE_name": "leave"})
	if got := reflect.ValueOf(r["data"].(map[string]interface{})["rows"]).Len(); got != 1 {
		t.Fatalf("design m_LIKE_name 应命中 1 行, got %d: %v", got, r)
	}
}

func TestDesignDeployRedeployIsDeployed(t *testing.T) {
	// issues/08：部署/重新部署/设计稿变更的 is_deployed 状态同步
	f, _, extRepo := setupFacade()
	content := string(flowContent(t, "01-simple.json"))

	// 保存（含内容快照）→ 未部署
	r := f.Flow("processDesign/save", map[string]interface{}{
		"name": "leave08", "displayName": "请假流程08", "content": content, "operator": "zhangsan",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("save: %v", r)
	}
	designID := mustI64(r["data"].(map[string]interface{})["id"])
	design, _ := extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 0 {
		t.Fatalf("保存后应为未部署: %v", design.IsDeployed)
	}

	// 部署 → is_deployed=1
	r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r)
	}
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 1 {
		t.Fatalf("部署后应为已部署: %v", design.IsDeployed)
	}

	// 重新部署 → 同一 defineId（内容替换，version 不变）+ is_deployed=1
	r = f.Flow("processDesign/redeploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy: %v", r)
	}
	if got := mustI64(r["data"].(map[string]interface{})["processDefineId"]); got != defineID {
		t.Fatalf("redeploy 应复用同一 defineId: %d != %d", got, defineID)
	}
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 1 {
		t.Fatalf("重新部署后应为已部署: %v", design.IsDeployed)
	}

	// 设计稿内容变更（updateDefine，不同 content）→ 新快照 + is_deployed=0 + name 同步
	content2 := string(flowContent(t, "02-multi-task.json"))
	r = f.Flow("processDesign/updateDefine", map[string]interface{}{
		"processDesignId": designID, "content": content2, "operator": "zhangsan",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("updateDefine: %v", r)
	}
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 0 {
		t.Fatalf("内容变更后应为未部署: %v", design.IsDeployed)
	}
	if design.Name != "multi-task" {
		t.Fatalf("updateDefine 应同步 name: %v", design.Name)
	}

	// 基本信息修改（update）→ is_deployed 不变
	r = f.Flow("processDesign/update", map[string]interface{}{"id": designID, "displayName": "改名08", "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("update: %v", r)
	}
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.DisplayName != "改名08" || design.IsDeployed != 0 {
		t.Fatalf("update 应只改基本信息: %+v", design)
	}

	// 部署 → 再置 1
	r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy: %v", r)
	}
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 1 {
		t.Fatalf("再部署后应为已部署: %v", design.IsDeployed)
	}
}

// TestSnowflakeIDPrecision 雪花 id 精度守卫（issues/38 E9 对齐 Node）——
// 模拟集成方 encoding/json 解析行为：数字 → float64 超 2^53 显性报错（不静默截断），
// 字符串传递精确解析（报"不存在"而非截断后的错误 id）。
func TestSnowflakeIDPrecision(t *testing.T) {
	f, _, _ := setupFacade()

	// ① float64 雪花 id（encoding/json 默认解析产物，已丢精度）→ 显性报错
	r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": float64(2084320543834124288), "operator": "user1",
	})
	if code, _ := r["code"].(int); code == 0 {
		t.Fatalf("float64 雪花 id 必须报错: %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "超出") {
		t.Fatalf("应提示超出 float64 精确范围: %v", r)
	}

	// ② 字符串雪花 id → 精确解析（无该定义 → 报不存在，且消息含原始 id）
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": "2084320543834124290", "operator": "user1",
	})
	if code, _ := r["code"].(int); code == 0 {
		t.Fatalf("无该定义应失败: %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "2084320543834124290") {
		t.Fatalf("字符串应精确解析（消息应含原始雪花 id）: %v", r)
	}
}

// mustI64 出口 id 解析（1.8.5 起出口 id 为 string，issues/38 E9）——兼容 string/int64
func mustI64(v interface{}) int64 {
	switch t := v.(type) {
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case int64:
		return t
	case int:
		return int64(t)
	}
	return 0
}

// TestFacadeIDStringify 出口 id string 化（issues/38 E9 对齐 Node/Java 全局序列化）：
// API 返回的 id 类字段必须是 string（前端 JS number 无法承载雪花 id）
func TestFacadeIDStringify(t *testing.T) {
	f, _, _ := setupFacade()
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r)
	}
	pid, ok := r["data"].(map[string]interface{})["processDefineId"].(string)
	if !ok || pid == "" {
		t.Fatalf("processDefineId 应为 string: %v", r)
	}
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": pid, "operator": "user1"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("start: %v", r)
	}
	if _, ok := r["data"].(map[string]interface{})["processInstanceId"].(string); !ok {
		t.Fatalf("processInstanceId 应为 string: %v", r)
	}
	// 列表行 id 也为 string
	r = f.Flow("processDefine/page", map[string]interface{}{"pageNum": 1, "pageSize": 10})
	rows, ok := r["data"].(map[string]interface{})["rows"].([]map[string]interface{})
	if !ok || len(rows) == 0 {
		t.Fatalf("rows 应为非空列表: %v", r)
	}
	row := rows[0]
	if _, ok := row["id"].(string); !ok {
		t.Fatalf("列表行 id 应为 string: %v", row)
	}
}

// TestHighLightNodeProgress 节点成员进度回显（issue 41）：顺序会签进行中/推进/完成
func TestHighLightNodeProgress(t *testing.T) {
	f, repo, _ := setupFacade()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "06-countersign-sequential.json"))})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r0)
	}
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	if code, _ := r1["code"].(int); code != 0 {
		t.Fatalf("start: %v", r1)
	}
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])

	hl := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := hl["code"].(int); code != 0 {
		t.Fatalf("highLight: %v", hl)
	}
	np := hl["data"].(map[string]interface{})["nodeProgress"].(map[string]interface{})
	// 历史节点 apply：发起人 done
	apply := np["apply"].(map[string]interface{})["members"].([]map[string]interface{})
	if apply[0]["id"] != "user1" || apply[0]["done"] != true {
		t.Fatalf("apply 应 done: %v", apply)
	}
	// 顺序会签进行中：type=SEQUENTIAL、第一位 active
	task1 := np["task1"].(map[string]interface{})
	if task1["type"] != "SEQUENTIAL" {
		t.Fatalf("task1 type 应为 SEQUENTIAL: %v", task1)
	}
	members := task1["members"].([]map[string]interface{})
	if members[0]["id"] != "userA" || members[0]["active"] != true {
		t.Fatalf("userA 应 active: %v", members)
	}
	// 姓名走 UserProvider SPI 解析（testUserProv realName = "用户" + id）
	if members[0]["name"] != "用户userA" {
		t.Fatalf("name 应经 SPI 解析: %v", members[0])
	}
	if members[1]["id"] != "userB" || members[1]["done"] != nil || members[1]["active"] != nil {
		t.Fatalf("userB 应无标记: %v", members)
	}
	// 推进会签：userA done → userB active
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	_ = repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userA"})
	re := f.Flow("processTask/execute", map[string]interface{}{"processTaskId": doing[0].ID, "operator": "userA", "submitType": 1})
	if code, _ := re["code"].(int); code != 0 {
		t.Fatalf("execute: %v", re)
	}
	hl2 := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	np2 := hl2["data"].(map[string]interface{})["nodeProgress"].(map[string]interface{})
	m2 := np2["task1"].(map[string]interface{})["members"].([]map[string]interface{})
	if m2[0]["done"] != true || m2[1]["active"] != true {
		t.Fatalf("推进后 userA done / userB active: %v", m2)
	}
	// 全部完成 → 全部 done
	doing2, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	_ = repo.AddTaskActor(context.Background(), doing2[0].ID, []string{"userB"})
	re2 := f.Flow("processTask/execute", map[string]interface{}{"processTaskId": doing2[0].ID, "operator": "userB", "submitType": 1})
	if code, _ := re2["code"].(int); code != 0 {
		t.Fatalf("execute2: %v", re2)
	}
	hl3 := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	np3 := hl3["data"].(map[string]interface{})["nodeProgress"].(map[string]interface{})
	m3 := np3["task1"].(map[string]interface{})["members"].([]map[string]interface{})
	if m3[0]["done"] != true || m3[1]["done"] != true || m3[1]["active"] != nil {
		t.Fatalf("完成后全部 done: %v", m3)
	}
}

// TestPerformTypeStringCompat performType 字符串兼容（issue 42）：
// 'ALL' 面板格式会签行为与数字 1 一致（对齐 Java codeOf）
func TestPerformTypeStringCompat(t *testing.T) {
	f, repo, _ := setupFacade()
	// 面板格式：performType 存 'ALL' 字符串
	contentAll := strings.Replace(string(flowContent(t, "05-countersign-parallel.json")),
		`"performType": 1`, `"performType": "ALL"`, 1)
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": contentAll})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r0)
	}
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	if code, _ := r1["code"].(int); code != 0 {
		t.Fatalf("start: %v", r1)
	}
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	cs := []string{}
	for _, t := range doing {
		if t.TaskName == "task1" {
			cs = append(cs, t.ActorIDs[0])
		}
	}
	// 并行会签：3 参与者 → 3 个任务（普通语义只有 1 个）
	if len(cs) != 3 {
		t.Fatalf("ALL 格式应生成 3 个会签任务: %v", cs)
	}
	// nodeProgress 对 ALL 格式同样识别为会签（type=PARALLEL）
	hl := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := hl["code"].(int); code != 0 {
		t.Fatalf("highLight: %v", hl)
	}
	np := hl["data"].(map[string]interface{})["nodeProgress"].(map[string]interface{})
	if np["task1"].(map[string]interface{})["type"] != "PARALLEL" {
		t.Fatalf("ALL 格式 nodeProgress type 应为 PARALLEL: %v", np["task1"])
	}
}
