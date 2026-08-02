package facade_test

import (
	"reflect"
	"context"
	"os"
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
		defineID = data["processDefineId"].(int64)
	}

	// startAndExecute：发起并自动完成 apply → task1(leader)
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "amount": "1000",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := r["data"].(map[string]interface{})["processInstanceId"].(int64)

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
	instanceID2 := r["data"].(map[string]interface{})["processInstanceId"].(int64)
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
	designID := r["data"].(map[string]interface{})["id"].(int64)

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
	surrogateID := r["data"].(map[string]interface{})["id"].(int64)
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
	defineID := r["data"].(map[string]interface{})["processDefineId"].(int64)

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
	instanceID := r["data"].(map[string]interface{})["processInstanceId"].(int64)

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
	defineID := r["data"].(map[string]interface{})["processDefineId"].(int64)

	// amount=500 → 走「amount <= 1000」分支（task3），task2 分支未执行
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "amount": 500,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := r["data"].(map[string]interface{})["processInstanceId"].(int64)

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
	defineID := r["data"].(map[string]interface{})["processDefineId"].(int64)

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
	instanceID := r["data"].(map[string]interface{})["processInstanceId"].(int64)
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
	defineID := r["data"].(map[string]interface{})["id"].(int64)
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
	designID := r["data"].(map[string]interface{})["id"].(int64)
	design, _ := extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 0 {
		t.Fatalf("保存后应为未部署: %v", design.IsDeployed)
	}

	// 部署 → is_deployed=1
	r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r)
	}
	defineID := r["data"].(map[string]interface{})["processDefineId"].(int64)
	design, _ = extRepo.FindDesignByID(nil, designID)
	if design.IsDeployed != 1 {
		t.Fatalf("部署后应为已部署: %v", design.IsDeployed)
	}

	// 重新部署 → 同一 defineId（内容替换，version 不变）+ is_deployed=1
	r = f.Flow("processDesign/redeploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy: %v", r)
	}
	if got := r["data"].(map[string]interface{})["processDefineId"].(int64); got != defineID {
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
