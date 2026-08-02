package facade_test

import (
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
