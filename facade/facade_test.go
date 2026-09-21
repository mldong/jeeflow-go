package facade_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/internal/flowsutil"
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
	data, err := os.ReadFile(filepath.Join(flowsutil.Dir(), name))
	if err != nil {
		t.Fatalf("flow json not found: %s", name)
	}
	return data
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

	// withdraw：新流程实例撤回（级联撤回 doing → 任务态 30）
	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	instanceID2 := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	before, _ := repo.FindDoingTasks(context.Background(), instanceID2, nil)
	if len(before) == 0 {
		t.Fatalf("撤回前应有 doing 任务")
	}
	r = f.Flow("processInstance/withdraw", map[string]interface{}{"id": instanceID2, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("withdraw failed: %v", r)
	}
	after, _ := repo.FindDoingTasks(context.Background(), instanceID2, nil)
	if len(after) != 0 {
		t.Fatalf("withdraw should withdraw doing tasks, got %+v", after)
	}
	// issues/113：撤回任务必须落 30（WITHDRAW），不能落 99（ABANDONED）——
	// "doing 清空"这条断言两种码值都满足，抓不到该缺陷
	for _, bt := range before {
		stored, err := repo.FindTaskByID(context.Background(), bt.ID)
		if err != nil || stored == nil {
			t.Fatalf("撤回后任务应仍可读到: %v", err)
		}
		if stored.TaskState != model.TaskStateWithdraw {
			t.Fatalf("撤回任务态 = %d, want %d（WITHDRAW）", stored.TaskState, model.TaskStateWithdraw)
		}
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
	// issues/58 E30：出口结构体切片已转 []interface{}（id 字符串化）
	rowsAny := r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rowsAny) != 1 {
		t.Fatalf("surrogate page rows = %d, want 1", len(rowsAny))
	}
	r = f.Flow("processSurrogate/remove", map[string]interface{}{"id": surrogateID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate remove failed: %v", r)
	}
}

// TestFacadeSurrogatePageInAndEqConditions issues/82-7：委托分页 m_IN_processName / m_EQ_enabled
// （对齐 Java 基准 testSurrogatePageInAndEqConditions；内存 ext 仓储须消费 m_ 条件，demo 即用内存）
func TestFacadeSurrogatePageInAndEqConditions(t *testing.T) {
	f, _, _ := setupFacade()
	for i, m := range []map[string]interface{}{
		{"operator": "zhangsan", "surrogate": "lisi", "processName": "leave", "enabled": 1},
		{"operator": "zhangsan", "surrogate": "wangwu", "processName": "overtime", "enabled": 1},
		{"operator": "zhangsan", "surrogate": "zhaoliu", "processName": "sick", "enabled": 0},
	} {
		r := f.Flow("processSurrogate/save", m)
		if code, _ := r["code"].(int); code != 0 {
			t.Fatalf("surrogate save %d failed: %v", i, r)
		}
	}
	surrogateCount := func(args map[string]interface{}) int {
		r := f.Flow("processSurrogate/page", args)
		if code, _ := r["code"].(int); code != 0 {
			t.Fatalf("surrogate page failed: %v", r)
		}
		return len(r["data"].(map[string]interface{})["rows"].([]interface{}))
	}

	// 无 m_ 过滤：3 条
	if got := surrogateCount(map[string]interface{}{"operator": "zhangsan"}); got != 3 {
		t.Fatalf("no-filter count = %d, want 3", got)
	}
	// m_IN_processName：命中 2
	inList := []interface{}{"leave", "overtime"}
	if got := surrogateCount(map[string]interface{}{"operator": "zhangsan", "m_IN_processName": inList}); got != 2 {
		t.Fatalf("m_IN count = %d, want 2", got)
	}
	// m_EQ_enabled=1：命中 2
	if got := surrogateCount(map[string]interface{}{"operator": "zhangsan", "m_EQ_enabled": 1}); got != 2 {
		t.Fatalf("m_EQ count = %d, want 2", got)
	}
	// m_IN + m_EQ 组合：sick/overtime 中仅启用 → 1（overtime）
	combined := map[string]interface{}{
		"operator": "zhangsan", "m_IN_processName": []interface{}{"sick", "overtime"}, "m_EQ_enabled": 1,
	}
	r := f.Flow("processSurrogate/page", combined)
	rows := r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("combined count = %d, want 1", len(rows))
	}
	if pn := rows[0].(map[string]interface{})["processName"]; pn != "overtime" {
		t.Fatalf("combined processName = %v, want overtime", pn)
	}
	// 负向：IN 全不命中 / EQ 无匹配 → 0
	if got := surrogateCount(map[string]interface{}{"operator": "zhangsan", "m_IN_processName": []interface{}{"none1", "none2"}}); got != 0 {
		t.Fatalf("IN no-hit count = %d, want 0", got)
	}
	if got := surrogateCount(map[string]interface{}{"operator": "zhangsan", "m_EQ_enabled": 2}); got != 0 {
		t.Fatalf("EQ no-match count = %d, want 0", got)
	}
}

// TestFacadeSurrogateEffectiveWindowAndEnabled issues/82-12：委托生效判断——
// 时间窗 startTime/endTime + enabled 过滤（五语言基准，对齐 Java testSurrogateEffectiveWindowAndEnabled）。
// 5 条委托各对应一个时间态；每条查询只命中其中一条（processName 精确区分），不依赖返回顺序。
func TestFacadeSurrogateEffectiveWindowAndEnabled(t *testing.T) {
	f, _, extRepo := setupFacade()
	op := "winop"
	saves := []map[string]interface{}{
		{"operator": op, "surrogate": "sA", "processName": "winA", "startTime": "2026-08-01 00:00:00", "endTime": "2026-08-31 23:59:59", "enabled": 1}, // 在窗
		{"operator": op, "surrogate": "sB", "processName": "winB", "startTime": "2026-09-01 00:00:00", "enabled": 1},                                          // 未到
		{"operator": op, "surrogate": "sC", "processName": "winC", "endTime": "2026-07-31 23:59:59", "enabled": 1},                                              // 已过
		{"operator": op, "surrogate": "sD", "processName": "winD", "enabled": 0},                                                                                // 无窗停用
		{"operator": op, "surrogate": "sE", "processName": "winE", "enabled": 1},                                                                                // 无窗启用
	}
	for i, m := range saves {
		r := f.Flow("processSurrogate/save", m)
		if code, _ := r["code"].(int); code != 0 {
			t.Fatalf("surrogate save %d failed: %v", i, r)
		}
	}
	ctx := context.Background()
	at := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	get := func(pn string, at time.Time) *model.ProcessSurrogate {
		hit, err := extRepo.GetSurrogate(ctx, op, pn, at)
		if err != nil {
			t.Fatalf("GetSurrogate(%s): %v", pn, err)
		}
		return hit
	}
	if h := get("winA", at); h == nil || h.Surrogate != "sA" {
		t.Fatalf("在窗委托应生效, got %+v", h)
	}
	if h := get("winB", at); h != nil {
		t.Fatalf("未到窗委托不应生效, got %+v", h)
	}
	if h := get("winC", at); h != nil {
		t.Fatalf("已过窗委托不应生效, got %+v", h)
	}
	if h := get("winD", at); h != nil {
		t.Fatalf("enabled=0 不应生效, got %+v", h)
	}
	if h := get("winE", at); h == nil || h.Surrogate != "sE" {
		t.Fatalf("无窗启用委托应生效（NULL=不限）, got %+v", h)
	}
	if h := get("winZ", at); h != nil {
		t.Fatalf("无匹配流程应返回 nil, got %+v", h)
	}
	// 换时间验证窗口边界随时间变化：B 在 9 月生效、A 在 9 月失效
	atSep := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	if h := get("winB", atSep); h == nil || h.Surrogate != "sB" {
		t.Fatalf("9 月：B 进入窗口应生效, got %+v", h)
	}
	if h := get("winA", atSep); h != nil {
		t.Fatalf("9 月：A 已出窗口不应生效, got %+v", h)
	}
}

// TestFacadeSurrogateDetailAndUpdate issues/77：委托编辑链路
// save（前端空格格式时间窗）→ detail 回显 → update 改字段 → detail 再回显 + 负向 id 不存在
func TestFacadeSurrogateDetailAndUpdate(t *testing.T) {
	f, _, extRepo := setupFacade()

	// 新增（带时间窗，前端 RangePicker 实际提交的 yyyy-MM-dd HH:mm:ss 空格格式）
	r := f.Flow("processSurrogate/save", map[string]interface{}{
		"operator": "zhangsan", "surrogate": "lisi", "processName": "leave",
		"startTime": "2026-08-01 00:00:00", "endTime": "2026-08-31 23:59:59", "enabled": 1,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate save failed: %v", r)
	}
	surrogateID := mustI64(r["data"].(map[string]interface{})["id"])

	// detail 回显：行结构齐全 + 时间格式化
	r = f.Flow("processSurrogate/detail", map[string]interface{}{"id": surrogateID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate detail failed: %v", r)
	}
	d := r["data"].(map[string]interface{})
	if d["processName"] != "leave" || d["operator"] != "zhangsan" || d["surrogate"] != "lisi" {
		t.Fatalf("detail fields = %+v", d)
	}
	if d["startTime"] != "2026-08-01 00:00:00" || d["endTime"] != "2026-08-31 23:59:59" {
		t.Fatalf("detail time window = %v / %v", d["startTime"], d["endTime"])
	}

	// update：改代理人/时间窗/启用状态（不带 operator，授权人应保留）
	r = f.Flow("processSurrogate/update", map[string]interface{}{
		"id": surrogateID, "surrogate": "wangwu", "processName": "leave",
		"startTime": "2026-09-01 00:00:00", "endTime": "2026-09-30 23:59:59", "enabled": 0,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate update failed: %v", r)
	}
	if got := mustI64(r["data"].(map[string]interface{})["id"]); got != surrogateID {
		t.Fatalf("update id = %d, want %d", got, surrogateID)
	}

	// detail 再回显：变更生效 + 授权人未被清空
	r = f.Flow("processSurrogate/detail", map[string]interface{}{"id": surrogateID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("surrogate detail(after update) failed: %v", r)
	}
	d = r["data"].(map[string]interface{})
	if d["surrogate"] != "wangwu" || d["operator"] != "zhangsan" || d["enabled"] != 0 {
		t.Fatalf("detail after update = %+v", d)
	}
	if d["startTime"] != "2026-09-01 00:00:00" || d["endTime"] != "2026-09-30 23:59:59" {
		t.Fatalf("detail after update time window = %v / %v", d["startTime"], d["endTime"])
	}
	// 仓储侧同步（update 真的写了）
	s, _ := extRepo.FindSurrogateByID(context.Background(), surrogateID)
	if s == nil || s.Surrogate != "wangwu" || s.Enabled != 0 {
		t.Fatalf("repo surrogate after update = %+v", s)
	}

	// 负向：id 不存在
	r = f.Flow("processSurrogate/detail", map[string]interface{}{"id": int64(99999)})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("detail missing id should be 99999999, got %v", r)
	}
	r = f.Flow("processSurrogate/update", map[string]interface{}{"id": int64(99999), "surrogate": "wangwu"})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("update missing id should be 99999999, got %v", r)
	}
	// 负向：update 缺 id
	r = f.Flow("processSurrogate/update", map[string]interface{}{"surrogate": "wangwu"})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("update without id should be 99999999, got %v", r)
	}
}

// TestFacadeSurrogateRemoveBatchIDs issues/95：前端「我的委托」行内与批量删除统一发
// {ids}（行内 = 长度 1 的数组），门面此前只读单数 {id} → 该页删除整体不可用；
// 单 {id} 形态保留兼容（移动端 workflow.uts 发这个）。
func TestFacadeSurrogateRemoveBatchIDs(t *testing.T) {
	f, _, extRepo := setupFacade()
	save := func(op, agent, name string) int64 {
		r := f.Flow("processSurrogate/save", map[string]interface{}{
			"operator": op, "surrogate": agent, "processName": name,
		})
		if code, _ := r["code"].(int); code != 0 {
			t.Fatalf("surrogate save(%s) failed: %v", name, r)
		}
		return mustI64(r["data"].(map[string]interface{})["id"])
	}
	gone := func(id int64, label string) {
		s, _ := extRepo.FindSurrogateByID(context.Background(), id)
		if s != nil {
			t.Fatalf("%s 应已删除, got %+v", label, s)
		}
	}

	a := save("zhangsan", "lisiA", "leaveA")
	b := save("zhangsan", "lisiB", "leaveB")

	// 批量删除 {ids}
	r := f.Flow("processSurrogate/remove", map[string]interface{}{"ids": []interface{}{a, b}})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("batch remove failed: %v", r)
	}
	gone(a, "批量 a")
	gone(b, "批量 b")

	// 行内删除：前端同样走 {ids}，长度 1
	c := save("lisiC", "lisiD", "leaveC")
	r = f.Flow("processSurrogate/remove", map[string]interface{}{"ids": []interface{}{c}})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("single-element ids remove failed: %v", r)
	}
	gone(c, "行内 c")

	// 单 {id} 兼容形态回归
	d := save("zhangsan", "lisiE", "leaveD")
	r = f.Flow("processSurrogate/remove", map[string]interface{}{"id": d})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("single id remove failed: %v", r)
	}
	gone(d, "单 id d")
}

// TestFacadeRemoveEmptyIDsRejected issues/95 §5②：{ids}/{id} 缺失或空数组一律报错，
// 禁止静默成功（Go 此前 {ids} 为空切片会循环 0 次直接回成功）。
func TestFacadeRemoveEmptyIDsRejected(t *testing.T) {
	f, _, _ := setupFacade()
	cases := []struct {
		action string
		args   map[string]interface{}
	}{
		{"processSurrogate/remove", map[string]interface{}{"ids": []interface{}{}}},
		{"processSurrogate/remove", map[string]interface{}{"surrogate": "lisi"}},
		{"processSurrogate/remove", map[string]interface{}{"ids": []interface{}{int64(123), nil}}},
		{"processDefine/remove", map[string]interface{}{"ids": []interface{}{}}},
		{"processDesign/remove", map[string]interface{}{"ids": []interface{}{}}},
		{"processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{}, "opType": 0}},
	}
	for _, tc := range cases {
		r := f.Flow(tc.action, tc.args)
		code, _ := r["code"].(int)
		if code != 99999999 {
			t.Fatalf("%s %v should be 99999999, got %v", tc.action, tc.args, r)
		}
		if msg, _ := r["msg"].(string); !strings.Contains(msg, "id 缺失或非法") {
			t.Fatalf("%s %v msg = %q, want 含「id 缺失或非法」", tc.action, tc.args, msg)
		}
	}
}

// ─── issues/96 §4B：门面「入口批量参数形态」矩阵（4 action × 4 形态）──────────
//
// 为什么要这一组：issues/95 的缺陷（引擎只认单数 {id}，前端 IdsParam 一律发 {ids}）
// 之所以六语言测试全绿仍漏检，根因是既有用例「按实现形状写、不按契约形状写」（issues/96 §1）。
// 本矩阵把每个删除/启停 action 的四种入口形态钉全，让同类回归下次红在 CI：
//
//	① {ids:[a,b]}         两条真实 id   → code=0，且事后回查两条都取不到（upAndDown：state 已改）
//	② {id:c}              单数旧形态     → code=0，且该条取不到（回归保护，防修坏）
//	③ {ids:[]}            空数组         → 99999999 + msg 含「id 缺失或非法」（禁止静默成功）
//	④ {ids:[""]} / 含 null 非法元素      → 99999999 + msg 含「id 缺失或非法」
//
// 与 issues/95 随批两用例的分工（避免重复断言）：
//
//	action                    ①              ②              ③                      ④
//	processSurrogate/remove   既有 BatchIDs    既有 BatchIDs    既有 EmptyIDsRejected   既有(null) + 新增("")
//	processDesign/remove      新增             新增             既有 EmptyIDsRejected   新增
//	processDefine/remove      新增             新增             既有 EmptyIDsRejected   新增
//	processDefine/upAndDown   新增             新增             既有 EmptyIDsRejected   新增
//
// ⚠️ upAndDown 的关键坑：facade.go 的 upAndDown 先校验 opType/state、后走 idListArgs，
// 负向态若不带合法 state，报错会来自 state 缺失而不是 ids —— 那样「msg 含 id 缺失或非法」
// 就成了恒真断言。故本矩阵每一态（含负向）都带合法 opType/state，并用 exclude 参数反向
// 断言 msg 不含「opType」，自证报错确实来自 ids 校验。

// mustIDsRejected 断言 ids 入口形态被拒：code=99999999 且 msg 含「id 缺失或非法」
// （Go 侧允许带诊断后缀，故用 contains 而非相等）。exclude 非空时再断 msg 不含该子串，
// 用于自证报错来自 ids 校验本身，而非同请求里更早的其它参数校验。
func mustIDsRejected(t *testing.T, f *facade.Facade, action string, args map[string]interface{}, exclude string) {
	t.Helper()
	r := f.Flow(action, args)
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s %v 应报错 99999999（禁止静默成功）, got %v", action, args, r)
	}
	msg, _ := r["msg"].(string)
	if !strings.Contains(msg, "id 缺失或非法") {
		t.Fatalf("%s %v msg = %q, want 含「id 缺失或非法」", action, args, msg)
	}
	if exclude != "" && strings.Contains(msg, exclude) {
		t.Fatalf("%s %v msg = %q 含「%s」→ 报错来自其它参数校验，ids 断言恒真", action, args, msg, exclude)
	}
}

// mustRowGone 删除后按门面详情 action 回查单条已取不到（契约级回查，不下探仓储）
func mustRowGone(t *testing.T, f *facade.Facade, detailAction string, id int64, label string) {
	t.Helper()
	r := f.Flow(detailAction, map[string]interface{}{"id": id})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s(%d) 事后回查 %s 应取不到, got %v", detailAction, id, label, r)
	}
}

// mustRowAlive 负向报错后回查数据仍在：报错不得有副作用（不得部分生效 / 不得误删）
func mustRowAlive(t *testing.T, f *facade.Facade, detailAction string, id int64, label string) {
	t.Helper()
	r := f.Flow(detailAction, map[string]interface{}{"id": id})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("%s(%d) %s 应仍可取到（负向报错不应有副作用）, got %v", detailAction, id, label, r)
	}
}

// mustDeployDefine 造「设计稿 + 已发布定义」：processDesign/save → updateDefine（内容快照，
// name 由内容同步）→ deploy → processDefine/getLastByName 取 define id。
// 载荷形态照抄 TestDesignDeployRedeployIsDeployed，未自创。defineName 须与流程 JSON 顶层 name 一致。
func mustDeployDefine(t *testing.T, f *facade.Facade, flowFile, defineName, displayName string) int64 {
	t.Helper()
	r := f.Flow("processDesign/save", map[string]interface{}{
		"name": defineName, "displayName": displayName, "operator": "matrix",
	})
	mustOk(t, r)
	designID := mustI64(r["data"].(map[string]interface{})["id"])
	mustOk(t, f.Flow("processDesign/updateDefine", map[string]interface{}{
		"processDesignId": designID, "content": string(flowContent(t, flowFile)), "operator": "matrix",
	}))
	mustOk(t, f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "matrix"}))
	r = f.Flow("processDefine/getLastByName", map[string]interface{}{"processDefineName": defineName})
	mustOk(t, r)
	return mustI64(r["data"].(map[string]interface{})["id"])
}

// mustDefineState 走门面取定义 state（停用/启用是否真的落库）
func mustDefineState(t *testing.T, f *facade.Facade, id int64, label string) int {
	t.Helper()
	r := f.Flow("processDefine/detail", map[string]interface{}{"id": id})
	mustOk(t, r)
	return int(mustI64(r["data"].(map[string]interface{})["state"]))
}

// TestFacadeIdsMatrixDefineRemove processDefine/remove 的四态矩阵（前端定义列表勾选删除走 {ids}）
func TestFacadeIdsMatrixDefineRemove(t *testing.T) {
	f, _, _ := setupFacade()

	// ① {ids:[a,b]}：两条真实定义批量删除 → 事后两条都取不到
	defA := mustDeployDefine(t, f, "01-simple.json", "simple", "矩阵定义A")
	defB := mustDeployDefine(t, f, "02-multi-task.json", "multi-task", "矩阵定义B")
	mustOk(t, f.Flow("processDefine/remove", map[string]interface{}{"ids": []interface{}{defA, defB}}))
	mustRowGone(t, f, "processDefine/detail", defA, "批量 defA")
	mustRowGone(t, f, "processDefine/detail", defB, "批量 defB")

	// ② {id:c}：单数旧形态回归保护
	defC := mustDeployDefine(t, f, "03-decision-expr.json", "decision-expr", "矩阵定义C")
	mustOk(t, f.Flow("processDefine/remove", map[string]interface{}{"id": defC}))
	mustRowGone(t, f, "processDefine/detail", defC, "单 id defC")

	// ③ {ids:[]}：空数组禁止静默成功，且已存在的定义不受影响
	defD := mustDeployDefine(t, f, "01-simple.json", "simple", "矩阵定义D")
	mustIDsRejected(t, f, "processDefine/remove", map[string]interface{}{"ids": []interface{}{}}, "")
	mustRowAlive(t, f, "processDefine/detail", defD, "空数组后 defD")

	// ④ {ids:[""]} / {ids:[id,nil]}：非法元素整批报错，未部分生效
	mustIDsRejected(t, f, "processDefine/remove", map[string]interface{}{"ids": []interface{}{""}}, "")
	mustIDsRejected(t, f, "processDefine/remove", map[string]interface{}{"ids": []interface{}{defD, nil}}, "")
	mustRowAlive(t, f, "processDefine/detail", defD, "非法元素后 defD")
}

// TestFacadeIdsMatrixDesignRemove processDesign/remove 的四态矩阵（设计器列表勾选删除走 {ids}）
func TestFacadeIdsMatrixDesignRemove(t *testing.T) {
	f, _, _ := setupFacade()
	content := string(flowContent(t, "01-simple.json"))
	saveDesign := func(displayName string) int64 {
		r := f.Flow("processDesign/save", map[string]interface{}{
			"name": "matrix", "displayName": displayName, "content": content, "operator": "matrix",
		})
		mustOk(t, r)
		return mustI64(r["data"].(map[string]interface{})["id"])
	}

	// ① {ids:[a,b]}
	designA, designB := saveDesign("矩阵设计A"), saveDesign("矩阵设计B")
	mustOk(t, f.Flow("processDesign/remove", map[string]interface{}{"ids": []interface{}{designA, designB}}))
	mustRowGone(t, f, "processDesign/detail", designA, "批量 designA")
	mustRowGone(t, f, "processDesign/detail", designB, "批量 designB")

	// ② {id:c}
	designC := saveDesign("矩阵设计C")
	mustOk(t, f.Flow("processDesign/remove", map[string]interface{}{"id": designC}))
	mustRowGone(t, f, "processDesign/detail", designC, "单 id designC")

	// ③ {ids:[]}
	designD := saveDesign("矩阵设计D")
	mustIDsRejected(t, f, "processDesign/remove", map[string]interface{}{"ids": []interface{}{}}, "")
	mustRowAlive(t, f, "processDesign/detail", designD, "空数组后 designD")

	// ④ {ids:[""]} / {ids:[id,nil]}
	mustIDsRejected(t, f, "processDesign/remove", map[string]interface{}{"ids": []interface{}{""}}, "")
	mustIDsRejected(t, f, "processDesign/remove", map[string]interface{}{"ids": []interface{}{designD, nil}}, "")
	mustRowAlive(t, f, "processDesign/detail", designD, "非法元素后 designD")
}

// TestFacadeIdsMatrixDefineUpAndDown processDefine/upAndDown 的四态矩阵。
// 与 remove 的差别：memory 仓储 UpdateDefineState 对不存在的 id 静默 no-op（对齐 JDBC 的
// 「影响 0 行不报错」），所以 code=0 毫无信息量——正向态必须回查 state 真的被改写。
// 每一态都带合法 opType/state，负向态额外断 msg 不含「opType」，排除「恒真」。
func TestFacadeIdsMatrixDefineUpAndDown(t *testing.T) {
	f, _, _ := setupFacade()
	defA := mustDeployDefine(t, f, "01-simple.json", "simple", "启停矩阵A")
	defB := mustDeployDefine(t, f, "02-multi-task.json", "multi-task", "启停矩阵B")
	defC := mustDeployDefine(t, f, "03-decision-expr.json", "decision-expr", "启停矩阵C")
	for _, id := range []int64{defA, defB, defC} {
		if got := mustDefineState(t, f, id, "建数后"); got != 1 {
			t.Fatalf("deploy 后 state = %d, want 1（前置不成立，矩阵断言会失真）", got)
		}
	}

	// ① {ids:[a,b], opType:0}：批量停用 → 两条 state 都真的变 0
	mustOk(t, f.Flow("processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{defA, defB}, "opType": 0}))
	if got := mustDefineState(t, f, defA, "批量停用 defA"); got != 0 {
		t.Fatalf("defA state = %d, want 0（批量停用未落库）", got)
	}
	if got := mustDefineState(t, f, defB, "批量停用 defB"); got != 0 {
		t.Fatalf("defB state = %d, want 0（批量停用未落库）", got)
	}

	// ①' {ids:[a,b], opType:1}：批量启用回 1（两个方向都要生效，防「state 被忽略」）
	mustOk(t, f.Flow("processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{defA, defB}, "opType": 1}))
	if got := mustDefineState(t, f, defA, "批量启用 defA"); got != 1 {
		t.Fatalf("defA state = %d, want 1", got)
	}

	// ② {id:c, state:0}：单数旧形态 + state 别名（opType/state 二选一）仍生效
	mustOk(t, f.Flow("processDefine/upAndDown", map[string]interface{}{"id": defC, "state": 0}))
	if got := mustDefineState(t, f, defC, "单 id defC"); got != 0 {
		t.Fatalf("defC state = %d, want 0（单 id + state 别名未落库）", got)
	}

	// ③ {ids:[], opType:0}：空数组报错，且报错来自 ids 校验（不带 opType 的那态见 ③'）
	mustIDsRejected(t, f, "processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{}, "opType": 0}, "opType")
	if got := mustDefineState(t, f, defA, "空数组后 defA"); got != 1 {
		t.Fatalf("defA state = %d, want 1（报错不应改状态）", got)
	}

	// ③' 不带 state：报错来自 state 校验而非 ids —— 记录本矩阵为何必须带 state（防恒真）
	r := f.Flow("processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{}})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("缺 state 也应报错, got %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "opType/state 缺失或非法") || strings.Contains(msg, "id 缺失或非法") {
		t.Fatalf("缺 state 时 msg 应只报 state 缺失（说明矩阵必须带合法 state 才测得到 ids）, got %q", msg)
	}

	// ④ {ids:[""], opType:0} / {ids:[id,nil], opType:0}：非法元素整批报错，状态不被改
	mustIDsRejected(t, f, "processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{""}, "opType": 0}, "opType")
	mustIDsRejected(t, f, "processDefine/upAndDown", map[string]interface{}{"ids": []interface{}{defA, nil}, "opType": 0}, "opType")
	if got := mustDefineState(t, f, defA, "非法元素后 defA"); got != 1 {
		t.Fatalf("defA state = %d, want 1（非法元素批次不应有部分生效）", got)
	}
}

// TestFacadeIdsMatrixSurrogateRemove processSurrogate/remove 形态④的空串变体。
// 该 action 的其余格已由 issues/95 随批两用例覆盖（见本段开头矩阵表）：
// ①② 在 TestFacadeSurrogateRemoveBatchIDs，③ 与「含 null」形态④ 在
// TestFacadeRemoveEmptyIDsRejected，此处只补 {ids:[""]}（前端多选组件偶发提交空串，
// PHP 侧正是这一格出现「空串静默成功」假绿，见 issues/96 §4A）。
func TestFacadeIdsMatrixSurrogateRemove(t *testing.T) {
	f, _, _ := setupFacade()
	r := f.Flow("processSurrogate/save", map[string]interface{}{
		"operator": "matrixop", "surrogate": "matrixagent", "processName": "matrixFlow",
		"startTime": "2026-08-01 00:00:00", "endTime": "2026-08-31 23:59:59", "enabled": 1,
	})
	mustOk(t, r)
	surrogateID := mustI64(r["data"].(map[string]interface{})["id"])

	mustIDsRejected(t, f, "processSurrogate/remove", map[string]interface{}{"ids": []interface{}{""}}, "")
	mustRowAlive(t, f, "processSurrogate/detail", surrogateID, "空串元素后委托")
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
	if len(r["data"].([]interface{})) != 2 { // apply + task1
		t.Fatalf("approvalRecord rows = %d, want 2", len(r["data"].([]interface{})))
	}

	r = f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	hl := r["data"].(map[string]interface{})
	if !containsStr2(toStrings(hl["activeNodeNames"].([]interface{})), "task1") {
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
	// issues/62：taskModel 补 form/ext（字段权限）
	dData := r["data"].(map[string]interface{})
	tm, _ := dData["taskModel"].(map[string]interface{})
	if tm["form"] != "leave-form" {
		t.Fatalf("taskModel.form = %v, want leave-form", tm["form"])
	}
	ext, _ := tm["ext"].(map[string]interface{})
	if ext["PERMISSION_f_leaveType"] != float64(1) {
		t.Fatalf("taskModel.ext.PERMISSION_f_leaveType = %v, want 1", ext["PERMISSION_f_leaveType"])
	}
	if ext["PERMISSION_days"] != float64(2) {
		t.Fatalf("taskModel.ext.PERMISSION_days = %v, want 2", ext["PERMISSION_days"])
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
	ccRows := ccData["rows"].([]interface{})
	if len(ccRows) != 1 {
		t.Fatalf("ccList rows = %d, want 1", len(ccRows))
	}
	row0, _ := ccRows[0].(map[string]interface{})
	if _, ok := row0["ext"]; !ok {
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
	rows := data["rows"].([]interface{})
	if len(rows) != 4 {
		t.Fatalf("candidates = %d, want 4 (userA/userB + finA/finB)", len(rows))
	}
	got := map[string]bool{}
	for _, rw := range rows {
		m, _ := rw.(map[string]interface{})
		got[m["userId"].(string)] = true
	}
	for _, want := range []string{"userA", "userB", "finA", "finB"} {
		if !got[want] {
			t.Fatalf("candidate missing %s: %v", want, rows)
		}
	}
	// issues/80：行键契约 {id, realName}（对齐前端 UserSelect valueField='id'）
	ids := make([]string, 0, len(rows))
	for _, rw := range rows {
		m, _ := rw.(map[string]interface{})
		id, ok := m["id"].(string)
		if !ok || id == "" {
			t.Fatalf("candidate row 缺 id 键: %v", m)
		}
		_, ok = m["realName"].(string)
		if !ok {
			t.Fatalf("candidate row 缺 realName 键: %v", m)
		}
		if uid, _ := m["userId"].(string); uid != "" && uid != id {
			t.Fatalf("id 与 userId 应一一对齐（行键归一）: id=%q userId=%q", id, uid)
		}
		ids = append(ids, id)
	}
	if !containsStr2(ids, "userA") {
		t.Fatalf("id 列表应含 userA: %v", ids)
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

// TestFacadeCreateCCInstanceEmptyActors issues/82 负向：抄送空 actors 报错
// （对齐 Java 基准 testCreateCCInstanceEmptyActors / PHP testCreateCCInstanceEmptyActors）。
// createCCInstance 空/缺失 actorIds → code 99999999 + "actorIds 缺失"
func TestFacadeCreateCCInstanceEmptyActors(t *testing.T) {
	f, _, _ := setupFacade()

	// 空 actorIds list
	r := f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": 123, "operator": "user1", "actorIds": []interface{}{},
	})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("empty actorIds should fail, got %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "actorIds 缺失") {
		t.Fatalf("empty actorIds msg = %q, want contains 'actorIds 缺失'", msg)
	}

	// 负向边界：actorIds 键完全缺失同样报错
	r = f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": 123, "operator": "user1",
	})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("missing actorIds should fail, got %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "actorIds 缺失") {
		t.Fatalf("missing actorIds msg = %q, want contains 'actorIds 缺失'", msg)
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
	histEdges := toStrings(hl["historyEdgeNames"].([]interface{}))
	histNodes := toStrings(hl["historyNodeNames"].([]interface{}))
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

// TestTaskDetailPerformTypeNumeric issues/78：taskDetail performType/taskType 出口数字契约
// （普通 0 / 会签 1，与 Java 修复后对齐；出口必须是数字而非枚举 name 字符串）。
func TestTaskDetailPerformTypeNumeric(t *testing.T) {
	f, repo, _ := setupFacade()

	// 普通流程：task1 performType=0 / taskType=0
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r0)
	}
	defID := r0["data"].(map[string]interface{})["processDefineId"]
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": defID, "operator": "zhangsan"})
	instID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	doing, _ := repo.FindDoingTasks(context.Background(), instID, nil)
	if len(doing) == 0 {
		t.Fatalf("应有进行中任务")
	}
	rd := f.Flow("processTask/detail", map[string]interface{}{"id": doing[0].ID, "operator": "leader"})
	if code, _ := rd["code"].(int); code != 0 {
		t.Fatalf("taskDetail: %v", rd)
	}
	vo := rd["data"].(map[string]interface{})
	if got := mustIntKind(t, vo["performType"], "普通 performType"); got != 0 {
		t.Fatalf("普通任务 performType 应=0: %v", vo["performType"])
	}
	if got := mustIntKind(t, vo["taskType"], "普通 taskType"); got != 0 {
		t.Fatalf("普通任务 taskType 应=0: %v", vo["taskType"])
	}

	// 会签流程：task1 performType=1
	r2 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "06-countersign-sequential.json"))})
	csDefID := r2["data"].(map[string]interface{})["processDefineId"]
	r3 := f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": csDefID, "operator": "user1"})
	csInstID := mustI64(r3["data"].(map[string]interface{})["processInstanceId"])
	csDoing, _ := repo.FindDoingTasks(context.Background(), csInstID, nil)
	if len(csDoing) == 0 {
		t.Fatalf("会签应有进行中任务")
	}
	rcs := f.Flow("processTask/detail", map[string]interface{}{"id": csDoing[0].ID, "operator": "userA"})
	if code, _ := rcs["code"].(int); code != 0 {
		t.Fatalf("会签 taskDetail: %v", rcs)
	}
	csVo := rcs["data"].(map[string]interface{})
	if got := mustIntKind(t, csVo["performType"], "会签 performType"); got != 1 {
		t.Fatalf("会签任务 performType 应=1（数字，非 \"COUNTERSIGN\"）: %v", csVo["performType"])
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
	rows := r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_LIKE_name 应过滤到 1 行: %v", r)
	}

	r = f.Flow("processDefine/page", map[string]interface{}{"m_LIKE_displayName": "简单"})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_LIKE_displayName 应过滤到 1 行: %v", r)
	}

	r = f.Flow("processDefine/page", map[string]interface{}{"m_LIKE_displayName": "流程"})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
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
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_pd_LIKE_displayName 应命中: %v", r)
	}
	r = f.Flow("processInstance/page", map[string]interface{}{
		"operator": "zhangsan", "m_pd_LIKE_displayName": "zzz",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 0 {
		t.Fatalf("m_pd_LIKE_displayName 不应命中: %v", r)
	}

	// issues/82-6：实例列表按编码搜 m_pd_LIKE_name（pd.name 白名单列）
	r = f.Flow("processInstance/page", map[string]interface{}{
		"operator": "zhangsan", "m_pd_LIKE_name": "simple",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_pd_LIKE_name 应命中 simple 实例: %v", r)
	}
	r = f.Flow("processInstance/page", map[string]interface{}{
		"operator": "zhangsan", "m_pd_LIKE_name": "zzz",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 0 {
		t.Fatalf("m_pd_LIKE_name 不应命中: %v", r)
	}

	// 任务列表：m_t_LIKE_displayName（别名 t → t.display_name）
	r = f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "leader", "m_t_LIKE_displayName": "审批",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("m_t_LIKE_displayName 应命中待办: %v", r)
	}
	r = f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "leader", "m_t_LIKE_displayName": "zzz",
	})
	rows = r["data"].(map[string]interface{})["rows"].([]interface{})
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

// ═══ processDesign/page 时间格式（issues/63）═══

func TestDesignPageTimeFormat(t *testing.T) {
	f, _, _ := setupFacade()
	r := f.Flow("processDesign/save", map[string]interface{}{
		"name": "time-fmt-test", "displayName": "时间格式测试", "operator": "zhangsan",
		// 82-9：设计页回显字段 remark/icon（对齐 Java 参考实现）
		"icon": "icon-echo", "remark": "回显验证备注",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("design save failed: %v", r)
	}

	r = f.Flow("processDesign/page", map[string]interface{}{"pageNum": 1, "pageSize": 100})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("design page failed: %v", r)
	}
	rows := r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) == 0 {
		t.Fatal("design page should return at least 1 row")
	}
	timeRe := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	for i, row := range rows {
		m := row.(map[string]interface{})
		for _, field := range []string{"createTime", "updateTime"} {
			v, ok := m[field].(string)
			if !ok {
				t.Fatalf("row[%d].%s should be string, got %T: %v", i, field, m[field], m[field])
			}
			if !timeRe.MatchString(v) {
				t.Fatalf("row[%d].%s = %q should match yyyy-MM-dd HH:mm:ss", i, field, v)
			}
		}
	}

	// 82-9：designPage 行回显 remark/icon（设计页回显字段，对齐 Java）
	var target map[string]interface{}
	for _, row := range rows {
		m := row.(map[string]interface{})
		if m["name"] == "time-fmt-test" {
			target = m
			break
		}
	}
	if target == nil {
		t.Fatalf("designPage 应含 time-fmt-test 行: %v", rows)
	}
	if target["remark"] != "回显验证备注" {
		t.Fatalf("designPage remark 应回显保存值, got %v", target["remark"])
	}
	if target["icon"] != "icon-echo" {
		t.Fatalf("designPage icon 应回显保存值, got %v", target["icon"])
	}
}

func TestDesignDeployRedeployIsDeployed(t *testing.T) {
	// issues/08：部署/重新部署/设计稿变更的 is_deployed 状态同步
	f, repo, extRepo := setupFacade()
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
	defAfterDeploy, _ := repo.FindDefineByID(nil, defineID)

	// 重新部署 → 同一 defineId（内容替换，version 不变）+ is_deployed=1
	r = f.Flow("processDesign/redeploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy: %v", r)
	}
	if got := mustI64(r["data"].(map[string]interface{})["processDefineId"]); got != defineID {
		t.Fatalf("redeploy 应复用同一 defineId: %d != %d", got, defineID)
	}
	// issues/59：redeploy 是替换语义，version 必须保持（JDBC 仓储曾因未携带 version 兜底误写 1）
	defAfterRedeploy, _ := repo.FindDefineByID(nil, defineID)
	if defAfterRedeploy.Version != defAfterDeploy.Version {
		t.Fatalf("redeploy 后 version 应不变: %d != %d", defAfterRedeploy.Version, defAfterDeploy.Version)
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

	// issues/59 强回归：把定义 version 抬到 >0 后 redeploy 必须保持
	// （修复前 facade 未携带 Version，memory 仓储整对象覆盖会把 version 打回 0，JDBC 兜底会误写 1）
	defineID2 := mustI64(r["data"].(map[string]interface{})["processDefineId"])
	defV1, _ := repo.FindDefineByID(nil, defineID2)
	defV1.Version = 5
	if err := repo.UpdateDefine(nil, defV1); err != nil {
		t.Fatalf("抬 version 失败: %v", err)
	}
	r = f.Flow("processDesign/redeploy", map[string]interface{}{"id": designID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("redeploy(2): %v", r)
	}
	defV2, _ := repo.FindDefineByID(nil, defineID2)
	if defV2.Version != 5 {
		t.Fatalf("redeploy 后 version 应保持 5, got %d", defV2.Version)
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

// mustIntKind 取整型值（含命名整型，如 model.PerformType）；字符串/非整型 → Fail。
// issues/78：钉 performType/taskType 出口必须为数字（非枚举 name 字符串）。
func mustIntKind(t *testing.T, v interface{}, label string) int {
	t.Helper()
	if v == nil {
		t.Fatalf("%s 应为数字，实际为 nil", label)
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Uint64 {
		return int(rv.Int())
	}
	t.Fatalf("%s 应为数字，实际为 %T(%v)", label, v, v)
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
	rows, ok := r["data"].(map[string]interface{})["rows"].([]interface{})
	if !ok || len(rows) == 0 {
		t.Fatalf("rows 应为非空列表: %v", r)
	}
	row, _ := rows[0].(map[string]interface{})
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
	apply := np["apply"].(map[string]interface{})["members"].([]interface{})
	apply0, _ := apply[0].(map[string]interface{})
	if apply0["id"] != "user1" || apply0["done"] != true {
		t.Fatalf("apply 应 done: %v", apply)
	}
	// 顺序会签进行中：type=SEQUENTIAL、第一位 active
	task1 := np["task1"].(map[string]interface{})
	if task1["type"] != "SEQUENTIAL" {
		t.Fatalf("task1 type 应为 SEQUENTIAL: %v", task1)
	}
	members := task1["members"].([]interface{})
	member0, _ := members[0].(map[string]interface{})
	if member0["id"] != "userA" || member0["active"] != true {
		t.Fatalf("userA 应 active: %v", members)
	}
	// 姓名走 UserProvider SPI 解析（testUserProv realName = "用户" + id）
	if member0["name"] != "用户userA" {
		t.Fatalf("name 应经 SPI 解析: %v", members[0])
	}
	member1, _ := members[1].(map[string]interface{})
	if member1["id"] != "userB" || member1["done"] != nil || member1["active"] != nil {
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
	m2 := np2["task1"].(map[string]interface{})["members"].([]interface{})
	if m2[0].(map[string]interface{})["done"] != true || m2[1].(map[string]interface{})["active"] != true {
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
	m3 := np3["task1"].(map[string]interface{})["members"].([]interface{})
	if m3[0].(map[string]interface{})["done"] != true || m3[1].(map[string]interface{})["done"] != true || m3[1].(map[string]interface{})["active"] != nil {
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

// TestE2EFeedbackRegression E2E 反馈回归（issues 53/52/56）：撤回状态 30 / 会签 performType 落库 / 发起抄送
func TestE2EFeedbackRegression(t *testing.T) {
	f, repo, _ := setupFacade()
	// 56：发起时抄送 f_ccActors
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	if code, _ := r0["code"].(int); code != 0 {
		t.Fatalf("deploy: %v", r0)
	}
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"],
		"operator":        "user1", "f_ccActors": "wangqiang,zhaomin",
	})
	if code, _ := r1["code"].(int); code != 0 {
		t.Fatalf("start: %v", r1)
	}
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	ccs, cctotal, _ := repo.PageCcInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 10}, "wangqiang")
	if cctotal < 1 || len(ccs) == 0 {
		t.Fatalf("抄送应创建: total=%d", cctotal)
	}
	// 52：会签任务 performType 落库
	r2 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "05-countersign-parallel.json"))})
	r3 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r2["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	csID := mustI64(r3["data"].(map[string]interface{})["processInstanceId"])
	csDoing, _ := repo.FindDoingTasks(context.Background(), csID, nil)
	cs := []*model.ProcessTask{}
	for _, t := range csDoing {
		if t.TaskName == "task1" {
			cs = append(cs, t)
		}
	}
	if len(cs) != 3 {
		t.Fatalf("会签任务数: %d", len(cs))
	}
	for _, ct := range cs {
		if ct.PerformType != 1 {
			t.Fatalf("会签任务 PerformType 应=1: %d", ct.PerformType)
		}
	}
	// 53：撤回状态 30
	r4 := f.Flow("processInstance/withdraw", map[string]interface{}{"id": instanceID, "operator": "user1"})
	if code, _ := r4["code"].(int); code != 0 {
		t.Fatalf("withdraw: %v", r4)
	}
	inst, _ := repo.FindInstanceByID(context.Background(), instanceID)
	if inst == nil || inst.State != model.InstanceStateWithdraw {
		t.Fatalf("撤回状态应=Withdraw(30): %v", inst.State)
	}
	doingAfter, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	if len(doingAfter) != 0 {
		t.Fatalf("撤回后无 doing: %d", len(doingAfter))
	}
	// issues/113：会签实例整单撤回时，全部 doing 会签任务同样落 30（WITHDRAW），
	// 不能落 99——99 留给一票否决的废弃路径（两码不得混用）
	r5 := f.Flow("processInstance/withdraw", map[string]interface{}{"id": csID, "operator": "user1"})
	if code, _ := r5["code"].(int); code != 0 {
		t.Fatalf("撤回会签实例: %v", r5)
	}
	for _, ct := range cs {
		stored, err := repo.FindTaskByID(context.Background(), ct.ID)
		if err != nil || stored == nil {
			t.Fatalf("撤回后会签任务应仍可读到: %v", err)
		}
		if stored.TaskState != model.TaskStateWithdraw {
			t.Fatalf("撤回会签任务态 = %d, want %d（WITHDRAW）", stored.TaskState, model.TaskStateWithdraw)
		}
	}
	if got, _ := repo.FindDoingTasks(context.Background(), csID, nil); len(got) != 0 {
		t.Fatalf("会签实例撤回后仍剩 %d 条 doing 任务", len(got))
	}
}

// ═══ execute submitType 2/3/4/5/6/20 门面行为（issues/79，前端按钮全量暴露路径）═══

// mustOk 断言门面返回 code=0
func mustOk(t *testing.T, r map[string]interface{}) {
	t.Helper()
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("expect code=0, got: %v", r)
	}
}

// doingTaskID 查实例下指定节点的进行中任务 id（无则 0）
func doingTaskID(t *testing.T, repo *memory.Repository, instanceID int64, name string) int64 {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName == name {
			return tk.ID
		}
	}
	return 0
}

// startMultiTaskAt 02-multi-task：发起（apply 自动完成）→ 推进到名为 name 的任务节点
func startMultiTaskAt(t *testing.T, f *facade.Facade, repo *memory.Repository, name string) int64 {
	t.Helper()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "02-multi-task.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	order := []string{"task1", "task2", "task3"}
	actor := []string{"leader", "manager", "boss"}
	target := 0
	for i, n := range order {
		if n == name {
			target = i
		}
	}
	for i := 0; i < target; i++ {
		tid := doingTaskID(t, repo, instanceID, order[i])
		if tid == 0 {
			t.Fatalf("应推进到 %s", order[i])
		}
		repo.AddTaskActor(context.Background(), tid, []string{actor[i]})
		mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
			"processTaskId": tid, "operator": actor[i], "submitType": 1,
		}))
	}
	return instanceID
}

// TestExecuteSubmitTypeBehavior issues/79：submitType 3/4/5/6 + 负向（对齐 Java 参考实现断言）
func TestExecuteSubmitTypeBehavior(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()

	// ── submitType=3 ROLLBACK：task2 退回上一步 → task1 新待办（actor=退回操作人），实例保持 DOING(10)
	rb := startMultiTaskAt(t, f, repo, "task2")
	t2 := doingTaskID(t, repo, rb, "task2")
	repo.AddTaskActor(ctx, t2, []string{"manager"})
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": t2, "operator": "manager", "submitType": 3}))
	rbTask1 := doingTaskID(t, repo, rb, "task1")
	if rbTask1 == 0 {
		t.Fatalf("ROLLBACK 应在 task1 产生新待办")
	}
	if actors, _ := repo.FindTaskActors(ctx, rbTask1); !containsStr2(actors, "manager") {
		t.Fatalf("退回任务 actor 应为退回操作人 manager: %v", actors)
	}
	if inst, _ := repo.FindInstanceByID(ctx, rb); inst.State != model.InstanceStateDoing {
		t.Fatalf("ROLLBACK 后实例应保持 DOING(10): %d", inst.State)
	}

	// ── submitType=4 JUMP：task3 跳转 apply（首任务节点 = start 直接后继，assignee 强制发起人）
	jp := startMultiTaskAt(t, f, repo, "task3")
	t3 := doingTaskID(t, repo, jp, "task3")
	repo.AddTaskActor(ctx, t3, []string{"boss"})
	jl := f.Flow("processTask/jumpAbleTaskNameList", map[string]interface{}{"processInstanceId": jp})
	mustOk(t, jl)
	jumpValues := []string{}
	for _, m := range jl["data"].([]interface{}) {
		jumpValues = append(jumpValues, m.(map[string]interface{})["value"].(string))
	}
	if !containsStr2(jumpValues, "task1") || !containsStr2(jumpValues, "apply") {
		t.Fatalf("jumpAble 应包含已完成的 task1/apply: %v", jumpValues)
	}
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": t3, "operator": "boss", "submitType": 4, "taskName": "apply",
	}))
	jpApply := doingTaskID(t, repo, jp, "apply")
	if jpApply == 0 {
		t.Fatalf("JUMP 应在 apply（首任务节点）产生新待办")
	}
	if actors, _ := repo.FindTaskActors(ctx, jpApply); len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("跳首任务节点 assignee 强制为发起人 zhangsan: %v", actors)
	}
	if inst, _ := repo.FindInstanceByID(ctx, jp); inst.State != model.InstanceStateDoing {
		t.Fatalf("JUMP 后实例应保持 DOING(10): %d", inst.State)
	}

	// ── 负向：JUMP taskName 不存在 → 99999999 + 「无法找到节点模型」
	jn := startMultiTaskAt(t, f, repo, "task2")
	t2n := doingTaskID(t, repo, jn, "task2")
	repo.AddTaskActor(ctx, t2n, []string{"manager"})
	jr := f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": t2n, "operator": "manager", "submitType": 4, "taskName": "no-such-node",
	})
	if code, _ := jr["code"].(int); code != 99999999 {
		t.Fatalf("JUMP 无效节点应报 99999999: %v", jr)
	}
	if !strings.Contains(fmt.Sprintf("%v", jr["msg"]), "无法找到节点模型") {
		t.Fatalf("JUMP 无效节点应报「无法找到节点模型」: %v", jr["msg"])
	}

	// ── submitType=5 RE_APPLY：task1 重新提交（前端 detail 抽屉场景，含 f_ 表单 + tf_nextNodeOperator）
	ra := startMultiTaskAt(t, f, repo, "task1")
	t1r := doingTaskID(t, repo, ra, "task1")
	repo.AddTaskActor(ctx, t1r, []string{"leader"})
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": t1r, "operator": "leader", "submitType": 5,
		"tf_nextNodeOperator": "manager", "f_leaveType": "annual",
	}))
	doingAfter, _ := repo.FindDoingTasks(ctx, ra, nil)
	if len(doingAfter) != 1 || doingAfter[0].TaskName != "task2" {
		t.Fatalf("RE_APPLY 后应推进到 task2: %v", doingAfter)
	}
	if actors, _ := repo.FindTaskActors(ctx, doingTaskID(t, repo, ra, "task2")); len(actors) != 1 || actors[0] != "manager" {
		t.Fatalf("tf_nextNodeOperator 应覆盖 task2 处理人: %v", actors)
	}
	if inst, _ := repo.FindInstanceByID(ctx, ra); inst.Variables["f_leaveType"] != "annual" {
		t.Fatalf("f_ 表单字段应落实例变量: %v", inst.Variables)
	}
	if inst, _ := repo.FindInstanceByID(ctx, ra); inst.State != model.InstanceStateDoing {
		t.Fatalf("RE_APPLY 后实例应保持 DOING(10): %d", inst.State)
	}

	// ── submitType=6 ROLLBACK_TO_OPERATOR：task3 退回发起人 → apply 重执行、actor=发起人 zhangsan
	ro := startMultiTaskAt(t, f, repo, "task3")
	t3o := doingTaskID(t, repo, ro, "task3")
	repo.AddTaskActor(ctx, t3o, []string{"boss"})
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": t3o, "operator": "boss", "submitType": 6}))
	roApply := doingTaskID(t, repo, ro, "apply")
	if roApply == 0 {
		t.Fatalf("ROLLBACK_TO_OPERATOR 应重执行首个任务节点 apply")
	}
	if actors, _ := repo.FindTaskActors(ctx, roApply); len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("退回发起人 assignee 强制为发起人 zhangsan: %v", actors)
	}
	if inst, _ := repo.FindInstanceByID(ctx, ro); inst.State != model.InstanceStateDoing {
		t.Fatalf("退回发起人后实例应保持 DOING(10): %d", inst.State)
	}

	// ── 负向：非处理人执行被拒（NOT_ALLOWED_EXECUTE）
	na := startMultiTaskAt(t, f, repo, "task1")
	t1n := doingTaskID(t, repo, na, "task1")
	nr := f.Flow("processTask/execute", map[string]interface{}{"processTaskId": t1n, "operator": "hacker", "submitType": 1})
	if code, _ := nr["code"].(int); code != 99999999 {
		t.Fatalf("非处理人执行应报 99999999: %v", nr)
	}
	if !strings.Contains(fmt.Sprintf("%v", nr["msg"]), "not allowed") {
		t.Fatalf("非处理人执行应报参与者错误: %v", nr["msg"])
	}
}

// TestExecuteReject issues/79：submitType=2 REJECT 门面参数路径（对齐 Java/PHP）
func TestExecuteReject(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	instID := startMultiTaskAt(t, f, repo, "task1")
	t1 := doingTaskID(t, repo, instID, "task1")
	repo.AddTaskActor(ctx, t1, []string{"leader"})
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": t1, "operator": "leader", "submitType": 2}))
	if inst, _ := repo.FindInstanceByID(ctx, instID); inst.State != model.InstanceStateReject {
		t.Fatalf("REJECT 后实例应为 REJECT(45): %d", inst.State)
	}
	if doing, _ := repo.FindDoingTasks(ctx, instID, nil); len(doing) != 0 {
		t.Fatalf("REJECT 后应无 DOING 任务: %d", len(doing))
	}
}

// doingTaskIDByActor 会签场景：同节点多个 DOING 任务（每 actor 一个），按 actor 定位
func doingTaskIDByActor(t *testing.T, repo *memory.Repository, instanceID int64, name, actor string) int64 {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName != name {
			continue
		}
		for _, a := range tk.ActorIDs {
			if a == actor {
				return tk.ID
			}
		}
	}
	return 0
}

// TestExecuteCountersignDisagreeSoft issues/91：未配 ONE_VOTE_VETO 时 submitType=20 为软拒绝
// （否决者任务正常完成、flag 记录、流程不阻断；06 串行会签继续推进到下一成员）
func TestExecuteCountersignDisagreeSoft(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	// 06-countersign-sequential：apply 自动完成 → task1 串行会签（Go 逐人创建，先 userA）
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "06-countersign-sequential.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskA := doingTaskIDByActor(t, repo, instanceID, "task1", "userA")
	if taskA == 0 {
		t.Fatalf("会签节点应有 userA 的 DOING 任务")
	}
	repo.AddTaskActor(ctx, taskA, []string{"userA"})
	// submitType=20（未配 ONE_VOTE_VETO → 软拒绝）：flag 记录，流程不阻断，串行推进到下一成员
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": taskA, "operator": "userA", "submitType": 20}))
	inst, _ := repo.FindInstanceByID(ctx, instanceID)
	if inst.State != model.InstanceStateDoing {
		t.Fatalf("软拒绝后实例应保持 DOING(10)，继续等 userB: %d", inst.State)
	}
	if v := toIntOfFlag(inst.Variables["countersignDisagreeFlag"]); v != 1 {
		t.Fatalf("countersignDisagreeFlag=1 应落实例变量: %v", inst.Variables["countersignDisagreeFlag"])
	}
	doneA, _ := repo.FindTaskByID(ctx, taskA)
	if doneA.TaskState != model.TaskStateDone {
		t.Fatalf("软拒绝任务应正常完成: %d", doneA.TaskState)
	}
	if v := toIntOfFlag(doneA.Variables["countersignDisagreeFlag"]); v != 1 {
		t.Fatalf("countersignDisagreeFlag=1 应落任务变量: %v", doneA.Variables)
	}
	if doneA.ActorID != "userA" {
		t.Fatalf("否决人应记录为实际操作人 userA: %s", doneA.ActorID)
	}
	// 软拒绝推进串行会签到下一成员：userB 任务应被创建且 DOING
	if doing := doingTaskIDByActor(t, repo, instanceID, "task1", "userB"); doing == 0 {
		t.Fatalf("软拒绝后串行会签应推进到 userB（DOING）")
	}
}

// TestExecuteCountersignOneVoteVeto issues/91：13（并行 + ONE_VOTE_VETO）→ 任一成员 submitType=20
// 一票否决 → 会签节点立即推进 end（实例 FINISHED），其余 DOING 会签任务废弃(99)
func TestExecuteCountersignOneVoteVeto(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "13-countersign-one-vote-veto.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	// 并行会签全员预创建：userA/userB/userC 三个 DOING 任务
	taskA := doingTaskIDByActor(t, repo, instanceID, "task1", "userA")
	taskB := doingTaskIDByActor(t, repo, instanceID, "task1", "userB")
	taskC := doingTaskIDByActor(t, repo, instanceID, "task1", "userC")
	if taskA == 0 || taskB == 0 || taskC == 0 {
		t.Fatalf("并行会签应预创建 userA/userB/userC 三个 DOING 任务: %d %d %d", taskA, taskB, taskC)
	}
	repo.AddTaskActor(ctx, taskA, []string{"userA"})
	// userA 会签不同意（已配 ONE_VOTE_VETO → 一票否决）
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": taskA, "operator": "userA", "submitType": 20}))
	inst, _ := repo.FindInstanceByID(ctx, instanceID)
	if inst.State != model.InstanceStateDone {
		t.Fatalf("一票否决后会签节点应立即推进 end（实例 FINISHED 20）: %d", inst.State)
	}
	if v := toIntOfFlag(inst.Variables["countersignDisagreeFlag"]); v != 1 {
		t.Fatalf("countersignDisagreeFlag=1 应落实例变量: %v", inst.Variables["countersignDisagreeFlag"])
	}
	doneA, _ := repo.FindTaskByID(ctx, taskA)
	if doneA.TaskState != model.TaskStateDone {
		t.Fatalf("否决任务应已完成: %d", doneA.TaskState)
	}
	if doneA.ActorID != "userA" {
		t.Fatalf("否决人应记录为实际操作人 userA: %s", doneA.ActorID)
	}
	// 否决应废弃其余成员（ABANDON 99）
	for _, tid := range []int64{taskB, taskC} {
		tk, _ := repo.FindTaskByID(ctx, tid)
		if tk == nil || tk.TaskState != model.TaskStateAbandoned {
			t.Fatalf("否决应废弃其余成员任务为 ABANDON(99): id=%d state=%v", tid, tk)
		}
	}
	if doing, _ := repo.FindDoingTasks(ctx, instanceID, nil); len(doing) != 0 {
		t.Fatalf("否决后应无 DOING 任务: %d", len(doing))
	}
}

// TestExecuteCountersignDisagreeParallelSoft issues/91：05 并行（未配 ONE_VOTE_VETO）submitType=20
// 软拒绝——否决者任务完成、flag 记录、流程不阻断，其余成员仍 DOING
func TestExecuteCountersignDisagreeParallelSoft(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "05-countersign-parallel.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "user1",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskA := doingTaskIDByActor(t, repo, instanceID, "task1", "userA")
	taskB := doingTaskIDByActor(t, repo, instanceID, "task1", "userB")
	taskC := doingTaskIDByActor(t, repo, instanceID, "task1", "userC")
	if taskA == 0 || taskB == 0 || taskC == 0 {
		t.Fatalf("并行会签应预创建 userA/userB/userC 三个 DOING 任务: %d %d %d", taskA, taskB, taskC)
	}
	repo.AddTaskActor(ctx, taskA, []string{"userA"})
	// userA 会签不同意（未配 ONE_VOTE_VETO → 软拒绝）
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": taskA, "operator": "userA", "submitType": 20}))
	inst, _ := repo.FindInstanceByID(ctx, instanceID)
	if inst.State != model.InstanceStateDoing {
		t.Fatalf("并行软拒绝后实例应保持 DOING(10)，等 userB/userC: %d", inst.State)
	}
	if v := toIntOfFlag(inst.Variables["countersignDisagreeFlag"]); v != 1 {
		t.Fatalf("countersignDisagreeFlag=1 应落实例变量: %v", inst.Variables["countersignDisagreeFlag"])
	}
	doneA, _ := repo.FindTaskByID(ctx, taskA)
	if doneA.TaskState != model.TaskStateDone {
		t.Fatalf("软拒绝任务应正常完成: %d", doneA.TaskState)
	}
	for _, tid := range []int64{taskB, taskC} {
		tk, _ := repo.FindTaskByID(ctx, tid)
		if tk == nil || tk.TaskState != model.TaskStateDoing {
			t.Fatalf("软拒绝不应废弃其余成员，应保持 DOING: id=%d state=%v", tid, tk)
		}
	}
}

// toIntOfFlag 断言辅助：变量中的数字 flag（int/float64 兼容）
func toIntOfFlag(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return -1
}

// toStrings 出口字符串数组转换（issues/58 E30：出口统一 []interface{}）
func toStrings(v []interface{}) []string {
	out := make([]string, 0, len(v))
	for _, x := range v {
		out = append(out, fmt.Sprintf("%v", x))
	}
	return out
}

// issues/81：listByType items 必含 processDefineState（前端发起按钮硬依赖），取值随定义 state 联动
func TestListByTypeProcessDefineState(t *testing.T) {
	f, _, _ := setupFacade()

	// design name 与 content 顶层 name 一致（listByType 按 name join 最新 define）
	r := f.Flow("processDesign/save", map[string]interface{}{
		"name": "leave81", "displayName": "请假81", "type": "approval", "operator": "user1"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("save failed: %v", r)
	}
	designID := mustI64(r["data"].(map[string]interface{})["id"])
	r = f.Flow("processDesign/updateDefine", map[string]interface{}{
		"processDesignId": designID, "operator": "user1",
		"name": "leave81", "displayName": "请假81", "type": "approval",
		"nodes": []interface{}{}, "edges": []interface{}{}})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("updateDefine failed: %v", r)
	}
	// 场景 A：deploy → 定义 state=1 → 可发起
	r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "user1"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r)
	}
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	itemA := listItemByType(t, f, "leave81")
	if itemA["processDefineId"] == nil || mustI64(itemA["processDefineId"]) != defineID {
		t.Fatalf("processDefineId 应回显 %d: %v", defineID, itemA)
	}
	if st, ok := itemA["processDefineState"].(int); !ok || st != 1 {
		t.Fatalf("启用定义 processDefineState 应为 1（前端可发起）: %v", itemA["processDefineState"])
	}

	// 场景 B：upAndDown 禁用 → 定义 state=0 → 前端置灰
	r = f.Flow("processDefine/upAndDown", map[string]interface{}{"id": defineID, "opType": 0})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("upAndDown failed: %v", r)
	}
	itemB := listItemByType(t, f, "leave81")
	if st, ok := itemB["processDefineState"].(int); !ok || st != 0 {
		t.Fatalf("禁用定义 processDefineState 应为 0（前端置灰）: %v", itemB["processDefineState"])
	}
}

// listItemByType：listByType 出口里按 design name 取 item（approval 分组）
func listItemByType(t *testing.T, f *facade.Facade, name string) map[string]interface{} {
	t.Helper()
	r := f.Flow("processDesign/listByType", map[string]interface{}{})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("listByType failed: %v", r)
	}
	groups, _ := r["data"].(map[string]interface{})
	approvalAny, _ := groups["approval"].([]interface{})
	for _, it := range approvalAny {
		item, _ := it.(map[string]interface{})
		if item["name"] == name {
			return item
		}
	}
	t.Fatalf("listByType 缺 %s item: %v", name, groups)
	return nil
}

// issues/82-2：分页五键整体（pageNum/pageSize/rows/recordCount/totalPage）
// issues/82-3：列表行 instanceExt 容器（任务行 + 实例行）
func TestPageEnvelopeAndInstanceExt(t *testing.T) {
	f, _, _ := setupFacade()
	if r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))}); r["code"].(int) != 0 {
		t.Fatalf("deploy: %v", r)
	}
	defineID := mustI64(f.Flow("processDefine/getLastByName", map[string]interface{}{"processDefineName": "simple"})["data"].(map[string]interface{})["id"])
	if r := f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": defineID, "operator": "zhangsan"}); r["code"].(int) != 0 {
		t.Fatalf("start: %v", r)
	}

	// 任务行：instanceExt 容器 + 分页五键
	r := f.Flow("processTask/todoList", map[string]interface{}{"operator": "leader"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("todoList: %v", r)
	}
	data := r["data"].(map[string]interface{})
	for _, k := range []string{"pageNum", "pageSize", "rows", "recordCount", "totalPage"} {
		if _, ok := data[k]; !ok {
			t.Fatalf("分页五键应含 %s: %v", k, data)
		}
	}
	rows := data["rows"].([]interface{})
	if len(rows) == 0 {
		t.Fatalf("todoList 应有行")
	}
	trow, _ := rows[0].(map[string]interface{})
	if _, ok := trow["instanceExt"]; !ok {
		t.Fatalf("任务行应含 instanceExt 容器: %v", trow)
	}

	// 实例行：ext（实例变量对象，对齐 Java 契约：实例行无 instanceExt 键）+ 分页五键
	r = f.Flow("processInstance/page", map[string]interface{}{"operator": "zhangsan"})
	data = r["data"].(map[string]interface{})
	for _, k := range []string{"pageNum", "pageSize", "rows", "recordCount", "totalPage"} {
		if _, ok := data[k]; !ok {
			t.Fatalf("分页五键应含 %s: %v", k, data)
		}
	}
	irows := data["rows"].([]interface{})
	if len(irows) == 0 {
		t.Fatalf("instancePage 应有行")
	}
	irow, _ := irows[0].(map[string]interface{})
	if _, ok := irow["ext"]; !ok {
		t.Fatalf("实例行应含 ext 容器: %v", irow)
	}
}

// issues/82-5：task detail 任务级 ext.isFirstTaskNode（前端 detail.vue 双兜底）
func TestTaskDetailExtIsFirstTaskNode(t *testing.T) {
	// 场景 1：startAndExecute 自动完成 apply → 剩 task1（DOING，非首节点）→ false
	f, repo, _ := setupFacade()
	if r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))}); r["code"].(int) != 0 {
		t.Fatalf("deploy: %v", r)
	}
	rStart := f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": mustDefineID(t, repo, "simple"), "operator": "zhangsan"})
	if code, _ := rStart["code"].(int); code != 0 {
		t.Fatalf("startAndExecute: %v", rStart)
	}
	instID := mustI64(rStart["data"].(map[string]interface{})["processInstanceId"])
	tasks, _ := repo.FindDoingTasks(context.Background(), instID, nil)
	var task1ID int64
	for _, tk := range tasks {
		if tk.TaskName == "task1" {
			task1ID = tk.ID
		}
	}
	if task1ID == 0 {
		t.Fatalf("应有 task1 进行中任务: %+v", tasks)
	}
	r := f.Flow("processTask/detail", map[string]interface{}{"id": task1ID, "operator": "leader"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("taskDetail: %v", r)
	}
	ext, ok := r["data"].(map[string]interface{})["ext"].(map[string]interface{})
	if !ok {
		t.Fatalf("task detail 应含 ext 容器: %v", r["data"])
	}
	if ext["isFirstTaskNode"] != false {
		t.Fatalf("task1 非首任务节点，ext.isFirstTaskNode 应为 false: %v", ext)
	}

	// 场景 2：直接启动（不自动完成 apply）→ apply 为首任务节点且 DOING → true
	repo2 := memory.New()
	eng := engine.New(repo2, &testUserProv{}, &testIDGen{}, &testExprEval{})
	f2 := facade.New(eng, repo2, memory.NewExt())
	if r := f2.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))}); r["code"].(int) != 0 {
		t.Fatalf("deploy2: %v", r)
	}
	def, _ := repo2.FindDefineByName(context.Background(), "simple")
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("startProcessInstanceByID: %v", err)
	}
	tasks2, _ := repo2.FindDoingTasks(context.Background(), inst.ID, nil)
	var applyID int64
	for _, tk := range tasks2 {
		if tk.TaskName == "apply" {
			applyID = tk.ID
		}
	}
	if applyID == 0 {
		t.Fatalf("apply 应为进行中任务: %+v", tasks2)
	}
	r = f2.Flow("processTask/detail", map[string]interface{}{"id": applyID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("taskDetail2: %v", r)
	}
	ext2, ok := r["data"].(map[string]interface{})["ext"].(map[string]interface{})
	if !ok {
		t.Fatalf("task detail 应含 ext 容器: %v", r["data"])
	}
	if ext2["isFirstTaskNode"] != true {
		t.Fatalf("apply 为首任务节点且 DOING，ext.isFirstTaskNode 应为 true: %v", ext2)
	}
}

// detailTaskRowOf 从 processInstance/detail 的 tasks 里按任务名取该行 VO（读出口值，不走内存聚合根）
func detailTaskRowOf(t *testing.T, f *facade.Facade, instanceID int64, taskName string) map[string]interface{} {
	t.Helper()
	r := f.Flow("processInstance/detail", map[string]interface{}{"id": instanceID})
	mustOk(t, r)
	raw, _ := r["data"].(map[string]interface{})["tasks"].([]interface{})
	for _, o := range raw {
		row, _ := o.(map[string]interface{})
		if row != nil && row["taskName"] == taskName {
			return row
		}
	}
	t.Fatalf("detail.tasks 里应有 %s 行（实际 %d 行）", taskName, len(raw))
	return nil
}

// detailTaskExtOf 同上，取该行 ext 容器
func detailTaskExtOf(t *testing.T, f *facade.Facade, instanceID int64, taskName string) map[string]interface{} {
	t.Helper()
	ext, _ := detailTaskRowOf(t, f, instanceID, taskName)["ext"].(map[string]interface{})
	return ext
}

// taskDetailExtOf processTask/detail 的任务级 ext
func taskDetailExtOf(t *testing.T, f *facade.Facade, taskID int64, operator string) map[string]interface{} {
	t.Helper()
	r := f.Flow("processTask/detail", map[string]interface{}{"id": taskID, "operator": operator})
	mustOk(t, r)
	ext, _ := r["data"].(map[string]interface{})["ext"].(map[string]interface{})
	return ext
}

// TestFacadeIsFirstTaskNodePrefersRowValue issues/121 P1：两处出口（实例详情 tasks 行 / 任务详情）
// 改成「行上值优先、缺键才回退现算」。差值只在**已办结的历史行**上看得见——现算带 doing 判定
// ⇒ 历史行恒 false，而行上值是真 true；血缘版回退（P2）要读的就是这条历史行。
// 存量老行没有这个键 ⇒ 回退现算得 false，且不报错。
func TestFacadeIsFirstTaskNodePrefersRowValue(t *testing.T) {
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	f := facade.New(eng, repo, memory.NewExt())
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "02-multi-task.json"))})
	mustOk(t, r0)
	defID := mustI64(r0["data"].(map[string]interface{})["processDefineId"])
	// 用引擎发起（不是 startAndExecute——它会把 apply 一并办结，就拿不到"进行中的首节点行"了）
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	applyID := doingTaskID(t, repo, inst.ID, "apply")
	if applyID == 0 {
		t.Fatalf("应有进行中的 apply 行")
	}

	// ① 进行中的首节点行：两处出口都读到行上值 true
	if v := detailTaskExtOf(t, f, inst.ID, "apply")[engine.KeyIsFirstTaskNode]; v != true {
		t.Fatalf("进行中的首节点行 instance detail 应读到 true，实际 %v", v)
	}
	if v := taskDetailExtOf(t, f, applyID, "applicant")[engine.KeyIsFirstTaskNode]; v != true {
		t.Fatalf("进行中的首节点行 task detail 应读到 true，实际 %v", v)
	}
	// ①b 门面 VO 真带得上这条曾经的死列：发起那条落库 0（不是 null、不是指针地址）
	if v := detailTaskRowOf(t, f, inst.ID, "apply")["taskParentId"]; v != "0" {
		t.Fatalf("门面 VO taskParentId 应为 \"0\"，实际 %v(%T)", v, v)
	}

	// ② 办结后读回**已办结的历史行**：标记随行存活（把出口退回纯现算，这两格必红）
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": applyID, "operator": "applicant", "submitType": 1,
	}))
	if tk, _ := repo.FindTaskByID(context.Background(), applyID); tk.TaskState == model.TaskStateDoing {
		t.Fatalf("apply 应已办结")
	}
	// ②b 下一行的 VO 血缘指针指向刚办结的 apply
	if v := detailTaskRowOf(t, f, inst.ID, "task1")["taskParentId"]; v != strconv.FormatInt(applyID, 10) {
		t.Fatalf("task1 行 VO taskParentId 应为 apply 的 id %d，实际 %v", applyID, v)
	}
	if v := detailTaskExtOf(t, f, inst.ID, "apply")[engine.KeyIsFirstTaskNode]; v != true {
		t.Fatalf("历史行仍应读到 true（标记随行存活）: %v", v)
	}
	if v := taskDetailExtOf(t, f, applyID, "applicant")[engine.KeyIsFirstTaskNode]; v != true {
		t.Fatalf("历史行 task detail 仍应读到 true: %v", v)
	}

	// ③ 存量行形状（引擎尚未写标记时落的老数据）：抹掉行上键 ⇒ 只能回退现算 ⇒ 历史行 false，不报错
	row, err := repo.FindTaskByID(context.Background(), applyID)
	if err != nil || row == nil {
		t.Fatalf("apply 行读不回: %v", err)
	}
	delete(row.Variables, model.IsFirstTaskNodeKey)
	_ = repo.UpdateTask(context.Background(), row)
	if ext := detailTaskExtOf(t, f, inst.ID, "apply"); ext[engine.KeyIsFirstTaskNode] != false {
		t.Fatalf("缺键历史行应回退现算得 false: %v", ext)
	}
	if ext := taskDetailExtOf(t, f, applyID, "applicant"); ext[engine.KeyIsFirstTaskNode] != false {
		t.Fatalf("缺键历史行 task detail 应回退现算得 false: %v", ext)
	}
}

// issues/82-8：doneList 行 finishTime 已格式化（yyyy-MM-dd HH:mm:ss 无 T，对齐 Java/Python/Node）
func TestFacadeDoneListFinishTime(t *testing.T) {
	f, repo, _ := setupFacade()
	mustOk(t, f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))}))
	rStart := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": mustDefineID(t, repo, "simple"), "operator": "zhangsan"})
	mustOk(t, rStart)

	// startAndExecute 自动完成 apply → 剩 task1（DOING, leader）；执行 task1 → done（finishTime 写入）
	instID := mustI64(rStart["data"].(map[string]interface{})["processInstanceId"])
	tasks, _ := repo.FindDoingTasks(context.Background(), instID, nil)
	var task1ID int64
	for _, tk := range tasks {
		if tk.TaskName == "task1" {
			task1ID = tk.ID
		}
	}
	if task1ID == 0 {
		t.Fatalf("应有 task1 进行中任务: %+v", tasks)
	}
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{"processTaskId": task1ID, "operator": "leader", "submitType": 1}))

	// doneList：leader 的已办（task1 已完成）→ finishTime 非空且格式化
	r := f.Flow("processTask/doneList", map[string]interface{}{"operator": "leader"})
	mustOk(t, r)
	rows := r["data"].(map[string]interface{})["rows"].([]interface{})
	if len(rows) == 0 {
		t.Fatalf("doneList 应有行（task1 已办）")
	}
	trow := rows[0].(map[string]interface{})
	ft, ok := trow["finishTime"].(string)
	if !ok || ft == "" {
		t.Fatalf("doneList 行 finishTime 应为非空字符串: %v", trow["finishTime"])
	}
	re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	if !re.MatchString(ft) {
		t.Fatalf("doneList finishTime 应格式化为 yyyy-MM-dd HH:mm:ss（无 T）: %s", ft)
	}
}

// mustDefineID：按 name 取定义 id
func mustDefineID(t *testing.T, repo *memory.Repository, name string) int64 {
	t.Helper()
	def, err := repo.FindDefineByName(context.Background(), name)
	if err != nil || def == nil {
		t.Fatalf("define %s not found: %v", name, err)
	}
	return def.ID
}

// ═══ issues/102 抄送知会 CC_CREATE 事件（facade 层 fire）═══
// 语义对齐 PHP(381bed0)/Java(fa18804)：cc 实例落库后**逐抄送人** fire CC_CREATE，
// ccActorId 直传事件体（监听器免反查 cc 表）；接收人过滤归集成层监听器。
// T0 断言（对齐 Java CcCreateEventTest 双用例）：
//   ① 逐抄送人 fire：带抄送人发起 → 每个抄送人一条 CC_CREATE（顺序/InstanceID/CcActorID 逐项），
//      手动 createCCInstance 同样逐抄送人 fire；
//   ② 纯增量：未装配监听器时 cc 实例照常落库、FireEvent 零副作用、不抛错。

func TestCCCreateEventPerActor(t *testing.T) {
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	f := facade.New(eng, repo, extRepo)

	var ccEvents []engine.ProcessEvent
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{
			func(evt engine.ProcessEvent) {
				if evt.Type == engine.EventCCCreate {
					ccEvents = append(ccEvents, evt)
				}
			},
		},
	})

	// deploy + startAndExecute with 2 cc actors → 2 CC_CREATE（逐抄送人，顺序保持）
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"],
		"operator":        "user1", "f_ccActors": "cc_a, cc_b",
	})
	mustOk(t, r1)
	instID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])

	if len(ccEvents) != 2 {
		t.Fatalf("want 2 CC_CREATE (per actor), got %d: %+v", len(ccEvents), ccEvents)
	}
	want := []string{"cc_a", "cc_b"}
	for i, evt := range ccEvents {
		if evt.InstanceID != instID {
			t.Fatalf("CC_CREATE[%d] InstanceID=%d want %d", i, evt.InstanceID, instID)
		}
		if evt.CcActorID != want[i] {
			t.Fatalf("CC_CREATE[%d] CcActorID=%q want %q", i, evt.CcActorID, want[i])
		}
	}
	// cc 实例落库与事件一一对应（每个抄送人一条 cc 行）
	for _, a := range want {
		rows, total, _ := repo.PageCcInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 10}, a)
		if total < 1 || len(rows) == 0 {
			t.Fatalf("抄送人 %s 应落 cc 实例: total=%d", a, total)
		}
	}

	// 手动 createCCInstance 同样逐抄送人 fire（+2 条）
	r2 := f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": r1["data"].(map[string]interface{})["processInstanceId"],
		"operator":          "user1", "actorIds": "cc_c, cc_d",
	})
	mustOk(t, r2)
	if len(ccEvents) != 4 {
		t.Fatalf("manual createCCInstance 后 want 4 CC_CREATE, got %d: %+v", len(ccEvents), ccEvents)
	}
	for i, a := range []string{"cc_c", "cc_d"} {
		if ccEvents[2+i].CcActorID != a {
			t.Fatalf("manual CC_CREATE[%d] CcActorID=%q want %q", i, ccEvents[2+i].CcActorID, a)
		}
	}
}

// TestCCCreateEventNoListenerPureIncremental 纯增量：无监听器装配时
// cc 实例照常落库、FireEvent 零副作用、不抛错（与上一版逐字节同行为，T0 断言）。
func TestCCCreateEventNoListenerPureIncremental(t *testing.T) {
	repo := memory.New()
	extRepo := memory.NewExt()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	// 注意：不 SetExtensions —— 未装配任何监听器
	f := facade.New(eng, repo, extRepo)

	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	// 带抄送人发起：无监听器 → 不 panic（FireEvent 对 ext==nil 直接 return）
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"],
		"operator":        "user1", "f_ccActors": "cc_only",
	})
	mustOk(t, r1)
	// cc 实例照常落库（纯增量：cc 落库路径与上一版一致，不受 fire 影响）
	instID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	rows, total, _ := repo.PageCcInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 10}, "cc_only")
	if total < 1 || len(rows) == 0 {
		t.Fatalf("无监听器时 cc 实例仍应落库: total=%d", total)
	}
	// 直接 FireEvent：无监听器零副作用、不抛错
	eng.FireEvent(engine.ProcessEvent{Type: engine.EventCCCreate, InstanceID: instID, CcActorID: "cc_only"})
}

// ═══ 撤回鉴权 / 转办（issues/114 · issues/115，spec 06 §processInstance/withdraw · §processTask/transfer）═══

// mustFailWithMsg 断言门面失败：code=99999999（引擎侧统一失败码）+ msg 含跨栈统一文案关键字
func mustFailWithMsg(t *testing.T, label string, r map[string]interface{}, keyword string) {
	t.Helper()
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s 应失败且 code=99999999（禁止静默成功）, got %v", label, r)
	}
	msg, _ := r["msg"].(string)
	if !strings.Contains(msg, keyword) {
		t.Fatalf("%s msg = %q, want 含「%s」", label, msg, keyword)
	}
}

// varNum 读回变量里的数字（内存仓为 int，SQL 仓 variable JSON 反序列化为 float64）
func varNum(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		x, _ := strconv.ParseInt(n, 10, 64)
		return x
	}
	return -1
}

// histTaskByName 实例下指定节点的任务行（含已完成；同名多任务取首条），无则 nil
func histTaskByName(t *testing.T, repo *memory.Repository, instanceID int64, taskName string) *model.ProcessTask {
	t.Helper()
	his, err := repo.FindHistoryTasks(context.Background(), instanceID)
	if err != nil {
		t.Fatalf("FindHistoryTasks: %v", err)
	}
	for _, tk := range his {
		if tk.TaskName == taskName {
			return tk
		}
	}
	return nil
}

// doingTaskOfActor 实例下「参与者含 actor」的进行中任务 id（会签逐人任务定位用）
func doingTaskOfActor(t *testing.T, repo *memory.Repository, instanceID int64, actor string) int64 {
	t.Helper()
	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if actors, _ := repo.FindTaskActors(context.Background(), tk.ID); containsStr2(actors, actor) {
			return tk.ID
		}
	}
	return 0
}

// todoIDsOf 某人待办（走门面 todoList，即用户视角）的任务 id 列表
func todoIDsOf(t *testing.T, f *facade.Facade, operator string) []int64 {
	t.Helper()
	r := f.Flow("processTask/todoList", map[string]interface{}{"operator": operator, "pageSize": 100})
	mustOk(t, r)
	rows, _ := r["data"].(map[string]interface{})["rows"].([]interface{})
	ids := make([]int64, 0, len(rows))
	for _, m := range rows {
		ids = append(ids, mustI64(m.(map[string]interface{})["id"]))
	}
	return ids
}

// containsI64 数字列表包含判定
func containsI64(list []int64, v int64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// recordRowWithVar 审批记录里 ext.<key> == want 的首行（Go 栈审批记录即任务行历史）
func recordRowWithVar(t *testing.T, f *facade.Facade, instanceID int64, key, want string) map[string]interface{} {
	t.Helper()
	r := f.Flow("processInstance/approvalRecord", map[string]interface{}{"id": instanceID})
	mustOk(t, r)
	rows, _ := r["data"].([]interface{})
	for _, m := range rows {
		row, _ := m.(map[string]interface{})
		ext, _ := row["ext"].(map[string]interface{})
		if s, _ := ext[key].(string); s == want {
			return row
		}
	}
	t.Fatalf("审批记录里找不到 ext.%s == %q 的行: %v", key, want, rows)
	return nil
}

// TestWithdrawAuthorization spec 08 用例 24：撤回鉴权（operator 硬必填 + 三条归属判据 + update_user 回写）
//
// 断言一律落在**持久值/读回值**上（issues/113 的教训：只断"doing 列表为空"时 30 与 99 都满足）。
func TestWithdrawAuthorization(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "02-multi-task.json"))})
	mustOk(t, r0)
	defineID := r0["data"].(map[string]interface{})["processDefineId"]
	// 发起并自动完成 apply → 停 task1（参与者 leader，发起人 zhangsan 不在参与者里）
	start := func() int64 {
		r := f.Flow("processInstance/startAndExecute", map[string]interface{}{"processDefineId": defineID, "operator": "zhangsan"})
		mustOk(t, r)
		return mustI64(r["data"].(map[string]interface{})["processInstanceId"])
	}
	// 读回"未被撤回"的原状（负向用例的零副作用证据）
	assertUntouched := func(instanceID int64, label string) {
		t.Helper()
		inst, err := repo.FindInstanceByID(ctx, instanceID)
		if err != nil || inst == nil {
			t.Fatalf("%s 实例读回失败: %v", label, err)
		}
		if inst.State != model.InstanceStateDoing {
			t.Fatalf("%s 实例态 = %d, want 仍 DOING(10)", label, inst.State)
		}
		if inst.UpdateUser == "user1" {
			t.Fatalf("%s 实例 update_user 被记成 user1（operator 缺省回落缺陷）: %q", label, inst.UpdateUser)
		}
		tk := histTaskByName(t, repo, instanceID, "task1")
		if tk == nil {
			t.Fatalf("%s task1 任务行读不到", label)
		}
		if tk.TaskState != model.TaskStateDoing {
			t.Fatalf("%s task1 任务态 = %d, want 仍 DOING(10)", label, tk.TaskState)
		}
	}

	// ── 负向①：operator 缺失 / 空串 → 明确报错，严禁回落 user1 ──
	instA := start()
	mustFailWithMsg(t, "撤回缺 operator", f.Flow("processInstance/withdraw", map[string]interface{}{"id": instA}), "operator 必填")
	mustFailWithMsg(t, "撤回空 operator",
		f.Flow("processInstance/withdraw", map[string]interface{}{"id": instA, "operator": "   "}), "operator 必填")
	assertUntouched(instA, "缺 operator")

	// ── 负向②：无关第三人（既非发起人，也不是任何进行中任务的参与者）──
	mustFailWithMsg(t, "无关第三人撤回",
		f.Flow("processInstance/withdraw", map[string]interface{}{"id": instA, "operator": "stranger"}),
		"无权限撤回该流程实例")
	assertUntouched(instA, "第三人撤回")

	// ── 正向①（判据 2 参与者可撤整单）+ update_user 回写真实撤回人 + 已完成任务不被改写 ──
	mustOk(t, f.Flow("processInstance/withdraw", map[string]interface{}{"id": instA, "operator": "leader"}))
	inst, _ := repo.FindInstanceByID(ctx, instA)
	if inst == nil || inst.State != model.InstanceStateWithdraw {
		t.Fatalf("参与者撤回后实例态 = %+v, want Withdraw(30)", inst)
	}
	if inst.UpdateUser != "leader" {
		t.Fatalf("实例 update_user = %q, want 真实撤回人 leader", inst.UpdateUser)
	}
	doneTask := histTaskByName(t, repo, instA, "apply")
	if doneTask == nil {
		t.Fatalf("apply 任务行读不到")
	}
	t1, _ := repo.FindTaskByID(ctx, histTaskByName(t, repo, instA, "task1").ID)
	if t1.TaskState != model.TaskStateWithdraw {
		t.Fatalf("撤回任务态 = %d, want 30（WITHDRAW；99 是废弃码）", t1.TaskState)
	}
	if t1.UpdateUser != "leader" {
		t.Fatalf("撤回任务 update_user = %q, want leader", t1.UpdateUser)
	}
	// 已完成(20) 的 apply 行：状态与 update_user 都不得被撤回改写
	if doneTask.TaskState != model.TaskStateDone || doneTask.UpdateUser == "leader" {
		t.Fatalf("已完成任务被撤回改写: state=%d update_user=%q", doneTask.TaskState, doneTask.UpdateUser)
	}
	// 撤回后不残留进行中任务（读回值，非"列表为空"这一弱形状）
	if doing, _ := repo.FindDoingTasks(ctx, instA, nil); len(doing) != 0 {
		t.Fatalf("撤回后仍剩 %d 条 doing 任务", len(doing))
	}

	// ── 正向②（判据 1 发起人）：zhangsan 不在任何进行中任务的参与者里，
	// 若实现直接复用引擎 isAllowed（只判 actorIds + auto/admin），这一支必红 ──
	instB := start()
	if actors, _ := repo.FindTaskActors(ctx, doingTaskID(t, repo, instB, "task1")); containsStr2(actors, "zhangsan") {
		t.Fatalf("前置被破坏：发起人成了 task1 参与者，判据 1 断言将恒真: %v", actors)
	}
	mustOk(t, f.Flow("processInstance/withdraw", map[string]interface{}{"id": instB, "operator": "zhangsan"}))
	if got, _ := repo.FindInstanceByID(ctx, instB); got.State != model.InstanceStateWithdraw || got.UpdateUser != "zhangsan" {
		t.Fatalf("发起人撤回后实例 = %+v, want state 30 + update_user zhangsan", got)
	}

	// ── 正向③（判据 3）：flow.admin / flow.auto 放行 ──
	for _, op := range []string{"flow.admin", "flow.auto"} {
		instC := start()
		mustOk(t, f.Flow("processInstance/withdraw", map[string]interface{}{"id": instC, "operator": op}))
		got, _ := repo.FindInstanceByID(ctx, instC)
		if got.State != model.InstanceStateWithdraw {
			t.Fatalf("%s 撤回后实例态 = %d, want 30", op, got.State)
		}
	}

	// ── 会签整单：任一参与者撤整单，其余成员的进行中任务同样落 30（不是只撤自己那一条）──
	r5 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "05-countersign-parallel.json"))})
	mustOk(t, r5)
	r6 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r5["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r6)
	csID := mustI64(r6["data"].(map[string]interface{})["processInstanceId"])
	csDoing, _ := repo.FindDoingTasks(ctx, csID, nil)
	if len(csDoing) != 3 {
		t.Fatalf("并行会签应有 3 条 doing 任务, got %d", len(csDoing))
	}
	mustOk(t, f.Flow("processInstance/withdraw", map[string]interface{}{"id": csID, "operator": "userB"}))
	for _, tk := range csDoing {
		stored, _ := repo.FindTaskByID(ctx, tk.ID)
		if stored.TaskState != model.TaskStateWithdraw {
			t.Fatalf("整单撤回后会签任务 %s(%d) 态 = %d, want 30", stored.TaskName, stored.ID, stored.TaskState)
		}
		if stored.UpdateUser != "userB" {
			t.Fatalf("整单撤回后会签任务 update_user = %q, want userB", stored.UpdateUser)
		}
	}
}

// TestTaskTransfer spec 08 用例 25：转办（摘原人一行 + 换新人 + submitType=7 留痕 + 四条明确报错）
func TestTaskTransfer(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskID := doingTaskID(t, repo, instanceID, "task1")
	if taskID == 0 {
		t.Fatalf("前置：应有 task1 进行中任务")
	}
	transfer := func(args map[string]interface{}) map[string]interface{} {
		return f.Flow("processTask/transfer", args)
	}

	// ── 回归前置：加签（surrogate）仍是"只追加"，原参与人保留可办 ──
	mustOk(t, f.Flow("processTask/surrogate", map[string]interface{}{
		"processTaskId": taskID, "actorIds": []string{"coworker"},
	}))
	base, _ := repo.FindTaskActors(ctx, taskID)
	if !containsStr2(base, "leader") || !containsStr2(base, "coworker") {
		t.Fatalf("加签后参与人 = %v, want 原人 leader 保留 + coworker", base)
	}

	// ── 负向：四类明确报错 + 报错零副作用（参与人、留痕都不动）──
	mustFailWithMsg(t, "转办缺 operator",
		transfer(map[string]interface{}{"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer"}),
		"operator 必填")
	mustFailWithMsg(t, "第三人转别人的待办",
		transfer(map[string]interface{}{"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer", "operator": "stranger"}),
		"无权限转办该任务")
	mustFailWithMsg(t, "fromActor 非参与者",
		transfer(map[string]interface{}{"processTaskId": taskID, "fromActor": "nobody", "toActor": "newcomer", "operator": "nobody"}),
		"原办理人不是该任务参与人")
	mustFailWithMsg(t, "toActor 已是参与者",
		transfer(map[string]interface{}{"processTaskId": taskID, "fromActor": "leader", "toActor": "coworker", "operator": "leader"}),
		"目标人已是该任务参与人")
	after, _ := repo.FindTaskActors(ctx, taskID)
	if !reflect.DeepEqual(after, base) {
		t.Fatalf("报错后参与人被改动: before=%v after=%v", base, after)
	}
	if tk, _ := repo.FindTaskByID(ctx, taskID); tk.Variables["tf_transferTo"] != nil ||
		varNum(tk.Variables["submitType"]) == varNum(int(model.SubmitTypeTransfer)) {
		t.Fatalf("报错却写入了转办留痕: %+v", tk.Variables)
	}

	// ── 正向：leader 转给 newcomer（只摘自己那一行，coworker 不动），沿用同一 taskId ──
	mustOk(t, transfer(map[string]interface{}{
		"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer",
		"reason": "出差三天", "operator": "leader",
	}))
	swapped, _ := repo.FindTaskActors(ctx, taskID)
	if containsStr2(swapped, "leader") || !containsStr2(swapped, "newcomer") || !containsStr2(swapped, "coworker") {
		t.Fatalf("转办后参与人 = %v, want 摘 leader、留 coworker、加 newcomer", swapped)
	}
	if got := todoIDsOf(t, f, "leader"); containsI64(got, taskID) {
		t.Fatalf("转办后 A 待办仍含该任务: %v", got)
	}
	gotB := todoIDsOf(t, f, "newcomer")
	if !containsI64(gotB, taskID) {
		t.Fatalf("转办后 B 待办应含同一 taskId(%d): %v", taskID, gotB)
	}

	// ── 留痕读回：审批记录出现 submitType=7，tf_transferTo/tf_transferReason 可取 ──
	row := recordRowWithVar(t, f, instanceID, "tf_transferTo", "newcomer")
	ext := row["ext"].(map[string]interface{})
	if got := varNum(ext["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("审批记录 submitType = %d, want 7（TRANSFER）", got)
	}
	if s, _ := ext["tf_transferReason"].(string); s != "出差三天" {
		t.Fatalf("审批记录 tf_transferReason = %v, want 出差三天", ext["tf_transferReason"])
	}
	// 契约 06 §transfer 留痕⚠️（Node 实测复现）：转办**严禁覆写 actor_id 列**——进行中任务
	// 该列恒无值是既有不变量，"办理人记谁"由 update_user + 账本 operator 承载
	tk, _ := repo.FindTaskByID(ctx, taskID)
	if tk.ActorID != "" {
		t.Fatalf("转办后进行中任务 actor_id = %q, want 恒无值（覆写会致撤回单冒进「我已办」）", tk.ActorID)
	}
	if tk.UpdateUser != "leader" {
		t.Fatalf("转办留痕 update_user = %v, want 操作人 leader", tk.UpdateUser)
	}
	if hop := ledgerOf(t, ext["tf_transferHistory"]); len(hop) != 1 || hop[0]["operator"] != "leader" {
		t.Fatalf("账本 tf_transferHistory = %v, want 1 条且 operator=leader（办理人经账本承载）", ext["tf_transferHistory"])
	}
	// 高亮/节点进度不变：任务仍在 task1 节点上活跃
	hl := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	mustOk(t, hl)
	if acts, _ := hl["data"].(map[string]interface{})["activeNodeNames"].([]interface{}); !containsStr2(toStringSlice2Any(acts), "task1") {
		t.Fatalf("转办后高亮活跃节点应仍含 task1: %v", acts)
	}

	// ── 回归：接手人正常办结，其 submitType 不被留痕顶掉（记录与会签否决判据都依赖它）──
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": taskID, "operator": "newcomer", "submitType": 1,
	}))
	done, _ := repo.FindTaskByID(ctx, taskID)
	if done.TaskState != model.TaskStateDone {
		t.Fatalf("B 办结后任务态 = %d, want 20", done.TaskState)
	}
	if got := varNum(done.Variables["submitType"]); got != 1 {
		t.Fatalf("B 办结记录 submitType = %d, want 1（留痕 7 不得反噬后续提交）", got)
	}
	if done.ActorID != "newcomer" {
		t.Fatalf("B 办结后办理人 = %q, want newcomer", done.ActorID)
	}
	// 转办证据仍留在同一任务行上（tf_transferTo 不在提交参数里，不被覆盖）
	if s, _ := done.Variables["tf_transferTo"].(string); s != "newcomer" {
		t.Fatalf("办结后转办证据丢失: %v", done.Variables["tf_transferTo"])
	}
	if inst, _ := repo.FindInstanceByID(ctx, instanceID); inst.State != model.InstanceStateDone {
		t.Fatalf("实例态 = %d, want 已完成(20)", inst.State)
	}

	// ── 负向：已完成任务不可转办 ──
	mustFailWithMsg(t, "已完成任务转办",
		transfer(map[string]interface{}{"processTaskId": taskID, "fromActor": "newcomer", "toActor": "third", "operator": "newcomer"}),
		"任务非进行中，不可转办")

	// ── flow.admin 代转（鉴权例外）+ 会签只摘 fromActor 那一行 ──
	r2 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "05-countersign-parallel.json"))})
	mustOk(t, r2)
	r3 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r2["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r3)
	csID := mustI64(r3["data"].(map[string]interface{})["processInstanceId"])
	taskA := doingTaskOfActor(t, repo, csID, "userA")
	taskB := doingTaskOfActor(t, repo, csID, "userB")
	if taskA == 0 || taskB == 0 || taskA == taskB {
		t.Fatalf("前置：会签逐人任务定位失败 A=%d B=%d", taskA, taskB)
	}
	mustOk(t, transfer(map[string]interface{}{
		"processTaskId": taskA, "fromActor": "userA", "toActor": "userD", "operator": "flow.admin",
	}))
	actorsA, _ := repo.FindTaskActors(ctx, taskA)
	if !reflect.DeepEqual(actorsA, []string{"userD"}) {
		t.Fatalf("管理员代转后本人任务参与人 = %v, want 只剩 userD", actorsA)
	}
	actorsB, _ := repo.FindTaskActors(ctx, taskB)
	if !reflect.DeepEqual(actorsB, []string{"userB"}) {
		t.Fatalf("同会签节点其他成员任务被改动: %v, want userB 不动", actorsB)
	}
	if got := todoIDsOf(t, f, "userA"); containsI64(got, taskA) {
		t.Fatalf("转办后 userA 待办仍含该票: %v", got)
	}
	if got := todoIDsOf(t, f, "userD"); !containsI64(got, taskA) {
		t.Fatalf("转办后 userD 待办应含该票(%d): %v", taskA, got)
	}
}

// TestTaskTransferKeepsOperatorColumnEmpty 契约 06 §transfer 留痕⚠️（spec 08 用例 25 延伸）：
// 转办严禁覆写任务 actor_id 列——进行中该列恒无值是既有不变量；把被摘走的人写进去，
// 该单一旦撤回（离开 DOING 但列值留着），会凭空出现在他从没办过的「我已办」列表
// （pageDoneTasks 判据 state <> 10 AND operator = ?，Node 实测复现）。断言全落持久值/读回值。
func TestTaskTransferKeepsOperatorColumnEmpty(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskID := doingTaskID(t, repo, instanceID, "task1")
	if taskID == 0 {
		t.Fatalf("前置：应有 task1 进行中任务")
	}
	mustOk(t, f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": taskID, "fromActor": "leader", "toActor": "newcomer",
		"reason": "出差三天", "operator": "leader",
	}))
	// 读回持久任务行：actor_id 列仍无值（不变量），办理人经 update_user + 账本 operator 承载
	tk, _ := repo.FindTaskByID(ctx, taskID)
	if tk.ActorID != "" {
		t.Fatalf("转办后 actor_id 列 = %q, want 恒无值", tk.ActorID)
	}
	if tk.UpdateUser != "leader" {
		t.Fatalf("转办后 update_user = %q, want leader", tk.UpdateUser)
	}

	// 撤回整单（发起人 zhangsan，判据 1）：task1 离开 DOING(→30)，operator 列保持无值
	mustOk(t, f.Flow("processInstance/withdraw", map[string]interface{}{"id": instanceID, "operator": "zhangsan"}))
	withdrawn, _ := repo.FindTaskByID(ctx, taskID)
	if withdrawn.TaskState != model.TaskStateWithdraw {
		t.Fatalf("撤回后任务态 = %d, want 30（撤回未生效则本用例失去意义）", withdrawn.TaskState)
	}
	if withdrawn.ActorID != "" {
		t.Fatalf("撤回后 actor_id 列 = %q, want 恒无值", withdrawn.ActorID)
	}
	// 被摘走的 leader 与未办的 newcomer 的「我已办」都不含该单（冒单即本缺陷的实证形态）
	doneIDs := func(operator string) []int64 {
		r := f.Flow("processTask/doneList", map[string]interface{}{"operator": operator, "pageSize": 100})
		mustOk(t, r)
		rows, _ := r["data"].(map[string]interface{})["rows"].([]interface{})
		ids := make([]int64, 0, len(rows))
		for _, m := range rows {
			ids = append(ids, mustI64(m.(map[string]interface{})["id"]))
		}
		return ids
	}
	for _, who := range []string{"leader", "newcomer"} {
		if got := doneIDs(who); containsI64(got, taskID) {
			t.Fatalf("转办→撤回后 %s 的已办列表冒入该单（他从没办过）: %v", who, got)
		}
	}
}

// transferTimeLayout tf_transferHistory.time 的落地格式：写字符串而非可序列化为对象的时刻，
// 保证跨 JSON 往返（内存仓 → SQL 仓）形状稳定，七栈对齐时按同一格式写入。
const transferTimeLayout = "2006-01-02 15:04:05"

// ledgerOf 归一 tf_transferHistory 的读出形态：内存仓为本仓写入的 []interface{}，SQL 仓为 variable
// 列 JSON 反序列化的 []interface{}（元素同为 map[string]interface{}）；另兼容 []map 形状。
// 元素不是对象、整体不是数组即 Fail——钉住跨栈对齐用的落地形状。
func ledgerOf(t *testing.T, v interface{}) []map[string]interface{} {
	t.Helper()
	switch list := v.(type) {
	case nil:
		return nil
	case []map[string]interface{}:
		return list
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(list))
		for i, e := range list {
			m, ok := e.(map[string]interface{})
			if !ok {
				t.Fatalf("tf_transferHistory[%d] 应为对象，实际 %T", i, e)
			}
			out = append(out, m)
		}
		return out
	}
	t.Fatalf("tf_transferHistory 落地形状异常: %T", v)
	return nil
}

// checkHop 逐字段核对账本里的一跳（不做整片 DeepEqual：内存仓 int / SQL 仓 float64 不同形）
func checkHop(t *testing.T, idx int, rec map[string]interface{}, from, to, reason, op string) {
	t.Helper()
	if got := varNum(rec["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("第 %d 跳 submitType = %d, want 7（TRANSFER）", idx, got)
	}
	for _, kv := range []struct{ key, want string }{
		{"fromActor", from}, {"toActor", to}, {"reason", reason}, {"operator", op},
	} {
		if s, _ := rec[kv.key].(string); s != kv.want {
			t.Fatalf("第 %d 跳 %s = %v, want %q", idx, kv.key, rec[kv.key], kv.want)
		}
	}
	s, _ := rec["time"].(string)
	if _, err := time.Parse(transferTimeLayout, s); err != nil {
		t.Fatalf("第 %d 跳 time = %v, want 「%s」格式（跨 JSON 往返形状稳定）: %v", idx, rec["time"], transferTimeLayout, err)
	}
}

// TestTaskTransferMultiHopLedger spec 06 §processTask/transfer 留痕②③（契约 fc0883a 改约三件）：
// A→B、B→C 两跳后 C 办结——tf_transferHistory 必须仍是两条且逐字段正确（只追加不覆盖，办结后存活），
// submitType 槽位被 C 自己的 1 覆盖属预期，末跳文案落 tf_approvalComment（前端审批意见读取位 issues/15）。
func TestTaskTransferMultiHopLedger(t *testing.T) {
	f, repo, _ := setupFacade()
	ctx := context.Background()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": r0["data"].(map[string]interface{})["processDefineId"], "operator": "zhangsan",
	})
	mustOk(t, r1)
	instanceID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskID := doingTaskID(t, repo, instanceID, "task1")
	if taskID == 0 {
		t.Fatalf("前置：应有 task1 进行中任务")
	}
	transfer := func(from, to, reason, operator string) {
		mustOk(t, f.Flow("processTask/transfer", map[string]interface{}{
			"processTaskId": taskID, "fromActor": from, "toActor": to,
			"reason": reason, "operator": operator,
		}))
	}

	transfer("leader", "newcomer", "出差三天", "leader")
	transfer("newcomer", "third", "不熟悉该业务", "newcomer")

	// ── 两跳后（C 尚未办结）：同一任务行上账本两条 + 末跳便捷键 + 末跳可读文案 + 槽位仍读作转办 ──
	hopped, _ := repo.FindTaskByID(ctx, taskID)
	recs := ledgerOf(t, hopped.Variables["tf_transferHistory"])
	if len(recs) != 2 {
		t.Fatalf("两跳后 tf_transferHistory = %d 条, want 2（B 再转给 C 不得抹掉 A→B 那条）", len(recs))
	}
	checkHop(t, 1, recs[0], "leader", "newcomer", "出差三天", "leader")
	checkHop(t, 2, recs[1], "newcomer", "third", "不熟悉该业务", "newcomer")
	if s, _ := hopped.Variables["tf_transferTo"].(string); s != "third" {
		t.Fatalf("末跳便捷键 tf_transferTo = %v, want third", hopped.Variables["tf_transferTo"])
	}
	if s, _ := hopped.Variables["tf_approvalComment"].(string); s != "newcomer 转办给 third（不熟悉该业务）" {
		t.Fatalf("末跳文案 tf_approvalComment = %q, want「newcomer 转办给 third（不熟悉该业务）」", s)
	}
	if got := varNum(hopped.Variables["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("办结前当前槽位 submitType = %d, want 7（转办后、办结前审批记录直接读作转办）", got)
	}
	// 任务不新建：两跳后仍是同一 taskId 的进行中任务，参与人已换成 C
	if actors, _ := repo.FindTaskActors(ctx, taskID); !reflect.DeepEqual(actors, []string{"third"}) {
		t.Fatalf("两跳后参与人 = %v, want 只剩 third", actors)
	}

	// ── C 办结（并自填审批意见）：槽位被 1 覆盖属预期、账本两条原样存活、意见槽位本次提交优先 ──
	mustOk(t, f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": taskID, "operator": "third", "submitType": 1,
		"tf_approvalComment": "已核对，同意",
	}))
	done, _ := repo.FindTaskByID(ctx, taskID)
	if done.TaskState != model.TaskStateDone {
		t.Fatalf("C 办结后任务态 = %d, want 20", done.TaskState)
	}
	if got := varNum(done.Variables["submitType"]); got != int64(model.SubmitTypeAgree) {
		t.Fatalf("C 办结后 submitType = %d, want 1（末跳槽位被办理参数覆盖属预期）", got)
	}
	kept := ledgerOf(t, done.Variables["tf_transferHistory"])
	if len(kept) != 2 {
		t.Fatalf("办结后 tf_transferHistory = %d 条, want 2（无追加式账本则转办事实整体消失、审计断链）", len(kept))
	}
	checkHop(t, 1, kept[0], "leader", "newcomer", "出差三天", "leader")
	checkHop(t, 2, kept[1], "newcomer", "third", "不熟悉该业务", "newcomer")
	if s, _ := done.Variables["tf_approvalComment"].(string); s != "已核对，同意" {
		t.Fatalf("C 自填意见被转办留痕反噬: tf_approvalComment = %q, want「已核对，同意」（spec 留痕前置条件：本次提交参数最高）", s)
	}

	// ── 前端读取位：approvalRecord 的 ext 直接取得账本与意见（issues/15）──
	row := recordRowWithVar(t, f, instanceID, "tf_transferTo", "third")
	ext := row["ext"].(map[string]interface{})
	if s, _ := ext["tf_approvalComment"].(string); s != "已核对，同意" {
		t.Fatalf("审批记录 ext.tf_approvalComment = %v, want「已核对，同意」", ext["tf_approvalComment"])
	}
	if n := len(ledgerOf(t, ext["tf_transferHistory"])); n != 2 {
		t.Fatalf("审批记录 ext.tf_transferHistory = %d 条, want 2: %v", n, ext["tf_transferHistory"])
	}
}

// toStringSlice2Any 审批记录/高亮数组（[]interface{} of string）转字符串列表
func toStringSlice2Any(list []interface{}) []string {
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
