// 委托代理运行期自动生效（issues/116，规范 06 §4.5 / 08 用例 26）——内存仓路径。
//
// 断言一律落在**读回的持久值**（repo.FindTaskActors，即 wf_process_task_actor 的内存态）
// 上，而不是"doing 列表为空"这类 30/99 都能满足的形状（issues/113 教训）。
package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 测试 SPI ──────────────────────────────────────────────────────────────────

type sUserProv struct{}

func (sUserProv) GetUser(string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: "u", RealName: "u"}, nil
}

type sIDGen struct{ n int64 }

func (g *sIDGen) NextID() int64 { g.n++; return g.n }

// 三个流程：单参与者 / 多参与者 / 并行会签（节点 assignee 决定参与者）
const (
	flowSingle = `{"name":"surr116","displayName":"委托生效流程","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"form":"f1","assignee":"zhangsan","taskType":0,"performType":0},"text":{"value":"审批"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`

	flowMulti = `{"name":"surrmulti116","displayName":"多参与者委托","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"assignee":"zhangsan,wangwu","taskType":0,"performType":0},"text":{"value":"会审"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`

	flowCountersign = `{"name":"surrcs116","displayName":"会签委托","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"assignee":"zhangsan,wangwu","taskType":0,"performType":"1","countersignType":"PARALLEL"},"text":{"value":"会签"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`
)

// newHarness 装配内存主仓 + 引擎。ext 非空即注入引擎（＝默认开启委托自动生效）；
// 传 nil 覆盖"未配置扩展仓储"形态。opts 用于追加开关等装配。
func newHarness(t *testing.T, name, content string, ext spi.ProcessExtRepository, opts ...engine.Option) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "委托测试", Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	if ext != nil {
		opts = append([]engine.Option{engine.WithSurrogateRepository(ext)}, opts...)
	}
	return engine.New(repo, sUserProv{}, &sIDGen{}, nil, opts...), repo, def.ID
}

// startOneTaskActor 发起流程并返回**读回**的首个进行中任务参与者
func startOneTaskActor(t *testing.T, eng *engine.EngineImpl, repo *memory.Repository, defineID int64) []string {
	t.Helper()
	inst, err := eng.StartProcessInstanceByID(context.Background(), defineID, "boss1", map[string]interface{}{"BUSINESS_NO": "BIZ-116"})
	if err != nil {
		t.Fatalf("建单被打断: %v", err)
	}
	tasks, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("doing 任务 = %d err=%v, want 1", len(tasks), err)
	}
	actors, err := repo.FindTaskActors(context.Background(), tasks[0].ID)
	if err != nil {
		t.Fatalf("读回参与者: %v", err)
	}
	return actors
}

func putSurr(t *testing.T, ext spi.ProcessExtRepository, operator, surrogate, processName string, start, end *time.Time, enabled int) {
	t.Helper()
	if err := ext.SaveSurrogate(context.Background(), &model.ProcessSurrogate{
		Operator: operator, Surrogate: surrogate, ProcessName: processName,
		StartTime: start, EndTime: end, Enabled: enabled,
	}); err != nil {
		t.Fatalf("save surrogate: %v", err)
	}
}

func ptrOf(ts time.Time) *time.Time { return &ts }

func countOf(list []string, want string) int {
	n := 0
	for _, v := range list {
		if v == want {
			n++
		}
	}
	return n
}

// ─── ① 正向：窗口内委托 → 代理人进参与者表，授权人保留 ─────────────────────────

func TestSurrogateAutoApplyAppendsActor(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surr116", flowSingle, ext)
	now := time.Now()
	putSurr(t, ext, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	if !eng.SurrogateAutoApply() {
		t.Fatalf("委托自动生效应默认开启（引擎内置）")
	}
	actors := startOneTaskActor(t, eng, repo, defID)
	if countOf(actors, "zhangsan") != 1 {
		t.Fatalf("授权人必须保留且只一行（委托不是转办）: %v", actors)
	}
	if countOf(actors, "lisi") != 1 {
		t.Fatalf("被委托人未真落参与者表，读回 %v", actors)
	}
	if len(actors) != 2 {
		t.Fatalf("参与者 = %v, want [zhangsan lisi]", actors)
	}
	// 台账未被运行期改写
	if _, total, err := ext.PageSurrogates(context.Background(), spi.PageQuery{
		PageNum: 1, PageSize: 10, Filters: map[string]interface{}{"operator": "zhangsan"}}); err != nil || total != 1 {
		t.Fatalf("委托台账应仍只有 1 条: total=%d err=%v", total, err)
	}
}

// ─── ② 负向：窗外 / enabled 非 1 / 自委托 → 无代理人行 ─────────────────────────

func TestSurrogateAutoApplyNegativeCases(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name, operator, agent, process string
		start, end                     *time.Time
		enabled                        int
	}{
		{"窗外-已过期", "zhangsan", "lisi", "surr116", ptrOf(now.Add(-10 * time.Hour)), ptrOf(now.Add(-9 * time.Hour)), 1},
		{"窗外-未开始", "zhangsan", "lisi", "surr116", ptrOf(now.Add(9 * time.Hour)), nil, 1},
		{"enabled=0", "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 0},
		{"enabled=2 非 1 值", "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 2},
		{"自委托", "zhangsan", "zhangsan", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1},
		{"流程名不符且非兜底行", "zhangsan", "lisi", "other-flow", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ext := memory.NewExt()
			eng, repo, defID := newHarness(t, "surr116", flowSingle, ext)
			putSurr(t, ext, c.operator, c.agent, c.process, c.start, c.end, c.enabled)
			// 种子自证：台账里确有这一条，否则"李四无行"会因为根本没数据而空转
			if back, _ := ext.FindSurrogateByID(context.Background(), 1); back == nil || back.Surrogate != c.agent {
				t.Fatalf("委托台账未落库（读回 %+v），负向断言无意义", back)
			}
			actors := startOneTaskActor(t, eng, repo, defID)
			if len(actors) != 1 || actors[0] != "zhangsan" {
				t.Fatalf("%s：不应并入代理人，读回 %v", c.name, actors)
			}
		})
	}
}

// 空 processName 全流程兜底在运行期同样生效（判据 a）
func TestSurrogateAutoApplyAllFlowFallback(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surr116", flowSingle, ext)
	now := time.Now()
	putSurr(t, ext, "zhangsan", "lisi", "", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	actors := startOneTaskActor(t, eng, repo, defID)
	if countOf(actors, "lisi") != 1 {
		t.Fatalf("全流程委托（processName 空）应兜底生效，读回 %v", actors)
	}
}

// ─── ③ 未配置扩展仓储：静默跳过，不打断建单 ───────────────────────────────────

func TestSurrogateNoExtRepoDoesNotBreakStart(t *testing.T) {
	eng, repo, defID := newHarness(t, "surr116", flowSingle, nil) // 完全不注入
	actors := startOneTaskActor(t, eng, repo, defID)
	if len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("未配置扩展仓储应只落授权人，读回 %v", actors)
	}
}

// 显式注入 nil（集成方传 nil 的姿势）同样静默跳过
func TestSurrogateNilRepoInjectedSilentlySkips(t *testing.T) {
	eng, repo, defID := newHarness(t, "surr116", flowSingle, nil, engine.WithSurrogateRepository(nil))
	if actors := startOneTaskActor(t, eng, repo, defID); len(actors) != 1 {
		t.Fatalf("注入 nil 应静默跳过，读回 %v", actors)
	}
}

// ─── ④ 显式关闭：回到仅台账 ───────────────────────────────────────────────────

func TestSurrogateExplicitDisable(t *testing.T) {
	now := time.Now()

	// Option 路
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surr116", flowSingle, ext, engine.WithSurrogateAutoApply(false))
	putSurr(t, ext, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	if eng.SurrogateAutoApply() {
		t.Fatalf("WithSurrogateAutoApply(false) 后开关应为关闭")
	}
	actors := startOneTaskActor(t, eng, repo, defID)
	if len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("显式关闭后不应并入代理人，读回 %v", actors)
	}
	// 关闭只影响运行期，台账照常可查
	if _, total, err := ext.PageSurrogates(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 10}); err != nil || total != 1 {
		t.Fatalf("关闭运行期后台账应仍可查: total=%d err=%v", total, err)
	}

	// 运行期 setter 路（同一开关的链式写法）：关 → 开
	ext2 := memory.NewExt()
	eng2, repo2, defID2 := newHarness(t, "surr116", flowSingle, ext2)
	putSurr(t, ext2, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	eng2.SetSurrogateAutoApply(false)
	if a := startOneTaskActor(t, eng2, repo2, defID2); countOf(a, "lisi") != 0 {
		t.Fatalf("SetSurrogateAutoApply(false) 未关闭，读回 %v", a)
	}
	if !eng2.SetSurrogateAutoApply(true).SurrogateAutoApply() {
		t.Fatalf("SetSurrogateAutoApply(true) 应重新开启")
	}
	if a := startOneTaskActor(t, eng2, repo2, defID2); countOf(a, "lisi") != 1 {
		t.Fatalf("SetSurrogateAutoApply(true) 未重新开启，读回 %v", a)
	}
}

// ─── 回归：多参与者 / 并行会签 / 不级联 / 不重复 ───────────────────────────────

func TestSurrogateMultiActorNotPolluted(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surrmulti116", flowMulti, ext)
	now := time.Now()
	putSurr(t, ext, "zhangsan", "lisi", "surrmulti116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	actors := startOneTaskActor(t, eng, repo, defID)
	if len(actors) != 3 || countOf(actors, "zhangsan") != 1 || countOf(actors, "wangwu") != 1 || countOf(actors, "lisi") != 1 {
		t.Fatalf("多参与者任务参与者 = %v, want [zhangsan wangwu lisi] 各一行", actors)
	}
	if actors[0] != "zhangsan" {
		t.Fatalf("追加语义下原解析顺序不变（代理人追加在尾），读回 %v", actors)
	}
}

func TestSurrogateParallelCountersign(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surrcs116", flowCountersign, ext)
	now := time.Now()
	putSurr(t, ext, "zhangsan", "lisi", "surrcs116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "boss1", nil)
	if err != nil {
		t.Fatalf("并行会签建单被打断: %v", err)
	}
	tasks, _ := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if len(tasks) != 2 {
		t.Fatalf("并行会签任务数 = %d, want 2", len(tasks))
	}
	seen := map[string][]string{}
	for _, tk := range tasks {
		actors, _ := repo.FindTaskActors(context.Background(), tk.ID)
		if len(actors) == 0 {
			t.Fatalf("会签任务 %d 参与者表为空", tk.ID)
		}
		// 会签建任务只写参与者表（ProcessTask.ActorID 在完成时才落），按首参与者归组
		seen[actors[0]] = actors
	}
	if a := seen["zhangsan"]; len(a) != 2 || countOf(a, "lisi") != 1 {
		t.Fatalf("zhangsan 的会签任务应各自并入代理人，读回 %v", a)
	}
	if a := seen["wangwu"]; len(a) != 1 || a[0] != "wangwu" {
		t.Fatalf("wangwu 无委托却多出参与者，读回 %v", a)
	}
}

// 环委托不级联：A→B、B→A 时建单只并入 B（规范"每个 actor 查一次"）
func TestSurrogateDoesNotCascade(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surr116", flowSingle, ext)
	now := time.Now()
	putSurr(t, ext, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	putSurr(t, ext, "lisi", "zhangsan", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	actors := startOneTaskActor(t, eng, repo, defID)
	if len(actors) != 2 {
		t.Fatalf("委托不应级联展开（A→B、B→A 只并入 B），读回 %v", actors)
	}
}

// 扩展仓储查询报错时跳过该参与者，不打断建单
type failingExt struct {
	*memory.ExtRepository
	calls int
}

func (f *failingExt) GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error) {
	f.calls++
	return nil, context.DeadlineExceeded
}

func TestSurrogateQueryErrorDoesNotBreakStart(t *testing.T) {
	repo := memory.New()
	def := &model.ProcessDefine{Name: "surr116", DisplayName: "委托", Type: "approval", State: 1, Version: 1, Content: []byte(flowSingle)}
	repo.AddDefine(def)
	ext := &failingExt{ExtRepository: memory.NewExt()}
	eng := engine.New(repo, sUserProv{}, &sIDGen{}, nil, engine.WithSurrogateRepository(ext))
	actors := startOneTaskActor(t, eng, repo, def.ID)
	if ext.calls == 0 {
		t.Fatalf("未调用委托查询，用例空转")
	}
	if len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("委托查询失败应跳过而非打断建单，读回 %v", actors)
	}
}
