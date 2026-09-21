// 委托代理运行期自动生效（issues/116，规范 06 §4.5 / 08 用例 26）——内存仓路径。
//
// 断言一律落在**读回的持久值**（repo.FindTaskActors，即 wf_process_task_actor 的内存态）
// 上，而不是"doing 列表为空"这类 30/99 都能满足的形状（issues/113 教训）。
package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

// ─── 流程 JSON 构造器（条款 1.1 / 条款 1 覆盖范围的用例共用）────────────────────

// taskSpec 一个任务节点：id / assignee（逗号分隔多人）/ countersign
// （"" = 普通任务，"PARALLEL" / "SEQUENTIAL" = 会签）。
type taskSpec struct{ id, assignee, countersign string }

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// buildFlow 线性流程：start → specs… → end（节点 text 与 id 同值，TaskName 即节点 id）
func buildFlow(nameSeg string, specs ...taskSpec) string {
	nodes := []string{`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}}`}
	edges := []string{}
	prev := "start"
	for _, s := range specs {
		props := fmt.Sprintf(`"assignee":%s,"taskType":0,"performType":0`, jsonQuote(s.assignee))
		if s.countersign != "" {
			props = fmt.Sprintf(`"assignee":%s,"taskType":0,"performType":"1","countersignType":%s`,
				jsonQuote(s.assignee), jsonQuote(s.countersign))
		}
		nodes = append(nodes, fmt.Sprintf(`{"id":%s,"type":"snaker:task","properties":{%s},"text":{"value":%s}}`,
			jsonQuote(s.id), props, jsonQuote(s.id)))
		edges = append(edges, fmt.Sprintf(`{"id":"e_%s_%s","sourceNodeId":%s,"targetNodeId":%s,"properties":{}}`,
			prev, s.id, jsonQuote(prev), jsonQuote(s.id)))
		prev = s.id
	}
	nodes = append(nodes, `{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}`)
	edges = append(edges, fmt.Sprintf(`{"id":"e_%s_end","sourceNodeId":%s,"targetNodeId":"end","properties":{}}`,
		prev, jsonQuote(prev)))
	return fmt.Sprintf(`{%s"displayName":"委托测试","type":"approval","nodes":[%s],"edges":[%s]}`,
		nameSeg, strings.Join(nodes, ","), strings.Join(edges, ","))
}

// mkFlow 带顶层 name 的线性流程——**name 原样写入**，所以 "   "（纯空白）与
// " padded "（首尾空白）这些条款 1.1 的形态都能被精确构造。
func mkFlow(name string, specs ...taskSpec) string {
	return buildFlow(`"name":`+jsonQuote(name)+`,`, specs...)
}

// mkFlowNoName 顶层**不带 name 键**的线性流程（条款 1.1「未带」的键缺失形态）
func mkFlowNoName(specs ...taskSpec) string { return buildFlow(``, specs...) }

// doingActors 读回指定节点唯一进行中任务的参与者（**读持久值**，issues/113 教训）
func doingActors(t *testing.T, repo *memory.Repository, instID int64, node string, wantTasks int) []string {
	t.Helper()
	tasks, err := repo.FindDoingTasks(context.Background(), instID, []string{node})
	if err != nil {
		t.Fatalf("读 doing 任务(%s): %v", node, err)
	}
	if len(tasks) != wantTasks {
		t.Fatalf("节点 %s 的进行中任务数 = %d, want %d", node, len(tasks), wantTasks)
	}
	actors, err := repo.FindTaskActors(context.Background(), tasks[0].ID)
	if err != nil {
		t.Fatalf("读回 %s 参与者: %v", node, err)
	}
	return actors
}

// ─── ⑤ 条款 1.1：processName 取值口径（trim 判空 + 回落定义行 + 传出去必 trim）──

// 诱饵行钉住"取的到底是哪一头"：模型 name 与 define.name **不一致**时，
// 两个名字各配一条委托、各指向不同代理人——取错那头必然选错人。
func TestSurrogateProcessNamePrefersModelName(t *testing.T) {
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "define-surr116",
		mkFlow("model-surr116", taskSpec{id: "t1", assignee: "nm-zhang"}), ext)
	now := time.Now()
	// 两条对同一授权人都生效、时间窗相同，只有流程名不同
	putSurr(t, ext, "nm-zhang", "from-model-agent", "model-surr116",
		ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	putSurr(t, ext, "nm-zhang", "from-define-agent", "define-surr116",
		ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	actors := startOneTaskActor(t, eng, repo, defID)
	if countOf(actors, "from-model-agent") != 1 {
		t.Fatalf("条款 1.1 模型 name 优先：应并入 from-model-agent，读回 %v", actors)
	}
	if countOf(actors, "from-define-agent") != 0 {
		t.Fatalf("取到 define.name 那头了（应取模型 name），读回 %v", actors)
	}
}

// 「未带」= 键缺失 / null / 空串 / **仅空白**：四种形态都要回落 wf_process_define.name。
// 纯空白是本轮修的跨栈分叉点——按假值判空会拿 "   " 去查委托，该流程自己配的委托
// 一条也查不到（只剩全流程兜底行能命中），用户视角＝委托静默失效。
func TestSurrogateProcessNameFallsBackToDefineName(t *testing.T) {
	cases := []struct {
		what, defineName, model string
		assignee, agent         string
	}{
		{"模型 name 纯空白", "blankdef-surr116", mkFlow("   ", taskSpec{id: "t1", assignee: "fb-zhang"}), "fb-zhang", "blank-agent"},
		{"模型 name 空串", "emptydef-surr116", mkFlow("", taskSpec{id: "t1", assignee: "fe-zhang"}), "fe-zhang", "empty-agent"},
		{"模型不带 name 键", "nodef-surr116", mkFlowNoName(taskSpec{id: "t1", assignee: "fn-zhang"}), "fn-zhang", "nokey-agent"},
		{"回落值也 trim（define.name 带首尾空白）", " spaced-def-surr116 ", mkFlow("\t ", taskSpec{id: "t1", assignee: "sd-zhang"}), "sd-zhang", "spaced-def-agent"},
	}
	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			ext := memory.NewExt()
			eng, repo, defID := newHarness(t, c.defineName, c.model, ext)
			now := time.Now()
			// 委托台账里存的是**干净**的流程名（trim 后的 define.name）
			putSurr(t, ext, c.assignee, c.agent, strings.TrimSpace(c.defineName),
				ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
			actors := startOneTaskActor(t, eng, repo, defID)
			if countOf(actors, c.agent) != 1 {
				t.Fatalf("条款 1.1「%s」应回落 wf_process_define.name=%q 并命中该流程的委托，读回 %v",
					c.what, c.defineName, actors)
			}
			if countOf(actors, c.assignee) != 1 {
				t.Fatalf("回落命中后授权人仍需保留，读回 %v", actors)
			}
		})
	}
}

// captureExt 记录每次传给 GetSurrogate 的流程名（条款 1.1「传出去必 trim」的断言面）
type captureExt struct {
	*memory.ExtRepository
	names     []string
	operators []string
}

func (c *captureExt) GetSurrogate(ctx context.Context, operator, processName string, at time.Time) (*model.ProcessSurrogate, error) {
	c.names = append(c.names, processName)
	c.operators = append(c.operators, operator)
	return c.ExtRepository.GetSurrogate(ctx, operator, processName, at)
}

// 模型 name 带首尾空白：传给 GetSurrogate 的必须已是 trim 后的值，
// `" 名 "` 与 `"名"` 要命中同一条委托。
func TestSurrogateProcessNameIsTrimmedBeforeQuery(t *testing.T) {
	ext := &captureExt{ExtRepository: memory.NewExt()}
	eng, repo, defID := newHarness(t, "paddeddef-surr116",
		mkFlow("  padded-surr116  ", taskSpec{id: "t1", assignee: "pd-zhang"}), ext)
	now := time.Now()
	putSurr(t, ext, "pd-zhang", "pd-agent", "padded-surr116",
		ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	actors := startOneTaskActor(t, eng, repo, defID)
	if countOf(actors, "pd-agent") != 1 {
		t.Fatalf("模型 name 首尾空白应 trim 后再查委托（台账存的是 padded-surr116），读回 %v", actors)
	}
	if len(ext.names) == 0 {
		t.Fatalf("未捕获到任何 GetSurrogate 调用，用例空转")
	}
	for i, n := range ext.names {
		if n != "padded-surr116" {
			t.Fatalf("第 %d 次传给 GetSurrogate 的流程名 = %q，必须是 trim 后的 %q", i+1, n, "padded-surr116")
		}
	}
}

// countDefineRepo 统计 FindDefineByID 次数（钉住条款 1.1 尾注「逐次 execution 解析一次后
// 复用，不要逐任务解析」——回落读定义行含 content BLOB，不得按任务放大）
type countDefineRepo struct {
	*memory.Repository
	defineReads int
}

func (c *countDefineRepo) FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error) {
	c.defineReads++
	return c.Repository.FindDefineByID(ctx, id)
}

// 一次发起里 fork 出**两个**任务节点（两次 createTask → 两次取流程名），
// 模型 name 纯空白 ⇒ 必须回落定义行；读定义行总次数应恒为 2：
// 发起本身 1 次（取 content）+ 回落填缓存 1 次；缓存失效则会是 3 次。
func TestSurrogateDefineNameFallbackIsCached(t *testing.T) {
	repo := &countDefineRepo{Repository: memory.New()}
	content := `{"name":"   ","displayName":"委托测试","type":"approval",
	 "nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"fork","type":"snaker:fork","properties":{},"text":{"value":"并行"}},
	  {"id":"ta","type":"snaker:task","properties":{"assignee":"ck-zhang","taskType":0,"performType":0},"text":{"value":"A"}},
	  {"id":"tb","type":"snaker:task","properties":{"assignee":"ck-wang","taskType":0,"performType":0},"text":{"value":"B"}},
	  {"id":"join","type":"snaker:join","properties":{},"text":{"value":"汇聚"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e0","sourceNodeId":"start","targetNodeId":"fork","properties":{}},
	  {"id":"e1","sourceNodeId":"fork","targetNodeId":"ta","properties":{}},
	  {"id":"e2","sourceNodeId":"fork","targetNodeId":"tb","properties":{}},
	  {"id":"e3","sourceNodeId":"ta","targetNodeId":"join","properties":{}},
	  {"id":"e4","sourceNodeId":"tb","targetNodeId":"join","properties":{}},
	  {"id":"e5","sourceNodeId":"join","targetNodeId":"end","properties":{}}]}`
	def := &model.ProcessDefine{Name: "cachedef-surr116", DisplayName: "委托测试",
		Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	ext := memory.NewExt()
	now := time.Now()
	// 回落名命中：两条委托都按 define.name 配
	putSurr(t, ext, "ck-zhang", "ck-agent-a", "cachedef-surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	putSurr(t, ext, "ck-wang", "ck-agent-b", "cachedef-surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	eng := engine.New(repo, sUserProv{}, &sIDGen{}, nil, engine.WithSurrogateRepository(ext))
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "boss1", nil)
	if err != nil {
		t.Fatalf("fork 建单被打断: %v", err)
	}
	if a := doingActors(t, repo.Repository, inst.ID, "ta", 1); countOf(a, "ck-agent-a") != 1 {
		t.Fatalf("分支 A 未并入回落命中后的代理人，读回 %v", a)
	}
	if a := doingActors(t, repo.Repository, inst.ID, "tb", 1); countOf(a, "ck-agent-b") != 1 {
		t.Fatalf("分支 B 未并入回落命中后的代理人，读回 %v", a)
	}
	if repo.defineReads != 2 {
		t.Fatalf("回落读定义行应按 defineId 缓存复用：FindDefineByID 次数 = %d, want 2"+
			"（发起取 content 1 次 + 回落填缓存 1 次；逐任务解析会是 3 次）", repo.defineReads)
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

// ─── ⑥ 条款 1「覆盖范围」：每条建任务路径各留一条独立用例 ──────────────────────
//
// 办理推进 / 跳转(JUMP) / 回退(ROLLBACK) / 串行会签的每一步推进 四条路径各一条，且各用
// **专属流程名 + 专属参与者 + 专属代理人**，并在断言前先做"起点自证"（前置任务里故意不
// 给对应参与者配委托 ⇒ 代理人只可能来自本路径）。这样把某一条路径的委托应用单独注掉时
// **只有它自己红**；共用代理人或合并成一条"全流程都覆盖"的用例，
// 单条路径失能时看不出是谁红（条款 1 尾句）。
// ⚠️ "办理推进"与"发起"在 Go 栈共用 createTask 的同一个 saveNewTask 调用点（无独立挂点），
// 所以它的失能无法做到"只红自己"——那一条注掉会连发起路径一起红（用例本身仍按路径独立）。

// 路径 1/4：办理推进（普通节点→下一节点）—— 与发起共用 createTask 的普通任务分支，
// 但用**专属流程名 + 专属参与者**单独立一条，覆盖条款 1 清单里的"办理推进"。
// 委托只配在第二节点的参与者身上 ⇒ 第一节点（发起）拿不到代理人。
func TestSurrogateAppliesOnNormalAdvance(t *testing.T) {
	ctx := context.Background()
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surradv116",
		mkFlow("surradv116",
			taskSpec{id: "a1", assignee: "adv-zhang"},
			taskSpec{id: "a2", assignee: "adv-wang"}), ext)
	now := time.Now()
	putSurr(t, ext, "adv-wang", "adv-agent", "surradv116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	inst, err := eng.StartProcessInstanceByID(ctx, defID, "boss1", nil)
	if err != nil {
		t.Fatalf("发起被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "a1", 1); len(a) != 1 || a[0] != "adv-zhang" {
		t.Fatalf("起点自证：a1 任务不该出现代理人，读回 %v", a)
	}
	a1, err := repo.FindDoingTasks(ctx, inst.ID, []string{"a1"})
	if err != nil || len(a1) != 1 {
		t.Fatalf("取 a1 任务: len=%d err=%v", len(a1), err)
	}
	if _, err := eng.ExecuteProcessTask(ctx, a1[0].ID, "adv-zhang", nil); err != nil {
		t.Fatalf("办理推进被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "a2", 1); len(a) != 2 || a[0] != "adv-wang" || a[1] != "adv-agent" {
		t.Fatalf("条款 1「办理推进」：推进新建的任务未并入代理人，读回 %v（期望 [adv-wang adv-agent]）", a)
	}
}

// 路径 2/4：跳转 JUMP —— ExecuteAndJumpTask(targetTaskName != "")
func TestSurrogateAppliesOnJumpPath(t *testing.T) {
	ctx := context.Background()
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surrjump116",
		mkFlow("surrjump116",
			taskSpec{id: "j1", assignee: "jmp-zhang"},
			taskSpec{id: "j2", assignee: "jmp-wang"}), ext)
	now := time.Now()
	// 委托只配在跳转目标节点的参与者身上：j1 的 jmp-zhang 没有委托 ⇒ 发起路径拿不到
	// jmp-agent，代理人只可能由跳转路径写进 j2 的新任务
	putSurr(t, ext, "jmp-wang", "jmp-agent", "surrjump116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)

	inst, err := eng.StartProcessInstanceByID(ctx, defID, "boss1", nil)
	if err != nil {
		t.Fatalf("发起被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "j1", 1); len(a) != 1 || a[0] != "jmp-zhang" {
		t.Fatalf("起点自证：发起产生的 j1 任务不该出现代理人，读回 %v", a)
	}
	j1, err := repo.FindDoingTasks(ctx, inst.ID, []string{"j1"})
	if err != nil || len(j1) != 1 {
		t.Fatalf("取 j1 任务: len=%d err=%v", len(j1), err)
	}
	if _, err := eng.ExecuteAndJumpTask(ctx, j1[0].ID, "jmp-zhang",
		map[string]interface{}{"comment": "跳转"}, "j2"); err != nil {
		t.Fatalf("跳转(JUMP)被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "j2", 1); len(a) != 2 || a[0] != "jmp-wang" || a[1] != "jmp-agent" {
		t.Fatalf("条款 1「跳转(JUMP)」：跳转新建的任务未并入代理人，读回 %v（期望 [jmp-wang jmp-agent]）", a)
	}
}

// 路径 3/4：回退 ROLLBACK —— ExecuteAndJumpTask(targetTaskName == "")，
// 新任务落在**上一个节点**、参与者 = 回退操作人（resolveActorsForRollback）
func TestSurrogateAppliesOnRollbackPath(t *testing.T) {
	ctx := context.Background()
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surrback116",
		mkFlow("surrback116",
			taskSpec{id: "b1", assignee: "rbk-zhang"},
			taskSpec{id: "b2", assignee: "rbk-wang"}), ext)
	now := time.Now()
	// issues/121 P2 血缘版：回退复活的是 b1 那条历史行；b1 是 start 直接后继 ⇒ 契约规定参与者
	// 取该行 variable.u_userId（发起人快照），不是执行回退的 rbk-wang。本 harness 的 UserProvider
	// 桩把 u_userId 写成 "u"，故委托配在 "u" 身上。台账延后到 b1 建单之后再配，否则起点自证不成立。

	inst, err := eng.StartProcessInstanceByID(ctx, defID, "boss1", nil)
	if err != nil {
		t.Fatalf("发起被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "b1", 1); len(a) != 1 || a[0] != "rbk-zhang" {
		t.Fatalf("起点自证：发起产生的 b1 任务不该出现代理人，读回 %v", a)
	}
	b1, err := repo.FindDoingTasks(ctx, inst.ID, []string{"b1"})
	if err != nil || len(b1) != 1 {
		t.Fatalf("取 b1 任务: len=%d err=%v", len(b1), err)
	}
	if _, err := eng.ExecuteProcessTask(ctx, b1[0].ID, "rbk-zhang", nil); err != nil {
		t.Fatalf("推进到 b2 被打断: %v", err)
	}
	putSurr(t, ext, "u", "rbk-agent", "surrback116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	b2, err := repo.FindDoingTasks(ctx, inst.ID, []string{"b2"})
	if err != nil || len(b2) != 1 {
		t.Fatalf("取 b2 任务: len=%d err=%v", len(b2), err)
	}
	if _, err := eng.ExecuteAndJumpTask(ctx, b2[0].ID, "rbk-wang", nil, ""); err != nil {
		t.Fatalf("回退(ROLLBACK)被打断: %v", err)
	}
	// 原 b1 任务已 DONE，b1 上唯一的进行中任务就是回退新建的那一条
	if a := doingActors(t, repo, inst.ID, "b1", 1); len(a) != 2 || a[0] != "u" || a[1] != "rbk-agent" {
		t.Fatalf("条款 1「回退(ROLLBACK)」：复活行应＝该行 u_userId + 其代理人，读回 %v（期望 [u rbk-agent]）", a)
	}
}

// 路径 4/4：串行会签的每一步推进 —— ExecuteProcessTask 里 SEQUENTIAL 分支
// 为下一步成员新建任务（引擎侧独立调用点，不在 createTask 里）
func TestSurrogateAppliesOnSequentialCountersignAdvance(t *testing.T) {
	ctx := context.Background()
	ext := memory.NewExt()
	eng, repo, defID := newHarness(t, "surrseq116",
		mkFlow("surrseq116", taskSpec{id: "cs", assignee: "seq-zhang,seq-wang", countersign: "SEQUENTIAL"}), ext)
	now := time.Hour
	// 只给**第二步**的 seq-wang 配委托：第一步任务（seq-zhang）由发起路径产生、拿不到
	// 代理人 ⇒ 代理人只可能来自"串行会签推进"这条路径
	putSurr(t, ext, "seq-wang", "seq-agent", "surrseq116", ptrOf(time.Now().Add(-now)), ptrOf(time.Now().Add(now)), 1)

	inst, err := eng.StartProcessInstanceByID(ctx, defID, "boss1", nil)
	if err != nil {
		t.Fatalf("串行会签发起被打断: %v", err)
	}
	if a := doingActors(t, repo, inst.ID, "cs", 1); len(a) != 1 || a[0] != "seq-zhang" {
		t.Fatalf("起点自证：第一步任务不该出现代理人（seq-zhang 无委托），读回 %v", a)
	}
	step1, err := repo.FindDoingTasks(ctx, inst.ID, []string{"cs"})
	if err != nil || len(step1) != 1 {
		t.Fatalf("取第一步任务: len=%d err=%v", len(step1), err)
	}
	if _, err := eng.ExecuteProcessTask(ctx, step1[0].ID, "seq-zhang", nil); err != nil {
		t.Fatalf("串行会签推进被打断: %v", err)
	}
	second, err := repo.FindDoingTasks(ctx, inst.ID, []string{"cs"})
	if err != nil || len(second) != 1 {
		t.Fatalf("取第二步任务: len=%d err=%v", len(second), err)
	}
	if lc := fmt.Sprint(second[0].Variables["loopCounter_cs"]); lc != "1" {
		t.Fatalf("自证：断言对象必须是串行会签的第 2 步，实读 loopCounter_cs = %s", lc)
	}
	if a := second[0].ActorIDs; len(a) != 2 || a[0] != "seq-wang" || a[1] != "seq-agent" {
		t.Fatalf("条款 1「串行会签的每一步推进」：推进出的下一步任务未并入代理人，读回 %v（期望 [seq-wang seq-agent]）", a)
	}
	// 条款 1.3：代理人只进当一步任务的参与者，不得扩 operatorList 投票名册（否则改票数）
	if got := fmt.Sprint(second[0].Variables["operatorList_cs"]); got != "[seq-zhang seq-wang]" {
		t.Fatalf("条款 1.3：串行会签推进不得把代理人写进投票名册，operatorList_cs = %s", got)
	}
	if got := fmt.Sprint(second[0].Variables["nrOfInstances_cs"]); got != "2" {
		t.Fatalf("条款 1.3：票数不得因代理人改变，nrOfInstances_cs = %s", got)
	}
}
