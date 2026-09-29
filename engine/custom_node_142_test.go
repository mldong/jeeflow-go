// issues/142 A 批（go 腿）· 记录类节点（`snaker:custom`）执行形状。
//
// 判据逐字来自 spec 02-flow-definition.md §6.1／§6.2（owner 2026-09-29、09-30 两次拍板）：
//
//	§6.1 表第二行（记录类）：执行 clazz → 落**历史行**（task_state=20）→ 令牌继续流转；
//	        禁止形状①「当任务类建 DOING 行」／②「兜底把行挂给当前操作人（伪造一条他不该收到的待办）」
//	        ／③「直接跳过节点不建行（丢留痕）」。
//	§6.2 第 1 条：历史行**必须真落库**——只在聚合内存对象里 append 一条不算做到
//	        （SQL 侧取证在 repository/jdbc/custom_history_142_sqlite_test.go）。
//	§6.2 第 2 条：clazz 解析不了 ⇒ 记 WARNING ＋ 照常落历史行 ＋ 续流，严禁报错打断建单；
//	        "未注册"与"clazz 空串"两档文案要分别可诊断；处理器**自身**失败不在豁免内，照旧外抛。
//	§6.2 第 3 条：记录类腿不解析参与者。
//	外加：不 fire 码 3（PROCESS_TASK_START 表达"新待办产生"，记录类不该有）。
//
// 断言一律落在**读回的持久行**上（repo.FindHistoryTasks / FindTaskActors），
// 不是引擎返回的聚合对象——issues/113 教训：只有读回值能证明"这一行真进了库"。
// 现成对照＝python `_exec_custom_node`（jeeflow/engine.py:781-839）。
package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 夹具 ──────────────────────────────────────────────────────────────────────

type c142UserProv struct{}

func (c142UserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID}, nil
}

type c142IDGen struct{ n int64 }

func (g *c142IDGen) NextID() int64 { g.n++; return g.n }

// c142Node 一个节点（id + type + properties 片段）。
type c142Node struct{ id, typ, props string }

// c142Flow 造 start →（逐个节点串成线性链）→ end 的流程定义 JSON。
// 记录类节点的"令牌继续流转"就靠这条链判得出来。
func c142Flow(name string, nodes ...c142Node) string {
	var ns, es []string
	ns = append(ns, `{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}}`)
	prev := "start"
	for i, n := range nodes {
		props := n.props
		if props == "" {
			props = "{}"
		}
		ns = append(ns, fmt.Sprintf(`{"id":%q,"type":%q,"properties":%s,"text":{"value":"节点%d"}}`,
			n.id, n.typ, props, i+1))
		es = append(es, fmt.Sprintf(`{"id":"e%d","sourceNodeId":%q,"targetNodeId":%q,"properties":{}}`,
			i, prev, n.id))
		prev = n.id
	}
	ns = append(ns, `{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}`)
	es = append(es, fmt.Sprintf(`{"id":"eend","sourceNodeId":%q,"targetNodeId":"end","properties":{}}`, prev))
	return `{"name":"` + name + `","displayName":"记录类节点142","type":"approval","nodes":[` +
		strings.Join(ns, ",") + `],"edges":[` + strings.Join(es, ",") + `]}`
}

// c142Harness 内存仓 + 引擎 + 事件记录仪。attachRegistry=false 时引擎不挂注册表
// （专门用来测"未装配 HandlerRegistry"那一档）。
type c142Harness struct {
	eng    *engine.EngineImpl
	repo   *memory.Repository
	events *[]string
	reg    *engine.HandlerRegistry
	defID  int64
}

func c142Setup(t *testing.T, name, content string, attachRegistry bool) *c142Harness {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "记录类节点142", Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	var events []string
	eng := engine.New(repo, c142UserProv{}, &c142IDGen{}, nil)
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{func(evt engine.ProcessEvent) {
			events = append(events, fmt.Sprintf("%s:%s", evt.Type.SpecName(), evt.NodeID))
		}},
	})
	reg := engine.NewHandlerRegistry()
	if attachRegistry {
		eng.SetRegistry(reg)
	}
	return &c142Harness{eng: eng, repo: repo, events: &events, reg: reg, defID: def.ID}
}

// c142Rows 读回某实例的全部持久任务行（FindHistoryTasks＝不带状态过滤的那条读路），按 id 升序。
func c142Rows(t *testing.T, repo *memory.Repository, instID int64) []*model.ProcessTask {
	t.Helper()
	rows, err := repo.FindHistoryTasks(context.Background(), instID)
	if err != nil {
		t.Fatalf("读回持久任务行: %v", err)
	}
	for i := 1; i < len(rows); i++ { // 内存仓遍历 map 无序 ⇒ 插入排序稳定化
		for j := i; j > 0 && rows[j-1].ID > rows[j].ID; j-- {
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
	return rows
}

// c142RowByNode 读回某节点的全部持久行（0 条合法，条数由调用方判）。
func c142RowByNode(t *testing.T, repo *memory.Repository, instID int64, node string) []*model.ProcessTask {
	t.Helper()
	var out []*model.ProcessTask
	for _, r := range c142Rows(t, repo, instID) {
		if r.TaskName == node {
			out = append(out, r)
		}
	}
	return out
}

func c142Doing(t *testing.T, repo *memory.Repository, instID int64) []*model.ProcessTask {
	t.Helper()
	doing, err := repo.FindDoingTasks(context.Background(), instID, nil)
	if err != nil {
		t.Fatalf("读回待办: %v", err)
	}
	return doing
}

func c142Names(ts []*model.ProcessTask) []string {
	out := make([]string, 0, len(ts))
	for _, x := range ts {
		out = append(out, fmt.Sprintf("%s(state=%d)", x.TaskName, x.TaskState))
	}
	return out
}

// c142CaptureLog 把标准库日志（引擎 WARNING 走的就是它）收到 buffer，测完自动还原。
func c142CaptureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	old := log.Writer()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return buf
}

// c142OnlyInstanceID 本案每个格子只开一条流 ⇒ 从分页读回那唯一一条实例的 id
// （不硬编码 idGen 生成的值，免得夹具一改就假红）。
func c142OnlyInstanceID(t *testing.T, repo *memory.Repository, operator string) int64 {
	t.Helper()
	rows, total, err := repo.PageInstances(context.Background(),
		spi.PageQuery{PageNum: 1, PageSize: 10}, operator)
	if err != nil {
		t.Fatalf("PageInstances: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("本案夹具应只有 1 条实例，实得 total=%d rows=%d", total, len(rows))
	}
	return rows[0].ID
}

// c142Vars 读回实例的持久变量。
func c142Vars(t *testing.T, repo *memory.Repository, instID int64) map[string]interface{} {
	t.Helper()
	inst, err := repo.FindInstanceByID(context.Background(), instID)
	if err != nil || inst == nil {
		t.Fatalf("读回实例 %d: %v", instID, err)
	}
	return inst.Variables
}

// c142Handler 可编程的记录类处理器桩。
type c142Handler struct {
	ret   interface{}
	err   error
	calls *int
}

func (h *c142Handler) Handle(node *model.FlowNode, inst *model.ProcessInstance, operator string,
	vars map[string]interface{}) (interface{}, error) {
	if h.calls != nil {
		*h.calls++
	}
	return h.ret, h.err
}

type panicHandler struct{}

func (panicHandler) Handle(*model.FlowNode, *model.ProcessInstance, string, map[string]interface{}) (interface{}, error) {
	panic("boom-142")
}

// c142Clazz 共享夹具 flows/08-custom-node.json:44 里那个原样串（本栈按"名字"注册，不反射）。
const c142Clazz = "com.mldong.jeeflow.test.TestCustomHandler"

// ─── ① 记录类形状：DONE 历史行 + 落库 + 续流 + 不发码 3 ───────────────────────

// TestIssue142CustomNodeLandsDoneRowNotTodo 短流 start→custom→end：
// 库里查得到那条 task_state=20 的行／待办数不因它增加／实例继续到 end／码 3 不为它出现。
//
// 改前必红（旧形状＝engine_impl.go:591 把 custom 与 task 同路走 createTask）：
//   - 「待办数 0」红：旧形状给它建了一条 task_state=10 的待办（§6.1 禁止形状①）；
//   - 「实例到 end」红：旧形状建单后不沿出边推进，实例停在 state=10；
//   - 「事件序列不含 PROCESS_TASK_START」红：旧形状建单就 fire 码 3；
//   - 「state=20 那一行」红：读回来的是 state=10。
func TestIssue142CustomNodeLandsDoneRowNotTodo(t *testing.T) {
	h := c142Setup(t, "c142-short", c142Flow("c142-short",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("记录类节点后令牌应走到 end，实例 state=%d", inst.State)
	}
	if doing := c142Doing(t, h.repo, inst.ID); len(doing) != 0 {
		t.Fatalf("记录类节点不该产生待办，实得 %v", c142Names(doing))
	}
	rows := c142RowByNode(t, h.repo, inst.ID, "custom1")
	if len(rows) != 1 {
		t.Fatalf("custom1 应有且只有一条持久行，实得 %d 条", len(rows))
	}
	ht := rows[0]
	if ht.TaskState != model.TaskStateDone {
		t.Fatalf("历史行 task_state 应为 20（DONE），实得 %d", ht.TaskState)
	}
	if ht.DisplayName != "节点1" {
		t.Fatalf("历史行 display_name 应取节点文本，实得 %q", ht.DisplayName)
	}
	// 建单不变量①②：发起路径 parent 落 0；custom1 是 start 直接后继 ⇒ 标记 true
	if ht.ParentTaskID == nil || *ht.ParentTaskID != 0 {
		t.Fatalf("历史行 parentTaskID 应落 0（发起 execution 无当前任务），实得 %v", ht.ParentTaskID)
	}
	if ht.Variables[model.IsFirstTaskNodeKey] != true {
		t.Fatalf("历史行首节点标记应随行写入 true，实得 %v", ht.Variables)
	}
	// 留痕主体＝当前操作人（不是"待办归属"：行状态是 DONE，谁也办不动）
	actors, _ := h.repo.FindTaskActors(context.Background(), ht.ID)
	if len(actors) != 1 || actors[0] != "boss1" {
		t.Fatalf("历史行参与者应为留痕主体 [boss1]，实得 %v", actors)
	}
	if ht.ActorID != "boss1" {
		t.Fatalf("历史行 operator 列应为当前操作人，实得 %q", ht.ActorID)
	}
	// 记录类不是会签、没有 form、没有到期时间（与 java createHistoryTask 的空档同形）
	if ht.PerformType != 0 || ht.FormKey != "" || ht.ExpireTime != nil {
		t.Fatalf("历史行应 perform_type=0／form 空／expire_time NULL，实得 %+v", ht)
	}
	if ht.FinishTime == nil {
		t.Fatal("历史行 finish_time 应有值（本栈 DONE 行口径，见 model.CreateHistoryTask 注释）")
	}
	// 码 3 不为它出现：整条短流只有"实例开始/实例结束"两枚事件
	want := []string{"PROCESS_INSTANCE_START:", "PROCESS_INSTANCE_END:end"}
	if got := *h.events; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("事件序列不该含 PROCESS_TASK_START，\n want %v\n  got %v", want, got)
	}
}

// TestIssue142CustomNodeNoTodoBetweenTasks start→apply(任务)→custom→task1(任务)→end：
// 待办列表里不多出那一条（§6.1 硬结论 2 的计数分母要把它摘出去），
// 且办理 apply 后令牌穿过 custom 落到 task1。
//
// 改前必红：旧形状下 apply 办结后落地的待办是 custom1（不是 task1），
// 且事件序列里多一枚 PROCESS_TASK_START:custom1。
func TestIssue142CustomNodeNoTodoBetweenTasks(t *testing.T) {
	h := c142Setup(t, "c142-mid", c142Flow("c142-mid",
		c142Node{"apply", model.TypeTask, `{"assignee":"applicant","performType":0}`},
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`},
		c142Node{"task1", model.TypeTask, `{"assignee":"leader","performType":0}`}), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	doing := c142Doing(t, h.repo, inst.ID)
	if len(doing) != 1 || doing[0].TaskName != "apply" {
		t.Fatalf("发起后待办应只有 apply，实得 %v", c142Names(doing))
	}
	applyID := doing[0].ID
	if _, err := h.eng.ExecuteProcessTask(context.Background(), applyID, "boss1",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("execute apply: %v", err)
	}
	doing = c142Doing(t, h.repo, inst.ID)
	if len(doing) != 1 || doing[0].TaskName != "task1" {
		t.Fatalf("穿过记录类节点后的待办应只有 task1，实得 %v", c142Names(doing))
	}
	if n := len(c142RowByNode(t, h.repo, inst.ID, "custom1")); n != 1 {
		t.Fatalf("custom1 应有 1 条历史行，实得 %d", n)
	}
	wantSeq := []string{
		"PROCESS_INSTANCE_START:", "PROCESS_TASK_START:apply", // 发起建 apply 待办
		"TASK_COMPLETE:apply", "PROCESS_TASK_START:task1", // 办 apply → 穿 custom（零事件）→ 建 task1 待办
	}
	if got := *h.events; strings.Join(got, "|") != strings.Join(wantSeq, "|") {
		t.Fatalf("事件序列不符（记录类节点不许出现任何事件），\n want %v\n  got %v", wantSeq, got)
	}
	// 血缘不变量①：task1 的 parent＝刚办结的 apply，不是 custom1（custom 不"办"）
	tasks := c142RowByNode(t, h.repo, inst.ID, "task1")
	if len(tasks) != 1 {
		t.Fatalf("task1 应只有一条行，实得 %d", len(tasks))
	}
	if tasks[0].ParentTaskID == nil || *tasks[0].ParentTaskID != applyID {
		t.Fatalf("task1 的 parent 应为刚办结的 apply=%d，实得 %v", applyID, tasks[0].ParentTaskID)
	}
}

// TestIssue142TwoCustomNodesChain custom1→custom2 连着两个记录类节点：各自落一条 DONE 行、
// 令牌一路走到 end（多次执行也不串味）。
func TestIssue142TwoCustomNodesChain(t *testing.T) {
	h := c142Setup(t, "c142-chain", c142Flow("c142-chain",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`},
		c142Node{"custom2", model.TypeCustom, `{}`}), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("链上两个记录类节点后应到 end，state=%d", inst.State)
	}
	rows := c142Rows(t, h.repo, inst.ID)
	if len(rows) != 2 {
		t.Fatalf("应恰好两条历史行，实得 %d: %v", len(rows), c142Names(rows))
	}
	for _, r := range rows {
		if r.TaskState != model.TaskStateDone {
			t.Fatalf("%s 行 state 应为 20，实得 %d", r.TaskName, r.TaskState)
		}
	}
	if n := len(c142Doing(t, h.repo, inst.ID)); n != 0 {
		t.Fatalf("记录类链不该有任何待办，实得 %d", n)
	}
}

// ─── ② clazz 分档：未注册／空串 ⇒ WARNING＋照常落行＋续流；处理器自身失败 ⇒ 外抛 ──

// TestIssue142UnregisteredClazzWarnsButStillRecords clazz 有值但没注册处理器：
// 不报错、记一条点名"未注册处理器"的 WARNING、历史行照落、实例继续到 end。
func TestIssue142UnregisteredClazzWarnsButStillRecords(t *testing.T) {
	logs := c142CaptureLog(t)
	h := c142Setup(t, "c142-unreg", c142Flow("c142-unreg",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"com.example.NotRegistered"}`}), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("未注册 clazz 不该打断建单，实得 err=%v", err)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("未注册 clazz 仍要令牌续流到 end，state=%d", inst.State)
	}
	rows := c142RowByNode(t, h.repo, inst.ID, "custom1")
	if len(rows) != 1 || rows[0].TaskState != model.TaskStateDone {
		t.Fatalf("未注册 clazz 也要照常落 DONE 历史行，实得 %v", c142Names(rows))
	}
	got := logs.String()
	if !strings.Contains(got, "WARNING") || !strings.Contains(got, "未注册处理器") {
		t.Fatalf("应记一条点名\"未注册处理器\"的 WARNING，实得日志：%q", got)
	}
	if !strings.Contains(got, "com.example.NotRegistered") {
		t.Fatalf("WARNING 要带上 clazz 原样串才可诊断，实得日志：%q", got)
	}
}

// TestIssue142BlankClazzWarnsSeparately clazz 为空白串 ⇒ **另一条文案**（"未配置 clazz"），
// 同样不报错、照常落行、续流。两档文案要分别可诊断（§6.2 第 2 条点名 c# 把两者合成
// 同一个异常、覆盖面比 java 宽，本栈分档）。
func TestIssue142BlankClazzWarnsSeparately(t *testing.T) {
	logs := c142CaptureLog(t)
	h := c142Setup(t, "c142-blank", c142Flow("c142-blank",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"   "}`}), true)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("clazz 空串不该打断建单，实得 err=%v", err)
	}
	rows := c142RowByNode(t, h.repo, inst.ID, "custom1")
	if len(rows) != 1 || rows[0].TaskState != model.TaskStateDone {
		t.Fatalf("clazz 空串也要照常落 DONE 历史行，实得 %v", c142Names(rows))
	}
	got := logs.String()
	if !strings.Contains(got, "未配置 clazz") {
		t.Fatalf("空串档应记\"未配置 clazz\"专属文案，实得日志：%q", got)
	}
	if strings.Contains(got, "未注册处理器") {
		t.Fatalf("空串档不该并入\"未注册处理器\"那一档，实得日志：%q", got)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("clazz 空串仍要续流到 end，state=%d", inst.State)
	}
}

// TestIssue142NoRegistryWarnsAndRecords 引擎压根没装配 HandlerRegistry（registry==nil）：
// 同样不报错、落行、续流，日志点名"未装配"。
func TestIssue142NoRegistryWarnsAndRecords(t *testing.T) {
	logs := c142CaptureLog(t)
	h := c142Setup(t, "c142-noreg", c142Flow("c142-noreg",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), false)
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("未装配注册表不该打断建单，实得 err=%v", err)
	}
	if len(c142RowByNode(t, h.repo, inst.ID, "custom1")) != 1 {
		t.Fatal("未装配注册表也要落历史行")
	}
	if !strings.Contains(logs.String(), "未装配") {
		t.Fatalf("日志应点名\"未装配 HandlerRegistry\"，实得 %q", logs.String())
	}
}

// TestIssue142RegisteredHandlerExecuted 注册了处理器 ⇒ 真被调用（按 clazz **原名**查表），
// 返回值按缺省键写进流程变量。
//
// 改前必红：旧形状下 `clazz` 在 go 根本不执行（全仓 clazz 只出现在夹具 JSON），
// calls 恒为 0、custom_return_val 也不会出现。
func TestIssue142RegisteredHandlerExecuted(t *testing.T) {
	h := c142Setup(t, "c142-ok", c142Flow("c142-ok",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), true)
	calls := 0
	h.reg.RegisterCustom(c142Clazz, &c142Handler{ret: "HELLO-142", calls: &calls})
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("已注册的 clazz 处理器应被调用 1 次，实得 %d", calls)
	}
	if got := c142Vars(t, h.repo, inst.ID)[engine.KeyCustomReturnVal]; got != "HELLO-142" {
		t.Fatalf("返回值应落进流程变量 custom_return_val，实得 %#v", got)
	}
}

// TestIssue142ValPropertyOverridesKey properties.val 给了名字 ⇒ 落进**自定义键**
// （缺省键不出现），对齐 java CustomModel.java:46-48 ＋ CustomParser.java:20-22。
func TestIssue142ValPropertyOverridesKey(t *testing.T) {
	h := c142Setup(t, "c142-val", c142Flow("c142-val",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `","val":"customResult"}`}), true)
	h.reg.RegisterCustom(c142Clazz, &c142Handler{ret: 42})
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	vars := c142Vars(t, h.repo, inst.ID)
	if vars["customResult"] != 42 {
		t.Fatalf("val=customResult 时返回值应落进 customResult，实得 %#v", vars["customResult"])
	}
	if _, ok := vars[engine.KeyCustomReturnVal]; ok {
		t.Fatalf("给了 val 就不该再写缺省键，实得变量 %v", vars)
	}
}

// TestIssue142NilReturnWritesNoKey 处理器返回 nil ⇒ 不写变量键
// （java 那边 `put(var, null)` 会写出一个 null 键，python 是 nil 不写 ⇒ 本栈跟 python，
// 分叉已写进交付报告）。
func TestIssue142NilReturnWritesNoKey(t *testing.T) {
	h := c142Setup(t, "c142-nil", c142Flow("c142-nil",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), true)
	h.reg.RegisterCustom(c142Clazz, &c142Handler{ret: nil})
	inst, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, ok := c142Vars(t, h.repo, inst.ID)[engine.KeyCustomReturnVal]; ok {
		t.Fatal("返回值 nil 不该凭空写出一个 null 键")
	}
	if len(c142RowByNode(t, h.repo, inst.ID, "custom1")) != 1 {
		t.Fatal("返回 nil 也要照常落历史行")
	}
}

// TestIssue142HandlerErrorPropagates 处理器**自身**报错 ⇒ 外抛（§6.2 第 2 条明写不在豁免内），
// 且不留"半截形状"：历史行不落、令牌不前进。这一格是上面三格"配错不炸"的负向对照。
func TestIssue142HandlerErrorPropagates(t *testing.T) {
	h := c142Setup(t, "c142-err", c142Flow("c142-err",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), true)
	h.reg.RegisterCustom(c142Clazz, &c142Handler{err: fmt.Errorf("业务处理器炸了")})
	_, err := h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
	if err == nil || !strings.Contains(err.Error(), "业务处理器炸了") {
		t.Fatalf("处理器自身失败应原样外抛，实得 err=%v", err)
	}
	instID := c142OnlyInstanceID(t, h.repo, "boss1")
	inst, _ := h.repo.FindInstanceByID(context.Background(), instID)
	if inst != nil && inst.State == model.InstanceStateDone {
		t.Fatal("处理器报错那一支不该把实例推成办结")
	}
	if n := len(c142RowByNode(t, h.repo, instID, "custom1")); n != 0 {
		t.Fatalf("处理器报错时不该落历史行（java exec 顺序：调用失败 ⇒ createHistoryTask/runOutTransition 都不发生），实得 %d 行", n)
	}
}

// TestIssue142HandlerPanicIsNotSwallowed 处理器 panic ⇒ 一路外抛、不被引擎吞掉
// （与"监听器 panic 被吞"的兜底语义（issues/104 P2）区分：那是监听器，这是业务执行腿）。
func TestIssue142HandlerPanicIsNotSwallowed(t *testing.T) {
	h := c142Setup(t, "c142-panic", c142Flow("c142-panic",
		c142Node{"custom1", model.TypeCustom, `{"clazz":"` + c142Clazz + `"}`}), true)
	h.reg.RegisterCustom(c142Clazz, panicHandler{})
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("处理器 panic 应外抛到调用方，不该被引擎吞掉")
		}
		if !strings.Contains(fmt.Sprint(r), "boom-142") {
			t.Fatalf("panic 值应原样透传，实得 %v", r)
		}
	}()
	_, _ = h.eng.StartProcessInstanceByID(context.Background(), h.defID, "boss1", nil)
}
