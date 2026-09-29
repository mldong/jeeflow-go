// issues/137 B（go 腿）：`createTaskWithActors` **零调用者**——全仓 grep 只有定义本身和两处注释
// 提到它（engine_impl.go:497/500、surrogate.go:112），主路径 createTask 自己走 resolveActors 解析
// 参与者，没有任何路径把"已经算好的显式参与者集合"递进来。
//
// owner 裁定：**不删**，补一格"直调它、行为与主路径一致"的用例。理由是这个函数承担**契约形状义务**——
// 引擎对外承诺"给定显式参与者集合即可按主路径同语义建单"，别的名同实异的实现（Java/其他栈）以此为
// 对照基准，未来的显式指派入口（预派人、外部算好参与者）也照这个形状接。零调用者 ≠ 零契约。
//
// 本文件在 **package engine** 内测包内函数（外部测试包够不着 createTaskWithActors）。
// 一致性判据：逐档把两条路各自跑一遍（各自一份内存仓），比对**读回的持久值**——
// 任务条数、逐行的 id/节点名/显示名/form_key/perform_type/task_state/parentTaskId/参与者集合/
// 行变量（含 isFirstTaskNode 与顺序会签簿记）/expire_time，以及建单期间 fire 的事件序列。
// 断言落在读回值而不是"看起来差不多"（issues/113 教训）。
package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// ─── 测试 SPI ──────────────────────────────────────────────────────────────────

type c137UserProv struct{}

func (c137UserProv) GetUser(id string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: id, RealName: id}, nil
}

type c137IDGen struct{ n int64 }

func (g *c137IDGen) NextID() int64 { g.n++; return g.n }

// c137Node 建"start → task1 → end"的单任务图，task1 即 start 直接后继
// （建单不变量 ② isFirstTaskNode=true 的判据成立，两条路都该带上这个行变量）。
func c137Node(props map[string]interface{}) (*model.FlowModel, *model.FlowNode) {
	flow := &model.FlowModel{
		Name:        "c137flow",
		DisplayName: "137B 形状对照流程",
		Type:        "approval",
		Nodes: []model.FlowNode{
			{ID: "start", Type: model.TypeStart, Text: struct {
				Value string `json:"value"`
			}{"开始"}},
			{ID: "task1", Type: model.TypeTask, Properties: props, Text: struct {
				Value string `json:"value"`
			}{"上级审批"}},
			{ID: "end", Type: model.TypeEnd, Text: struct {
				Value string `json:"value"`
			}{"结束"}},
		},
		Edges: []model.FlowEdge{
			{ID: "e0", SourceNodeID: "start", TargetNodeID: "task1"},
			{ID: "e1", SourceNodeID: "task1", TargetNodeID: "end"},
		},
	}
	return flow, &flow.Nodes[1]
}

// c137Harness 一份干净的内存仓 + 引擎（idGen 从 1 起，两条路各自一份 ⇒ 生成的任务 id 天然可直比）。
// listeners 非 nil 即装配 Extensions，用来记事件序列。
func c137Harness(listeners *[]string) (*EngineImpl, *memory.Repository) {
	repo := memory.New()
	e := New(repo, c137UserProv{}, &c137IDGen{}, nil)
	if listeners != nil {
		e.SetExtensions(&Extensions{Listeners: []ProcessEventListener{func(evt ProcessEvent) {
			*listeners = append(*listeners, fmt.Sprintf("type=%d node=%s task=%d ops=[%s]",
				int(evt.Type), evt.NodeID, evt.TaskID, strings.Join(evt.Actors, ",")))
		}}})
	}
	return e, repo
}

// c137Inst 一个流程内实例（不经引擎 StartProcess 建，直接用手搓实例——本案只对照建单那一段）。
// 两侧各搓一份、id 相同（777），建单时间戳由引擎自己取，不进比对快照 ⇒ 无时钟抖动。
func c137Inst() *model.ProcessInstance {
	return model.NewProcessInstance(777, 888, "boss137", map[string]interface{}{}, time.Now())
}

// c137Snapshot 读回持久值，按任务 id 升序展开成可直比的形状。
func c137Snapshot(t *testing.T, repo *memory.Repository, instID int64) []string {
	t.Helper()
	tasks, err := repo.FindDoingTasks(context.Background(), instID, nil)
	if err != nil {
		t.Fatalf("读回任务: %v", err)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	var out []string
	for _, tk := range tasks {
		parent := int64(0)
		if tk.ParentTaskID != nil {
			parent = *tk.ParentTaskID
		}
		expire := "nil"
		if tk.ExpireTime != nil {
			expire = tk.ExpireTime.Format("2006-01-02 15:04:05")
		}
		out = append(out, fmt.Sprintf("id=%d node=%s name=%s form=%s perform=%d state=%d parent=%d "+
			"actors=[%s] first=%v vars=%s expire=%s creator=%s",
			tk.ID, tk.TaskName, tk.DisplayName, tk.FormKey, tk.PerformType, int(tk.TaskState), parent,
			strings.Join(tk.ActorIDs, ","), tk.Variables[model.IsFirstTaskNodeKey],
			c137VarsString(tk.Variables), expire, tk.CreateUser))
	}
	return out
}

// c137VarsString 行变量全集（剔除随行 id 之外的抖动项：本案只有 isFirstTaskNode + 会签簿记）。
func c137VarsString(vars map[string]interface{}) string {
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%v;", k, vars[k])
	}
	return b.String()
}

// c137Compare 跑一档：主路径 createTask vs 直调 createTaskWithActors，比对读回值 + 事件序列。
func c137Compare(t *testing.T, label string, props map[string]interface{}, explicit []string) {
	t.Helper()

	// 主路径：createTask 自己 resolveActors
	var mainEvents, actEvents []string
	flowM, nodeM := c137Node(props)
	em, repoM := c137Harness(&mainEvents)
	instM := c137Inst()
	if err := em.createTask(context.Background(), flowM, nodeM, instM, "boss137", map[string]interface{}{}, 9001); err != nil {
		t.Fatalf("%s 主路径 createTask: %v", label, err)
	}

	// 直调：显式参与者集合＝主路径本会解析出的那一个（先取出来，避免本案把"输入相同"当成前提偷偷假设）
	flowA, nodeA := c137Node(props)
	ea, repoA := c137Harness(&actEvents)
	instA := c137Inst()
	resolved := ea.resolveActors(nodeA, instA, "boss137", map[string]interface{}{})
	if strings.Join(resolved, ",") != strings.Join(explicit, ",") {
		t.Fatalf("%s 夹具失效：显式参与者集合 %v ≠ 主路径解析结果 %v ⇒ 对照不成立", label, explicit, resolved)
	}
	if err := ea.createTaskWithActors(context.Background(), flowA, nodeA, instA, "boss137",
		map[string]interface{}{}, explicit, 9001); err != nil {
		t.Fatalf("%s 直调 createTaskWithActors: %v", label, err)
	}

	gotMain := c137Snapshot(t, repoM, instM.ID)
	gotAct := c137Snapshot(t, repoA, instA.ID)
	if strings.Join(gotMain, "\n") != strings.Join(gotAct, "\n") {
		t.Fatalf("%s 直调与主路径的建单结果不一致：\n主路径 %d 行:\n%s\n直调 %d 行:\n%s",
			label, len(gotMain), strings.Join(gotMain, "\n"), len(gotAct), strings.Join(gotAct, "\n"))
	}
	if strings.Join(mainEvents, "|") != strings.Join(actEvents, "|") {
		t.Fatalf("%s 事件序列不一致：主路径 %v vs 直调 %v", label, mainEvents, actEvents)
	}
	// 至少建出一行（空参与者档另测）——防止"两侧都啥也没干"被当成一致蒙过去
	if len(gotMain) == 0 && len(explicit) > 0 {
		t.Fatalf("%s 两侧都没建出任务，对照无意义: %v", label, gotMain)
	}
	t.Logf("%s ⇒ %d 行一致，事件 %d 条一致", label, len(gotMain), len(mainEvents))
}

// TestIssue137BPlainMultiActor 普通节点（非会签）多参与者：一条任务、全部参与者、perform_type=0。
func TestIssue137BPlainMultiActor(t *testing.T) {
	c137Compare(t, "普通多参与者", map[string]interface{}{
		"assignee": "a137a,a137b", "taskType": 0, "performType": 0, "form": "f137",
	}, []string{"a137a", "a137b"})
}

// TestIssue137BParallelCountersign 并行会签：逐人一条任务，每条 perform_type=1。
func TestIssue137BParallelCountersign(t *testing.T) {
	c137Compare(t, "并行会签", map[string]interface{}{
		"assignee": "a137a,a137b,a137c", "taskType": 0, "performType": "1",
		"countersignType": "PARALLEL", "form": "f137",
	}, []string{"a137a", "a137b", "a137c"})
}

// TestIssue137BSequentialCountersign 顺序会签：只建首位成员一条任务，簿记变量（nrOfInstances/
// loopCounter/operatorList）随行并入，且不能把 isFirstTaskNode 覆写掉。
func TestIssue137BSequentialCountersign(t *testing.T) {
	c137Compare(t, "串行会签", map[string]interface{}{
		"assignee": "a137a,a137b", "taskType": 0, "performType": "1",
		"countersignType": "SEQUENTIAL", "form": "f137",
	}, []string{"a137a", "a137b"})
}

// TestIssue137BUnknownCountersignType 未知 countersignType：两条路都走兜底分支（逐人建单、perform_type=1）。
func TestIssue137BUnknownCountersignType(t *testing.T) {
	c137Compare(t, "未知会签类型兜底", map[string]interface{}{
		"assignee": "a137a,a137b", "taskType": 0, "performType": "1",
		"countersignType": "ALL", "form": "f137",
	}, []string{"a137a", "a137b"})
}

// TestIssue137BSingleActor 单人档：显式一个参与者 ≡ 主路径解析出一个。
func TestIssue137BSingleActor(t *testing.T) {
	c137Compare(t, "单参与者", map[string]interface{}{
		"assignee": "a137a", "taskType": 0, "performType": 0, "form": "f137",
	}, []string{"a137a"})
}

// TestIssue137BApplicantToken 契约特殊值 applicant：主路径换成发起人；直调那侧由调用方算好传入
// ——本案显式传发起人本人，验证"换完之后的集合"两条路同形（不参与 assignee→变量替换的差异）。
func TestIssue137BApplicantToken(t *testing.T) {
	c137Compare(t, "applicant 解析后同形", map[string]interface{}{
		"assignee": "applicant", "taskType": 0, "performType": 0,
	}, []string{"boss137"})
}

// TestIssue137BEmptyActorsCreatesRowInstead 空参与者集合这一档**已于 2026-09-30 改判**：
//
// 旧格叫 TestIssue137BEmptyActorsIsNoop，钉的是"空参与者 ⇒ 不建单、不报错"——
// 那正是 spec 02 §6.1/§6.2 第 3 条点名要拆掉的**死锁黑洞**形状
// （实例停在 state=10 却一条可办行都没有，谁也办不动）。
// owner 09-30 拍「任务类零参与者必须建单」⇒ 现在直调与主路径都必须建**一条 task_state=10 的行**，
// 参与者为空集；空集的落库形状是 `wf_process_task_actor` **一行都不插**
// （不是插一条 actor_id 空串——那是 issues/129/141 B 表"空归属值读全库"的上游进水口）。
// 本格保留旧格另外两半判据（不 panic、不报错），只把"该不该建这一行"翻过来。
func TestIssue137BEmptyActorsCreatesRowInstead(t *testing.T) {
	flow, node := c137Node(map[string]interface{}{"assignee": "", "performType": 0})
	e, repo := c137Harness(nil)
	inst := c137Inst()
	if err := e.createTaskWithActors(context.Background(), flow, node, inst, "boss137",
		map[string]interface{}{}, nil, 9001); err != nil {
		t.Fatalf("空参与者不该报错，实得 %v", err)
	}
	if err := e.createTaskWithActors(context.Background(), flow, node, inst, "boss137",
		map[string]interface{}{}, []string{}, 9001); err != nil {
		t.Fatalf("空切片参与者不该报错，实得 %v", err)
	}
	got := c137Snapshot(t, repo, inst.ID)
	if len(got) != 2 {
		t.Fatalf("两次空参与者调用应各建一行（改判后），实得 %d 行 %v", len(got), got)
	}
	for _, line := range got {
		if !strings.Contains(line, "actors=[]") {
			t.Fatalf("行的参与者必须是空集，实得 %v", line)
		}
		if !strings.Contains(line, "state=10") {
			t.Fatalf("改判后要建的是 DOING 行，实得 %v", line)
		}
	}
	// 主路径同档：解析不到参与者也照样建这一行
	e2, repo2 := c137Harness(nil)
	inst2 := c137Inst()
	if err := e2.createTask(context.Background(), flow, node, inst2, "boss137", map[string]interface{}{}, 9001); err != nil {
		t.Fatalf("主路径空参与者不该报错，实得 %v", err)
	}
	if got2 := c137Snapshot(t, repo2, inst2.ID); len(got2) != 1 || !strings.Contains(got2[0], "actors=[]") {
		t.Fatalf("主路径也要建一行零参与者的 DOING 行，实得 %v", got2)
	}
	// 落库形状：参与者**零行**（不是插一条空串）
	tasks, err := repo2.FindDoingTasks(context.Background(), inst2.ID, nil)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("应恰有一行 DOING，实得 %d 行 err=%v", len(tasks), err)
	}
	if actors, _ := repo2.FindTaskActors(context.Background(), tasks[0].ID); len(actors) != 0 {
		t.Fatalf("零参与者行不得往 wf_process_task_actor 插任何一行（含空串），实得 %v", actors)
	}
}

// TestIssue137BInvariantsHold 直调路径也得兑现两条建单不变量（否则"契约形状义务"是空的）：
// ① parentTaskID 随行落库（传 0 也落 0，不是 nil）；② isFirstTaskNode 行变量随行写入。
// ③ expire_time：节点未配到期表达式 ⇒ 该列保持 nil（issues/126 口径，两条路同形）。
func TestIssue137BInvariantsHold(t *testing.T) {
	for _, ct := range []string{"", "PARALLEL", "SEQUENTIAL"} {
		props := map[string]interface{}{
			"assignee": "a137a,a137b", "taskType": 0, "performType": 0, "form": "f137",
		}
		if ct != "" {
			props["performType"] = "1"
			props["countersignType"] = ct
		}
		flow, node := c137Node(props)
		e, repo := c137Harness(nil)
		inst := c137Inst()
		if err := e.createTaskWithActors(context.Background(), flow, node, inst, "boss137",
			map[string]interface{}{}, []string{"a137a", "a137b"}, 0); err != nil {
			t.Fatalf("countersignType=%q 直调: %v", ct, err)
		}
		tasks, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
		if err != nil || len(tasks) == 0 {
			t.Fatalf("countersignType=%q 应建出任务: %d/%v", ct, len(tasks), err)
		}
		for _, tk := range tasks {
			if tk.ParentTaskID == nil {
				t.Fatalf("countersignType=%q 不变量①破：parentTaskID 为 nil（应落 0）", ct)
			}
			if *tk.ParentTaskID != 0 {
				t.Fatalf("countersignType=%q 发起路径 parentTaskID 应为 0，实得 %d", ct, *tk.ParentTaskID)
			}
			if v, ok := tk.Variables[model.IsFirstTaskNodeKey]; !ok || v != true {
				t.Fatalf("countersignType=%q 不变量②破：行变量 isFirstTaskNode 应随行写入 true，实得 %v/%v", ct, v, ok)
			}
			if tk.ExpireTime != nil {
				t.Fatalf("countersignType=%q 未配到期表达式时 expire_time 应保持 nil，实得 %v", ct, *tk.ExpireTime)
			}
		}
	}
}
