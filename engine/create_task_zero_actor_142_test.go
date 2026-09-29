// issues/142 A 批（go 腿）· 零参与者档：两条建单路**锁死同形** ＋ 聚合根两个新工厂的形状。
//
// `createTaskWithActors` 在本仓是零调用者，但它承担契约形状义务（见 engine_impl.go 上那段
// issues/137 B 的注释），owner 09-29 拍"不删"，并留了判据：
//
//	改动 createTask 的建单语义时，请同步看那一格——它红了就是两条路分叉了。
//
// 本轮把 createTask 的"空参与者 ⇒ 一行不建"改成"空参与者 ⇒ 建一条零参与者 DOING 行"
// （spec 02 §6.2 第 3 条），于是**两条路必须一起改**；这一格就是那把尺子：
// 主路径（自己 resolveActors 得到空集）与直调（显式传 nil / 显式空切片）三份读回值＋事件序列
// 必须逐字一致，且那一致的形状是"有一行"，不是"什么都没有"
// （两侧都啥也没干在这里不算通过——沿用 137 那格的护栏思路）。
//
// 本文件在 **package engine** 内测包内函数（外部测试包够不着 createTask/createTaskWithActors），
// 夹具复用同目录 create_task_with_actors_137_test.go 的 c137Node/c137Harness/c137Inst/c137Snapshot。
package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

func TestIssue142ZeroActorBothPathsBuildOneRow(t *testing.T) {
	props := map[string]interface{}{"performType": 0} // 没有 assignee ⇒ 主路径解析结果为空集

	// 主路径：createTask 自己 resolveActors
	var mainEvents []string
	flowM, nodeM := c137Node(props)
	em, repoM := c137Harness(&mainEvents)
	instM := c137Inst()
	if len(em.resolveActors(nodeM, instM, "boss137", map[string]interface{}{})) != 0 {
		t.Fatal("夹具失效：主路径本应解析到零参与者")
	}
	if err := em.createTask(context.Background(), flowM, nodeM, instM, "boss137", map[string]interface{}{}, 9001); err != nil {
		t.Fatalf("主路径 createTask: %v", err)
	}

	// 直调两档：显式 nil 与显式空切片（各自一份仓，id 序列对齐才可直比）
	var nilEvents, emptyEvents []string
	flowN, nodeN := c137Node(props)
	en, repoN := c137Harness(&nilEvents)
	instN := c137Inst()
	if err := en.createTaskWithActors(context.Background(), flowN, nodeN, instN, "boss137",
		map[string]interface{}{}, nil, 9001); err != nil {
		t.Fatalf("直调 nil 参与者: %v", err)
	}
	flowE, nodeE := c137Node(props)
	ee, repoE := c137Harness(&emptyEvents)
	instE := c137Inst()
	if err := ee.createTaskWithActors(context.Background(), flowE, nodeE, instE, "boss137",
		map[string]interface{}{}, []string{}, 9001); err != nil {
		t.Fatalf("直调空切片参与者: %v", err)
	}

	gotMain := c137Snapshot(t, repoM, instM.ID)
	gotNil := c137Snapshot(t, repoN, instN.ID)
	gotEmpty := c137Snapshot(t, repoE, instE.ID)
	if strings.Join(gotMain, "\n") != strings.Join(gotNil, "\n") ||
		strings.Join(gotMain, "\n") != strings.Join(gotEmpty, "\n") {
		t.Fatalf("零参与者档三条路不一致：\n主路径 %v\n显式 nil %v\n显式空切片 %v", gotMain, gotNil, gotEmpty)
	}
	if strings.Join(mainEvents, "|") != strings.Join(nilEvents, "|") ||
		strings.Join(mainEvents, "|") != strings.Join(emptyEvents, "|") {
		t.Fatalf("零参与者档事件序列不一致：主路径 %v / nil %v / 空切片 %v", mainEvents, nilEvents, emptyEvents)
	}
	// 一致 ≠ 都啥也没干：必须是"建出一行 DOING、参与者空集"
	if len(gotMain) != 1 {
		t.Fatalf("零参与者应建恰好一条 DOING 行（§6.2 第 3 条），实得 %d 行: %v", len(gotMain), gotMain)
	}
	snap := gotMain[0]
	for _, want := range []string{"state=10", "actors=[]", "parent=9001", "first=true", "node=task1"} {
		if !strings.Contains(snap, want) {
			t.Fatalf("快照应含 %q，实得 %s", want, snap)
		}
	}

	tasks, err := repoM.FindDoingTasks(context.Background(), instM.ID, nil)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("读回 DOING 行: %d/%v", len(tasks), err)
	}
	if len(tasks[0].ActorIDs) != 0 {
		t.Fatalf("参与者应为空集，实得 %q", tasks[0].ActorIDs)
	}
	if a, _ := repoM.FindTaskActors(context.Background(), tasks[0].ID); len(a) != 0 {
		t.Fatalf("wf_process_task_actor 不该有行（空归属值＝issues/129 病根），实得 %q", a)
	}
	if tasks[0].Variables[model.IsFirstTaskNodeKey] != true {
		t.Fatalf("建单不变量②破：isFirstTaskNode 应随行写入，实得 %v", tasks[0].Variables)
	}
	// 码 3 照发（一条事件，载荷 actors 为空集）
	if len(mainEvents) != 1 || !strings.HasPrefix(mainEvents[0], "type=3 node=task1") {
		t.Fatalf("零参与者行也应 fire 一次码 3，实得 %v", mainEvents)
	}
	if !strings.Contains(mainEvents[0], "ops=[]") {
		t.Fatalf("码 3 载荷 actors 应为空集，实得 %s", mainEvents[0])
	}
}

// TestIssue142HistoryRowShape 聚合根工厂 CreateHistoryTask 自己的形状（包内直测，
// 不借引擎路径——引擎侧那几格在 custom_node_142_test.go）。
// 逐条对齐 java `ProcessInstance.createHistoryTask`（domain/ProcessInstance.java:425-442）。
func TestIssue142HistoryRowShape(t *testing.T) {
	inst := c137Inst()
	now := time.Now()
	ht := inst.CreateHistoryTask(555, "custom1", "通知外部系统", "boss137", now, 9001, true)
	if ht.TaskState != model.TaskStateDone {
		t.Fatalf("历史行 state 应为 20，实得 %d", ht.TaskState)
	}
	if len(ht.ActorIDs) != 1 || ht.ActorIDs[0] != "boss137" {
		t.Fatalf("留痕主体应为当前操作人，实得 %q", ht.ActorIDs)
	}
	if ht.ActorID != "boss137" {
		t.Fatalf("operator 列应为当前操作人（本栈 DONE 行口径）,实得 %q", ht.ActorID)
	}
	if ht.ParentTaskID == nil || *ht.ParentTaskID != 9001 {
		t.Fatalf("不变量①：parent 应随行落库，实得 %v", ht.ParentTaskID)
	}
	if ht.Variables[model.IsFirstTaskNodeKey] != true {
		t.Fatalf("不变量②：首节点标记应随行写入，实得 %v", ht.Variables)
	}
	if ht.FormKey != "" || ht.PerformType != 0 || ht.TaskType != 0 || ht.ExpireTime != nil {
		t.Fatalf("历史行不该有 form/会签档/到期时间，实得 %+v", ht)
	}
	if ht.FinishTime == nil || !ht.FinishTime.Equal(now) {
		t.Fatalf("历史行 finish_time 应取 now（跟 python；java 那边留 NULL，分叉见交付报告），实得 %v", ht.FinishTime)
	}
	if ht.ID != 555 || ht.ProcessInstanceID != inst.ID {
		t.Fatalf("id/实例归属应照传入落，实得 %d/%d", ht.ID, ht.ProcessInstanceID)
	}
	if ht.CreateUser != "boss137" || ht.UpdateUser != "boss137" {
		t.Fatalf("create/update_user 应为当前操作人，实得 %q/%q", ht.CreateUser, ht.UpdateUser)
	}
	// 挂在聚合根上（java `this.tasks.add(task)` 同形）
	if len(inst.Tasks) != 1 || inst.Tasks[0] != ht {
		t.Fatalf("历史行应 append 进聚合根 tasks，实得 %v", inst.Tasks)
	}

	// operator 为空串 ⇒ 参与者落空集（不往 actor_id 灌空归属值），不 panic
	empty := c137Inst()
	ht2 := empty.CreateHistoryTask(556, "custom1", "通知外部系统", "", now, 0, false)
	if len(ht2.ActorIDs) != 0 {
		t.Fatalf("operator 空串时参与者应为空集，实得 %q", ht2.ActorIDs)
	}
	if ht2.ParentTaskID == nil || *ht2.ParentTaskID != 0 {
		t.Fatalf("不变量①：传 0 也要落 0（不是 NULL），实得 %v", ht2.ParentTaskID)
	}
}

// TestIssue142CreateTaskDelegatesToActorListFactory `CreateTask` 的语义逐字不变：
// 单人壳把那一个人包成集合后交给 CreateTaskWithActors（本轮为了不改动既有建单形状
// 只加了这层委托）。这一格钉"委托没改变形状"，含空串 actor 这个既有调用形状也不动。
func TestIssue142CreateTaskDelegatesToActorListFactory(t *testing.T) {
	now := time.Now()
	instA := c137Inst()
	a := instA.CreateTask(1, "task1", "上级审批", "solo", "boss137", "f1", now, 0, true, 1)
	instB := c137Inst()
	b := instB.CreateTaskWithActors(1, "task1", "上级审批", []string{"solo"}, "boss137", "f1", now, 0, true, 1)
	if a.TaskState != model.TaskStateDoing || b.TaskState != model.TaskStateDoing {
		t.Fatal("两条都该是 DOING")
	}
	if len(a.ActorIDs) != 1 || a.ActorIDs[0] != "solo" || len(b.ActorIDs) != 1 || b.ActorIDs[0] != "solo" {
		t.Fatalf("单人壳与显式集合的参与者应一致，实得 %q / %q", a.ActorIDs, b.ActorIDs)
	}
	if a.CreateUser != b.CreateUser || a.FormKey != b.FormKey || a.PerformType != b.PerformType ||
		a.Variables[model.IsFirstTaskNodeKey] != b.Variables[model.IsFirstTaskNodeKey] {
		t.Fatalf("其余字段应逐字一致：a=%+v b=%+v", a, b)
	}
	// 单人壳显式传空串 actor 的旧形状保持 [""]（本轮没顺手改它——引擎零参与者档走的是
	// CreateTaskWithActors(nil)，见 createTask 那段注释），免得把既有调用点一起掀了
	instC := c137Inst()
	c := instC.CreateTask(2, "task1", "上级审批", "", "boss137", "", now, 0, false)
	if len(c.ActorIDs) != 1 || c.ActorIDs[0] != "" {
		t.Fatalf("CreateTask(\"\") 的既有形状应仍是 [\"\"]，实得 %q", c.ActorIDs)
	}
}
