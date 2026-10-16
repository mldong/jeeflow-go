// audit8 R80 段二（issues/175 §6.5 M4／M5／M6 三组等价档）——办理·回退族五处写点的**上抛有牙**判据。
//
// 病灶回看：R74 把这些写点的错误改成逐调用点上抛，但 §6.5 的变异对照里这三组「摘掉上抛后
// 175 五腿＋R9 tier 七格全绿＝等价」——当年夹具是单参与者线性流，既不走会签两支，也没有一格
// 注入「仓储写失败」。本文件按组补上这一格，五处写点逐处咬合（行号＝本轮现读，案面 §6.5 的号已漂）：
//
//	M4a `executeProcessTaskInner:214` 顺序会签推进建单
//	M4b `executeProcessTaskInner:278` 会签 merged 后废弃残留行
//	M5b `prepareExecuteTask:477`      办理序言的任务行 update
//	M5a `prepareExecuteTask:508`      办理序言的实例变量写回
//	M6  `rollbackToParent:588`        退回上一步的复活血缘行建单
//
// 判据形状与段一（r80_savenewtask_teeth_test.go）一致，三件事缺一不可：
//  1. **前置读数**——对照组（不注入）必须把该支的写次、事件、读回行都算清楚，防"夹具走错支"式假绿；
//  2. **原样透出**——`errors.Is` 认仓储那一枚错误，防被包成别的错（另一种没牙）；
//  3. **抛错即停**——后续写点调用次数不再增长、且读回值停在原地（防"吞掉错误继续跑完"那一形）。
//
// 两处形状说明：
//   - 内存仓 `UpdateInstance` 会**级联**把聚合根内的任务副本写进 store（v1.0.1 语义），但级联走的是
//     `*memory.Repository` 自己的方法、不经过本装饰器 ⇒ 计数器只数**引擎层**调用，判读干净。
//   - M5b 额外钉一条 spec 11-events §11.2 原则 3：任务行 update **失败**时 TASK_COMPLETE(5) 不得发
//     （"不发假事件"）。这一条同时是"吞错继续跑"的独立探针——它比"没往下建单"更难被绕开。
package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// r80ExecRepo 在段一的 SaveTask 之外再多挡两枚：UpdateTask 第 failOnUpdateTask 次、
// UpdateInstance 第 failOnUpdInst 次（0＝不挡）。计数器各数各的，0 档零影响。
type r80ExecRepo struct {
	*memory.Repository
	failOnSave       int
	failOnUpdateTask int
	failOnUpdInst    int
	saveCalls        int
	updateTaskCalls  int
	updInstCalls     int
}

var (
	errR80UpdateTask = errors.New("r80: UpdateTask 注入失败")
	errR80UpdateInst = errors.New("r80: UpdateInstance 注入失败")
)

func (r *r80ExecRepo) SaveTask(ctx context.Context, t *model.ProcessTask) error {
	r.saveCalls++
	if r.failOnSave == r.saveCalls {
		return errR80Save
	}
	return r.Repository.SaveTask(ctx, t)
}

func (r *r80ExecRepo) UpdateTask(ctx context.Context, t *model.ProcessTask) error {
	r.updateTaskCalls++
	if r.failOnUpdateTask == r.updateTaskCalls {
		return errR80UpdateTask
	}
	return r.Repository.UpdateTask(ctx, t)
}

func (r *r80ExecRepo) UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error {
	r.updInstCalls++
	if r.failOnUpdInst == r.updInstCalls {
		return errR80UpdateInst
	}
	return r.Repository.UpdateInstance(ctx, inst)
}

type r80Exec struct {
	eng    *EngineImpl
	repo   *r80ExecRepo
	inst   *model.ProcessInstance
	events []ProcessEvent
}

// 三份内联夹具：线性两节点（M5／M6）／串行会签（M4a）／并行会签＋一票否决（M4b）。
// 内联而不取共享 flows/ 那十六份，是因为本轮判据要钉**写次读数**——共享夹具的节点数、
// 参与者数会被别的轮次改动，写次跟着漂，判据就成了"读数对不上⇒改期望值"的反向操作。
const (
	r80FlowLinear = `{"name":"r80linear","displayName":"R80 线性两节点","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"form":"f1","assignee":"zhangsan","taskType":0,"performType":0},"text":{"value":"初审"}},
  {"id":"task2","type":"snaker:task","properties":{"form":"f1","assignee":"lisi","taskType":0,"performType":0},"text":{"value":"复审"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"task2","properties":{}},
  {"id":"e2","sourceNodeId":"task2","targetNodeId":"end","properties":{}}]}`

	r80FlowSeq = `{"name":"r80seq","displayName":"R80 串行会签","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"form":"f1","assignee":"a1,a2,a3","taskType":0,"performType":1,"countersignType":"SEQUENTIAL"},"text":{"value":"串行会签"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`

	r80FlowVeto = `{"name":"r80veto","displayName":"R80 并行会签一票否决","type":"approval",
 "nodes":[
  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
  {"id":"task1","type":"snaker:task","properties":{"form":"f1","assignee":"a1,a2,a3","taskType":0,"performType":1,"countersignType":"PARALLEL","countersignCompletionCondition":"ONE_VOTE_VETO"},"text":{"value":"并行会签"}},
  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
  {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
  {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`
)

// r80Start 装配内存仓＋引擎＋事件记录器并发起一次，返回时**发起腿已经走完**
// （对照组与注入组都从这里起步，写次读数才对得齐）。
func r80Start(t *testing.T, flowJSON string, inject func(*r80ExecRepo)) *r80Exec {
	t.Helper()
	repo := &r80ExecRepo{Repository: memory.New()}
	if inject != nil {
		inject(repo)
	}
	x := &r80Exec{repo: repo}
	eng := New(repo, c137UserProv{}, &c137IDGen{}, nil)
	eng.SetExtensions(&Extensions{Listeners: []ProcessEventListener{
		func(_ context.Context, evt ProcessEvent) { x.events = append(x.events, evt) },
	}})
	def := &model.ProcessDefine{
		Name: "r80", DisplayName: "R80 写点判据", Type: "approval", State: 1, Version: 1,
		Content: []byte(flowJSON),
	}
	repo.AddDefine(def)
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "boss1",
		map[string]interface{}{"BUSINESS_NO": "BIZ-R80"})
	if err != nil {
		t.Fatalf("发起被打断: %v", err)
	}
	x.eng = eng
	x.inst = inst
	return x
}

func (x *r80Exec) doing(t *testing.T, nodeIDs ...string) []*model.ProcessTask {
	t.Helper()
	var names []string
	if len(nodeIDs) > 0 {
		names = append(names, nodeIDs...)
	}
	tasks, err := x.repo.FindDoingTasks(context.Background(), x.inst.ID, names)
	if err != nil {
		t.Fatalf("读回 DOING 行: %v", err)
	}
	return tasks
}

func (x *r80Exec) doingOne(t *testing.T, nodeID string) *model.ProcessTask {
	t.Helper()
	tasks := x.doing(t, nodeID)
	if len(tasks) != 1 {
		t.Fatalf("%s 应有且只有 1 行 DOING，实读 %d 行", nodeID, len(tasks))
	}
	return tasks[0]
}

func (x *r80Exec) taskOf(t *testing.T, nodeID, actor string) *model.ProcessTask {
	t.Helper()
	// DOING 行的归属看 **ActorIDs**（参与者集合）；`ActorID` 是办结人，只在 Finish 时落（model/instance.go:226）
	for _, tk := range x.doing(t, nodeID) {
		for _, a := range tk.ActorIDs {
			if a == actor {
				return tk
			}
		}
	}
	t.Fatalf("%s 里没有参与者 %s 的 DOING 行", nodeID, actor)
	return nil
}

func r80FirstActor(tk *model.ProcessTask) string {
	if len(tk.ActorIDs) == 0 {
		return ""
	}
	return tk.ActorIDs[0]
}

func (x *r80Exec) countEvent(typ EventType) int {
	n := 0
	for _, e := range x.events {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func (x *r80Exec) instRow(t *testing.T) *model.ProcessInstance {
	t.Helper()
	row, err := x.repo.FindInstanceByID(context.Background(), x.inst.ID)
	if err != nil || row == nil {
		t.Fatalf("读回持久实例行: row=%v err=%v", row, err)
	}
	return row
}

// ─── M4a：顺序会签推进建单（engine_impl.go:214）───────────────────────────────

func TestR80顺序会签推进写点上抛(t *testing.T) {
	// 对照组：首位 a1 办结 ⇒ 推进那一枚 SaveTask 是全局第 2 次，读回 a2 一行
	x := r80Start(t, r80FlowSeq, nil)
	if x.repo.saveCalls != 1 {
		t.Fatalf("前置读数：发起后 SaveTask 应 1 次（串行会签只建首位），实读 %d（夹具形状变了，判据要跟着改）", x.repo.saveCalls)
	}
	if _, err := x.eng.ExecuteProcessTask(context.Background(), x.doingOne(t, "task1").ID, "a1",
		map[string]interface{}{}); err != nil {
		t.Fatalf("对照组不该报错: %v", err)
	}
	if x.repo.saveCalls != 2 {
		t.Fatalf("对照组应走到推进写点（累计 2 次 SaveTask），实读 %d", x.repo.saveCalls)
	}
	if got := r80FirstActor(x.doingOne(t, "task1")); got != "a2" {
		t.Fatalf("对照组推进应落在 a2，实读 %q", got)
	}

	// 注入组：挡在推进那一枚（第 2 次）⇒ 上抛＋停在原地（a1 已办结、a2 那行根本没建）
	y := r80Start(t, r80FlowSeq, func(r *r80ExecRepo) { r.failOnSave = 2 })
	t1 := y.doingOne(t, "task1")
	err := firstErr(y.eng.ExecuteProcessTask(context.Background(), t1.ID, "a1", map[string]interface{}{}))
	if err == nil {
		t.Fatalf("挡在推进写点必须上抛，实得 nil（＝吞错继续跑）")
	}
	if !errors.Is(err, errR80Save) {
		t.Fatalf("要原样透出仓储错误，实得 %v", err)
	}
	if y.repo.saveCalls != 2 {
		t.Fatalf("抛错即停：SaveTask 不得再被调（实读 %d 次）", y.repo.saveCalls)
	}
	if n := len(y.doing(t, "task1")); n != 0 {
		t.Fatalf("抛错即停：推进行不该存在，实读 %d 行", n)
	}
}

// ─── M4b：会签 merged 后废弃残留行（engine_impl.go:278）───────────────────────

func TestR80会签废弃残留行上抛(t *testing.T) {
	// 对照组：a1 软拒绝（submitType=20）＋ ONE_VOTE_VETO ⇒ merged ⇒ 废弃 a2/a3 两行，
	// UpdateTask 第 2、3 次就是这一循环（第 1 次是序言里办 a1 自己那行）
	x := r80Start(t, r80FlowVeto, nil)
	if x.repo.saveCalls != 3 {
		t.Fatalf("前置读数：并行会签应预建 3 行，实读 SaveTask %d 次", x.repo.saveCalls)
	}
	a1 := x.taskOf(t, "task1", "a1")
	if _, err := x.eng.ExecuteProcessTask(context.Background(), a1.ID, "a1",
		map[string]interface{}{"submitType": 20}); err != nil {
		t.Fatalf("对照组不该报错: %v", err)
	}
	if x.repo.updateTaskCalls != 3 {
		t.Fatalf("对照组应废弃 2 行（累计 3 次 UpdateTask），实读 %d", x.repo.updateTaskCalls)
	}
	if n := len(x.doing(t, "task1")); n != 0 {
		t.Fatalf("对照组残留行应全部废弃，实读 %d 行 DOING", n)
	}

	for _, tc := range []struct {
		failOn    int
		wantDoing int
	}{
		{2, 2}, // 挡在第一枚废弃 ⇒ a2/a3 都还在
		{3, 1}, // 挡在第二枚 ⇒ a2 已废弃、a3 没轮到
	} {
		y := r80Start(t, r80FlowVeto, func(r *r80ExecRepo) { r.failOnUpdateTask = tc.failOn })
		b1 := y.taskOf(t, "task1", "a1")
		err := firstErr(y.eng.ExecuteProcessTask(context.Background(), b1.ID, "a1",
			map[string]interface{}{"submitType": 20}))
		if err == nil {
			t.Fatalf("挡在第 %d 次 UpdateTask（废弃循环）必须上抛，实得 nil", tc.failOn)
		}
		if !errors.Is(err, errR80UpdateTask) {
			t.Fatalf("第 %d 次注入要原样透出仓储错误，实得 %v", tc.failOn, err)
		}
		if y.repo.updateTaskCalls != tc.failOn {
			t.Fatalf("抛错即停：第 %d 次之后不得再调 UpdateTask（实读 %d 次）", tc.failOn, y.repo.updateTaskCalls)
		}
		if n := len(y.doing(t, "task1")); n != tc.wantDoing {
			t.Fatalf("第 %d 次注入后该节点应剩 %d 行 DOING，实读 %d 行", tc.failOn, tc.wantDoing, n)
		}
	}
}

// ─── M5b：办理序言的任务行 update（engine_impl.go:477）────────────────────────

func TestR80办理序言任务行写点上抛(t *testing.T) {
	// 对照组：task1 办结 ⇒ UpdateTask 第 1 次＝序言这一处，随后实例写回、推进建 task2
	x := r80Start(t, r80FlowLinear, nil)
	if _, err := x.eng.ExecuteProcessTask(context.Background(), x.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{"f_score": "1"}); err != nil {
		t.Fatalf("对照组不该报错: %v", err)
	}
	if x.repo.updateTaskCalls != 1 || x.repo.updInstCalls != 1 || x.repo.saveCalls != 2 {
		t.Fatalf("前置读数：对照组应 1/1/2（任务行／实例行／建单），实读 %d/%d/%d",
			x.repo.updateTaskCalls, x.repo.updInstCalls, x.repo.saveCalls)
	}
	if n := x.countEvent(EventTaskComplete); n != 1 {
		t.Fatalf("对照组应发一条 TASK_COMPLETE(5)，实读 %d 条", n)
	}

	// 注入组：挡在第 1 次 UpdateTask ⇒ 上抛＋实例没写回＋没推进＋**不发假事件**
	y := r80Start(t, r80FlowLinear, func(r *r80ExecRepo) { r.failOnUpdateTask = 1 })
	err := firstErr(y.eng.ExecuteProcessTask(context.Background(), y.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{"f_score": "1"}))
	if err == nil {
		t.Fatalf("挡在任务行 update 必须上抛，实得 nil")
	}
	if !errors.Is(err, errR80UpdateTask) {
		t.Fatalf("要原样透出仓储错误，实得 %v", err)
	}
	if y.repo.updateTaskCalls != 1 || y.repo.updInstCalls != 0 || y.repo.saveCalls != 1 {
		t.Fatalf("抛错即停：应停在任务行这一格（1/0/1），实读 %d/%d/%d",
			y.repo.updateTaskCalls, y.repo.updInstCalls, y.repo.saveCalls)
	}
	if n := y.countEvent(EventTaskComplete); n != 0 {
		t.Fatalf("spec 11-events §11.2 原则 3：任务行 update 失败不得发 TASK_COMPLETE，实读 %d 条", n)
	}
	if n := len(y.doing(t, "task1")); n != 1 {
		t.Fatalf("任务行 update 失败 ⇒ 该行仍应 DOING，实读 %d 行", n)
	}
}

// ─── M5a：办理序言的实例变量写回（engine_impl.go:508）─────────────────────────

func TestR80办理序言实例变量写点上抛(t *testing.T) {
	x := r80Start(t, r80FlowLinear, nil)
	if _, err := x.eng.ExecuteProcessTask(context.Background(), x.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{"f_score": "1"}); err != nil {
		t.Fatalf("对照组不该报错: %v", err)
	}
	if v, ok := x.instRow(t).Variables["f_score"]; !ok || v != "1" {
		t.Fatalf("前置读数：对照组实例变量应已落库（f_score=1），实读 %v", x.instRow(t).Variables["f_score"])
	}

	// 注入组：挡在实例写回 ⇒ 上抛＋变量没落库＋不推进（task1 已成 FINISHED 行＝令牌卡死，
	// 这正是"吞掉错误继续跑"必须报红的理由：用户视角是流程静默停住）
	y := r80Start(t, r80FlowLinear, func(r *r80ExecRepo) { r.failOnUpdInst = 1 })
	err := firstErr(y.eng.ExecuteProcessTask(context.Background(), y.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{"f_score": "1"}))
	if err == nil {
		t.Fatalf("挡在实例写回必须上抛，实得 nil")
	}
	if !errors.Is(err, errR80UpdateInst) {
		t.Fatalf("要原样透出仓储错误，实得 %v", err)
	}
	if _, ok := y.instRow(t).Variables["f_score"]; ok {
		t.Fatalf("UpdateInstance 失败后实例变量不该落库，实读 %v", y.instRow(t).Variables["f_score"])
	}
	if y.repo.saveCalls != 1 {
		t.Fatalf("抛错即停：不得推进建 task2（SaveTask 实读 %d 次）", y.repo.saveCalls)
	}
	if n := len(y.doing(t)); n != 0 {
		t.Fatalf("对照组形状核对：task1 已 FINISHED 且未建下一行 ⇒ 全实例 DOING 应为 0，实读 %d 行", n)
	}
}

// ─── M6：退回上一步的复活血缘行建单（engine_impl.go:588）──────────────────────

func TestR80回退复活建单写点上抛(t *testing.T) {
	// 走到 task2（SaveTask 第 2 次），再以 targetTaskName="" 触发血缘回退（第 3 次＝本写点）
	x := r80Start(t, r80FlowLinear, nil)
	if _, err := x.eng.ExecuteProcessTask(context.Background(), x.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{}); err != nil {
		t.Fatalf("对照组初审不该报错: %v", err)
	}
	if x.repo.saveCalls != 2 {
		t.Fatalf("前置读数：进入复审后累计 2 次 SaveTask，实读 %d", x.repo.saveCalls)
	}
	if _, err := x.eng.ExecuteAndJumpTask(context.Background(), x.doingOne(t, "task2").ID, "lisi",
		map[string]interface{}{"submitType": 3}, ""); err != nil {
		t.Fatalf("对照组回退不该报错: %v", err)
	}
	if x.repo.saveCalls != 3 {
		t.Fatalf("对照组应走到复活建单（累计 3 次），实读 %d", x.repo.saveCalls)
	}
	if n := len(x.doing(t, "task1")); n != 1 {
		t.Fatalf("对照组回退应复活上一节点一行，实读 %d 行", n)
	}

	// 注入组：挡在复活建单那一枚 ⇒ 上抛＋停在原地（初审行没被重建，复审行已办结）
	y := r80Start(t, r80FlowLinear, func(r *r80ExecRepo) { r.failOnSave = 3 })
	if _, err := y.eng.ExecuteProcessTask(context.Background(), y.doingOne(t, "task1").ID, "zhangsan",
		map[string]interface{}{}); err != nil {
		t.Fatalf("注入组初审不该报错: %v", err)
	}
	_, err := y.eng.ExecuteAndJumpTask(context.Background(), y.doingOne(t, "task2").ID, "lisi",
		map[string]interface{}{"submitType": 3}, "")
	if err == nil {
		t.Fatalf("挡在复活建单必须上抛，实得 nil（＝回退静默失败，实例 DOING 却零待办）")
	}
	if !errors.Is(err, errR80Save) {
		t.Fatalf("要原样透出仓储错误，实得 %v", err)
	}
	if y.repo.saveCalls != 3 {
		t.Fatalf("抛错即停：SaveTask 不得再被调（实读 %d 次）", y.repo.saveCalls)
	}
	if n := len(y.doing(t)); n != 0 {
		t.Fatalf("抛错即停：不得有复活行，实读 %d 行 DOING", n)
	}
}

// firstErr 取 ExecuteProcessTask 的第二返回值（写测试时只关心错误，产出列表在别处已钉）。
func firstErr[T any](_ []T, err error) error { return err }
