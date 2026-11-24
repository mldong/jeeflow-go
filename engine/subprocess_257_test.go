package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// issues/257 定向 T0：夹具全部内联生成，不读/改共享 flows，不连数据库。

type sub257IDGen struct{ n int64 }

func (g *sub257IDGen) NextID() int64 { g.n++; return g.n }

type sub257UserProv struct{}

func (sub257UserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID}, nil
}

type sub257Repo struct {
	*memory.Repository
	writes    int
	saveCalls map[int64]int
}

type sub257LegacyRepo struct{ spi.ProcessRepository }

func newSub257Harness() (*EngineImpl, *sub257Repo) {
	repo := &sub257Repo{Repository: memory.New(), saveCalls: make(map[int64]int)}
	return New(repo, sub257UserProv{}, &sub257IDGen{}, nil), repo
}

func (r *sub257Repo) resetWrites() {
	r.writes = 0
	clear(r.saveCalls)
}

func (r *sub257Repo) SaveTask(ctx context.Context, task *model.ProcessTask) error {
	r.writes++
	r.saveCalls[task.ID]++
	return r.Repository.SaveTask(ctx, task)
}

func (r *sub257Repo) SaveInstance(ctx context.Context, inst *model.ProcessInstance) error {
	r.writes++
	return r.Repository.SaveInstance(ctx, inst)
}

func (r *sub257Repo) UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error {
	r.writes++
	return r.Repository.UpdateInstance(ctx, inst)
}

func (r *sub257Repo) UpdateTask(ctx context.Context, task *model.ProcessTask) error {
	r.writes++
	return r.Repository.UpdateTask(ctx, task)
}

func (r *sub257Repo) AddTaskActor(ctx context.Context, taskID int64, actors []string) error {
	r.writes++
	return r.Repository.AddTaskActor(ctx, taskID, actors)
}

type sub257NodeSpec struct {
	id    string
	typ   string
	props map[string]interface{}
}

func sub257TaskSpec(id, actor string) sub257NodeSpec {
	return sub257NodeSpec{id: id, typ: model.TypeTask, props: map[string]interface{}{
		"assignee": actor, "taskType": 0, "performType": 0, "form": id + "Form",
	}}
}

func sub257SubSpec(id, child string, version interface{}) sub257NodeSpec {
	props := map[string]interface{}{"processName": child, "form": "__wrong__"}
	if version != nil {
		props["version"] = version
	}
	return sub257NodeSpec{id: id, typ: model.TypeSubProcess, props: props}
}

func sub257Flow(name string, nodes []sub257NodeSpec, edges ...[2]string) []byte {
	wireNodes := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		props := node.props
		if props == nil {
			props = map[string]interface{}{}
		}
		wireNodes = append(wireNodes, map[string]interface{}{
			"id": node.id, "type": node.typ, "properties": props, "text": map[string]interface{}{"value": node.id},
		})
	}
	wireEdges := make([]map[string]interface{}, 0, len(edges))
	for i, edge := range edges {
		wireEdges = append(wireEdges, map[string]interface{}{
			"id": "e" + string(rune('A'+i)), "sourceNodeId": edge[0], "targetNodeId": edge[1],
			"properties": map[string]interface{}{},
		})
	}
	content, err := json.Marshal(map[string]interface{}{
		"name": name, "displayName": name, "type": "approval", "nodes": wireNodes, "edges": wireEdges,
	})
	if err != nil {
		panic(err)
	}
	return content
}

func sub257Linear(name, child string, ids ...string) []byte {
	nodes := make([]sub257NodeSpec, 0, len(ids))
	edges := make([][2]string, 0, len(ids)-1)
	for i, id := range ids {
		switch {
		case id == "start":
			nodes = append(nodes, sub257NodeSpec{id: id, typ: model.TypeStart})
		case id == "end":
			nodes = append(nodes, sub257NodeSpec{id: id, typ: model.TypeEnd})
		case strings.HasPrefix(id, "sub"):
			nodes = append(nodes, sub257SubSpec(id, child, 1))
		case id == "apply":
			nodes = append(nodes, sub257TaskSpec(id, "applicant"))
		default:
			actor := "manager"
			if strings.Contains(id, "child") || strings.HasPrefix(id, "first") {
				actor = "leader"
			}
			nodes = append(nodes, sub257TaskSpec(id, actor))
		}
		if i > 0 {
			edges = append(edges, [2]string{ids[i-1], id})
		}
	}
	return sub257Flow(name, nodes, edges...)
}

func sub257AddDefine(repo *sub257Repo, name string, version, state int, content []byte) *model.ProcessDefine {
	def := &model.ProcessDefine{Name: name, DisplayName: name, Type: "approval", State: state, Version: version, Content: content}
	repo.AddDefine(def)
	return def
}

func sub257StartParent(t *testing.T, eng *EngineImpl, repo *sub257Repo, def *model.ProcessDefine) (*model.ProcessInstance, *model.ProcessTask) {
	t.Helper()
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "applicant", nil)
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}
	apply := sub257Task(t, repo, inst.ID, "apply", model.TaskStateDoing)
	if err := repo.AddTaskActor(context.Background(), apply.ID, []string{"applicant"}); err != nil {
		t.Fatalf("add parent applicant: %v", err)
	}
	return inst, apply
}

func sub257OpenChild(t *testing.T, eng *EngineImpl, repo *sub257Repo, parent *model.ProcessInstance, apply *model.ProcessTask) (*model.ProcessInstance, []*model.ProcessTask) {
	t.Helper()
	result, err := eng.ExecuteProcessTask(context.Background(), apply.ID, KeyAutoExecute,
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeApply)})
	if err != nil {
		t.Fatalf("open child: %v", err)
	}
	return sub257OnlyChild(t, repo, parent.ID, "applicant"), result
}

func sub257OnlyChild(t *testing.T, repo *sub257Repo, parentID int64, operator string) *model.ProcessInstance {
	t.Helper()
	rows, _, err := repo.PageInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 100}, operator)
	if err != nil {
		t.Fatalf("page instances: %v", err)
	}
	var found *model.InstanceRow
	for _, row := range rows {
		if row.ParentID != nil && *row.ParentID == parentID {
			if found != nil {
				t.Fatalf("parent %d has more than one child for operator %s", parentID, operator)
			}
			found = row
		}
	}
	if found == nil {
		t.Fatalf("parent %d has no child for operator %s", parentID, operator)
	}
	inst, err := repo.FindInstanceByID(context.Background(), found.ID)
	if err != nil || inst == nil {
		t.Fatalf("load child: inst=%v err=%v", inst, err)
	}
	return inst
}

func sub257Task(t *testing.T, repo *sub257Repo, instanceID int64, node string, state model.TaskState) *model.ProcessTask {
	t.Helper()
	tasks, err := repo.FindHistoryTasks(context.Background(), instanceID)
	if err != nil {
		t.Fatalf("history tasks: %v", err)
	}
	var found *model.ProcessTask
	for _, task := range tasks {
		if task.TaskName == node && task.TaskState == state {
			if found != nil {
				t.Fatalf("instance %d node %s state %d has duplicate rows", instanceID, node, state)
			}
			found = task
		}
	}
	if found == nil {
		t.Fatalf("instance %d node %s state %d not found", instanceID, node, state)
	}
	return found
}

func sub257Doing(t *testing.T, repo *sub257Repo, instanceID int64) []*model.ProcessTask {
	t.Helper()
	tasks, err := repo.FindDoingTasks(context.Background(), instanceID, nil)
	if err != nil {
		t.Fatalf("doing tasks: %v", err)
	}
	return tasks
}

func sub257Approve(t *testing.T, eng *EngineImpl, repo *sub257Repo, task *model.ProcessTask, actor string, args map[string]interface{}) []*model.ProcessTask {
	t.Helper()
	if err := repo.AddTaskActor(context.Background(), task.ID, []string{actor}); err != nil {
		t.Fatalf("add actor: %v", err)
	}
	if args == nil {
		args = map[string]interface{}{KeySubmitType: int(model.SubmitTypeAgree)}
	}
	result, err := eng.ExecuteProcessTask(context.Background(), task.ID, actor, args)
	if err != nil {
		t.Fatalf("approve %s: %v", task.TaskName, err)
	}
	return result
}

func sub257RecordEvents(eng *EngineImpl, repo *sub257Repo) (*[]ProcessEvent, map[int64]model.InstanceState) {
	events := new([]ProcessEvent)
	endStates := make(map[int64]model.InstanceState)
	eng.SetExtensions(&Extensions{Listeners: []ProcessEventListener{func(ctx context.Context, event ProcessEvent) {
		*events = append(*events, event)
		if event.Type == EventProcessInstanceEnd {
			if inst, _ := repo.FindInstanceByID(ctx, event.InstanceID); inst != nil {
				endStates[event.InstanceID] = inst.State
			}
		}
	}}})
	return events, endStates
}

func sub257CountEvent(events []ProcessEvent, typ EventType, instanceID int64, node string) int {
	count := 0
	for _, event := range events {
		if event.Type == typ && event.InstanceID == instanceID && (node == "" || event.NodeID == node) {
			count++
		}
	}
	return count
}

func sub257EventIndex(events []ProcessEvent, typ EventType, instanceID int64, node string) int {
	for i, event := range events {
		if event.Type == typ && event.InstanceID == instanceID && (node == "" || event.NodeID == node) {
			return i
		}
	}
	return -1
}

func Test257ParentChildClosedLoopAndPersistedResults(t *testing.T) {
	eng, repo := newSub257Harness()
	childDef := sub257AddDefine(repo, "child257", 1, 1,
		sub257Linear("child257", "", "start", "apply", "childReview", "end"))
	parentDef := sub257AddDefine(repo, "parent257", 1, 1,
		sub257Linear("parent257", "child257", "start", "apply", "sub257", "parentAfter", "end"))
	events, endStates := sub257RecordEvents(eng, repo)
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	args := map[string]interface{}{
		KeySubmitType: int(model.SubmitTypeApply), "f_keep": "business",
		"tf_parentSecret": "drop", KeyNextNodeOperator: "drop-next", KeyProcessStartNextNodeOperator: "drop-start-next",
		VarBranchCtx: "drop-branch", VarReapplyCtx: "drop-reapply",
	}
	result, err := eng.ExecuteProcessTask(context.Background(), apply.ID, KeyAutoExecute, args)
	if err != nil {
		t.Fatalf("execute parent apply: %v", err)
	}
	child := sub257OnlyChild(t, repo, parent.ID, "applicant")
	if child.DefineID != childDef.ID || child.ParentID == nil || *child.ParentID != parent.ID || child.ParentNodeName != "sub257" {
		t.Fatalf("child identity/parent mismatch: %+v", child)
	}
	if child.Operator != "applicant" || child.State != model.InstanceStateDoing {
		t.Fatalf("child operator/state mismatch: operator=%s state=%d", child.Operator, child.State)
	}
	// startAndExecute 自办会按常规把 submitType=0 写进子实例变量；启动参数的剥离由
	// subProcessChildArgs 直接验证，其余内部控制键则不能在子实例中残留。
	for _, key := range []string{"tf_parentSecret", KeyNextNodeOperator, KeyProcessStartNextNodeOperator, VarBranchCtx, VarReapplyCtx} {
		if _, ok := child.Variables[key]; ok {
			t.Errorf("child args retained forbidden key %s", key)
		}
	}
	if child.Variables["f_keep"] != "business" {
		t.Errorf("child args lost business key: %v", child.Variables)
	}
	childApply := sub257Task(t, repo, child.ID, "apply", model.TaskStateDone)
	childReview := sub257Task(t, repo, child.ID, "childReview", model.TaskStateDoing)
	if childApply.ActorID != KeyAutoExecute || childApply.Variables[KeySubmitType] != int(model.SubmitTypeApply) {
		t.Errorf("child apply must be flow.auto + submitType=0: %+v", childApply)
	}
	actors, _ := repo.FindTaskActors(context.Background(), childApply.ID)
	if len(actors) != 1 || actors[0] != "applicant" {
		t.Errorf("child apply actors = %v", actors)
	}
	if len(result) != 2 {
		t.Fatalf("persisted result size = %d, want child apply + child review", len(result))
	}
	seen := make(map[int64]bool)
	for _, task := range result {
		if seen[task.ID] {
			t.Errorf("persisted result duplicates task %d", task.ID)
		}
		seen[task.ID] = true
	}
	if !seen[childApply.ID] || !seen[childReview.ID] {
		t.Errorf("persisted result missing child tasks: %v", seen)
	}
	for _, task := range result {
		switch task.ID {
		case childApply.ID:
			if task.TaskState != model.TaskStateDone {
				t.Errorf("persisted child apply is stale: state=%d", task.TaskState)
			}
		case childReview.ID:
			if task.TaskState != model.TaskStateDoing {
				t.Errorf("persisted child review state = %d", task.TaskState)
			}
		}
	}
	for _, task := range []*model.ProcessTask{childApply, childReview} {
		if repo.saveCalls[task.ID] != 1 {
			t.Errorf("task %d SaveTask calls = %d, want 1", task.ID, repo.saveCalls[task.ID])
		}
		if sub257CountEvent(*events, EventProcessTaskStart, child.ID, task.TaskName) != 1 {
			t.Errorf("task %s TASK_START not exactly once", task.TaskName)
		}
	}
	if len(sub257Doing(t, repo, parent.ID)) != 0 || parent.State != model.InstanceStateDoing {
		t.Fatal("parent outgoing task must wait for child end")
	}

	newTasks := sub257Approve(t, eng, repo, childReview, "leader", map[string]interface{}{
		KeySubmitType: int(model.SubmitTypeAgree), "childResult": "ready",
	})
	parentAfter := sub257Task(t, repo, parent.ID, "parentAfter", model.TaskStateDoing)
	if parentAfter.ProcessInstanceID != parent.ID || parentAfter.FormKey != "parentAfterForm" || parentAfter.ParentTaskID == nil || *parentAfter.ParentTaskID != 0 {
		t.Errorf("parent continuation row mismatch: %+v", parentAfter)
	}
	if len(newTasks) != 1 || newTasks[0].ID != parentAfter.ID {
		t.Errorf("child completion new tasks = %+v, want parentAfter only", newTasks)
	}
	parentFresh, _ := repo.FindInstanceByID(context.Background(), parent.ID)
	if parentFresh.Variables["childResult"] != "ready" {
		t.Errorf("parent variables not persisted on task continuation: %v", parentFresh.Variables)
	}
	childFresh, _ := repo.FindInstanceByID(context.Background(), child.ID)
	if childFresh.State != model.InstanceStateDone {
		t.Errorf("child state = %d, want done", childFresh.State)
	}
	childEnd := sub257EventIndex(*events, EventProcessInstanceEnd, child.ID, "")
	parentTaskStart := sub257EventIndex(*events, EventProcessTaskStart, parent.ID, "parentAfter")
	if childEnd < 0 || parentTaskStart < 0 || childEnd >= parentTaskStart {
		t.Errorf("child END must precede parent continuation: childEnd=%d parentTaskStart=%d", childEnd, parentTaskStart)
	}
	if endStates[child.ID] != model.InstanceStateDone {
		t.Errorf("child END observed before terminal persistence: %d", endStates[child.ID])
	}
	sub257Approve(t, eng, repo, parentAfter, "manager", nil)
	parentFresh, _ = repo.FindInstanceByID(context.Background(), parent.ID)
	if parentFresh.State != model.InstanceStateDone {
		t.Errorf("parent state = %d, want done", parentFresh.State)
	}
	for _, instanceID := range []int64{parent.ID, child.ID} {
		if sub257CountEvent(*events, EventProcessInstanceStart, instanceID, "") != 1 ||
			sub257CountEvent(*events, EventProcessInstanceEnd, instanceID, "") != 1 {
			t.Errorf("instance %d START/END not exactly once", instanceID)
		}
	}
	if sub257CountEvent(*events, EventProcessTaskStart, parent.ID, "parentAfter") != 1 || repo.saveCalls[parentAfter.ID] != 1 {
		t.Error("parent outgoing edge/task executed more than once")
	}
}

func Test257NoApplySelfCompletesFirstTask(t *testing.T) {
	eng, repo := newSub257Harness()
	sub257AddDefine(repo, "childNoApply257", 1, 1,
		sub257Linear("childNoApply257", "", "start", "first257", "second257", "end"))
	parentDef := sub257AddDefine(repo, "parentNoApply257", 1, 1,
		sub257Linear("parentNoApply257", "childNoApply257", "start", "apply", "subNoApply257", "end"))
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	child, _ := sub257OpenChild(t, eng, repo, parent, apply)
	sub257Task(t, repo, child.ID, "second257", model.TaskStateDoing)
	first := sub257Task(t, repo, child.ID, "first257", model.TaskStateDone)
	if first.ActorID != KeyAutoExecute || first.Variables[KeySubmitType] != int(model.SubmitTypeApply) {
		t.Errorf("no-apply first task was not self-completed correctly: %+v", first)
	}
	actors, _ := repo.FindTaskActors(context.Background(), first.ID)
	if len(actors) != 2 || !strings.Contains(strings.Join(actors, ","), "applicant") {
		t.Errorf("no-apply first task actors = %v, want original assignee plus applicant", actors)
	}
}

func Test257MissingDefinitionHasZeroWrites(t *testing.T) {
	eng, repo := newSub257Harness()
	parentDef := sub257AddDefine(repo, "parentMissing257", 1, 1,
		sub257Linear("parentMissing257", "missing257", "start", "apply", "subMissing257", "end"))
	events, _ := sub257RecordEvents(eng, repo)
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	repo.resetWrites()
	eventCount := len(*events)
	_, beforeTotal, _ := repo.PageInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 100}, "applicant")

	_, err := eng.ExecuteProcessTask(context.Background(), apply.ID, KeyAutoExecute,
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeApply)})
	if err == nil {
		t.Fatal("missing child definition must fail")
	}
	var subErr *SubProcessDefineNotFoundError
	if !errors.As(err, &subErr) || subErr.Code() != 20010002 || err.Error() != "子流程missing257定义不存在" {
		t.Fatalf("missing definition error mismatch: %T %v code=%v", err, err, subErr)
	}
	if repo.writes != 0 {
		t.Errorf("missing definition performed %d writes, want 0", repo.writes)
	}
	if len(*events) != eventCount {
		t.Errorf("missing definition fired events: before=%d after=%d", eventCount, len(*events))
	}
	applyFresh := sub257Task(t, repo, parent.ID, "apply", model.TaskStateDoing)
	if applyFresh.ID != apply.ID {
		t.Error("parent apply row changed")
	}
	_, afterTotal, _ := repo.PageInstances(context.Background(), spi.PageQuery{PageNum: 1, PageSize: 100}, "applicant")
	if beforeTotal != 1 || afterTotal != beforeTotal {
		t.Errorf("instance count changed: before=%d after=%d", beforeTotal, afterTotal)
	}
}

func Test257ProcessNameFallbackAliasesAndVersions(t *testing.T) {
	parentArgs := map[string]interface{}{
		"f_keep": "business", KeySubmitType: 1, "tf_secret": "drop", "tf_": "drop",
		KeyProcessStartNextNodeOperator: "drop", KeyNextNodeOperator: "drop",
		VarBranchCtx: "drop", VarReapplyCtx: "drop",
	}
	childArgs := subProcessChildArgs(parentArgs)
	if len(childArgs) != 1 || childArgs["f_keep"] != "business" {
		t.Errorf("child args were not sanitized: %v", childArgs)
	}
	childArgs["f_keep"] = "changed"
	if parentArgs["f_keep"] != "business" {
		t.Error("child args alias the parent argument map")
	}
	parent := &model.ProcessInstance{Operator: "applicant"}
	for _, execution := range []string{KeyAutoExecute, KeyAdminID} {
		if got := subProcessRealOperator(execution, parent); got != "applicant" {
			t.Errorf("execution %s real operator = %s", execution, got)
		}
	}
	if got := subProcessRealOperator("leader", parent); got != "leader" {
		t.Errorf("explicit real operator overwritten: %s", got)
	}

	cases := []struct {
		name, typ, id, want string
		props               map[string]interface{}
		version             *int
	}{
		{name: "processName", typ: model.TypeSubProcess, id: "sub1", want: "target", version: sub257IntPtr(1), props: map[string]interface{}{"processName": "target", "name": "wrong", "form": "kept", "version": float64(1)}},
		{name: "name", typ: "subProcess", id: "sub2", want: "target", props: map[string]interface{}{"name": "target", "form": "kept"}},
		{name: "id ignores subProcessName", typ: "subprocess", id: "target", want: "target", props: map[string]interface{}{"subProcessName": "wrong", "form": "kept"}},
		{name: "wf alias", typ: "wfSubProcess", id: "sub7", want: "target", props: map[string]interface{}{"processName": "target"}},
		{name: "snaker lowercase alias", typ: "snaker:subprocess", id: "sub8", want: "target", props: map[string]interface{}{"processName": "target"}},
		{name: "snaker wf alias", typ: "snaker:wfSubProcess", id: "sub9", want: "target", props: map[string]interface{}{"processName": "target"}},
		{name: "blank processName falls back", typ: model.TypeSubProcess, id: "sub4", want: "target", props: map[string]interface{}{"processName": "  ", "name": "target"}},
		{name: "fractional version is not truncated", typ: model.TypeSubProcess, id: "sub5", want: "sub5", version: sub257IntPtr(-1), props: map[string]interface{}{"version": 1.5}},
		{name: "invalid version is not zero", typ: model.TypeSubProcess, id: "sub6", want: "sub6", version: sub257IntPtr(-1), props: map[string]interface{}{"version": "bad"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &model.FlowNode{ID: tc.id, Type: tc.typ, Properties: tc.props}
			sub := model.ParseSubProcessModel(node)
			if sub == nil || sub.ProcessName != tc.want || sub.ID != tc.id {
				t.Fatalf("parsed subprocess = %+v, want id=%s name=%s", sub, tc.id, tc.want)
			}
			if tc.props["form"] == "kept" && sub.Form != "kept" {
				t.Errorf("form not retained: %q", sub.Form)
			}
			if tc.version == nil && sub.Version != nil {
				t.Errorf("version = %d, want nil", *sub.Version)
			}
			if tc.version != nil && (sub.Version == nil || *sub.Version != *tc.version) {
				t.Errorf("version = %v, want %d", sub.Version, *tc.version)
			}
		})
	}

	eng, repo := newSub257Harness()
	v1 := sub257AddDefine(repo, "versioned257", 1, 1,
		sub257Linear("versioned257", "", "start", "reviewV1", "end"))
	v2 := sub257AddDefine(repo, "versioned257", 2, 0,
		sub257Linear("versioned257", "", "start", "reviewV2", "end"))
	v2NewestID := sub257AddDefine(repo, "versioned257", 2, 0,
		sub257Linear("versioned257", "", "start", "reviewV2Newest", "end"))
	latest, err := repo.FindDefineByNameAndVersion(context.Background(), "versioned257", nil)
	if err != nil || latest == nil || latest.ID != v2NewestID.ID {
		t.Fatalf("latest definition mismatch: %+v err=%v", latest, err)
	}
	exactVersion := 1
	exact, err := repo.FindDefineByNameAndVersion(context.Background(), "versioned257", &exactVersion)
	if err != nil || exact == nil || exact.ID != v1.ID {
		t.Fatalf("explicit definition mismatch: %+v err=%v", exact, err)
	}
	legacy := sub257LegacyRepo{ProcessRepository: repo}
	fallbackLatest, err := findSubProcessDefine(context.Background(), legacy,
		&model.SubProcessModel{ProcessName: "versioned257"})
	if err != nil || fallbackLatest == nil || fallbackLatest.ID != v2NewestID.ID {
		t.Fatalf("legacy repository latest fallback mismatch: %+v err=%v", fallbackLatest, err)
	}
	fallbackExact, err := findSubProcessDefine(context.Background(), legacy,
		&model.SubProcessModel{ProcessName: "versioned257", Version: &exactVersion})
	if err != nil || fallbackExact == nil || fallbackExact.ID != v1.ID {
		t.Fatalf("legacy repository exact fallback mismatch: %+v err=%v", fallbackExact, err)
	}
	_ = v2 // 同版本第二条定义专门验证 version desc,id desc 的 id 次序。

	latestParent := sub257AddDefine(repo, "parentLatest257", 1, 1, sub257Flow("parentLatest257", []sub257NodeSpec{
		{id: "start", typ: model.TypeStart}, sub257TaskSpec("apply", "applicant"),
		sub257SubSpec("subLatest257", "versioned257", nil), {id: "end", typ: model.TypeEnd},
	}, [2]string{"start", "apply"}, [2]string{"apply", "subLatest257"}, [2]string{"subLatest257", "end"}))
	parent, apply := sub257StartParent(t, eng, repo, latestParent)
	child, _ := sub257OpenChild(t, eng, repo, parent, apply)
	if child.DefineID != v2NewestID.ID {
		t.Errorf("latest child define = %d, want %d", child.DefineID, v2NewestID.ID)
	}
	sub257Task(t, repo, child.ID, "reviewV2Newest", model.TaskStateDone)

	exactParent := sub257AddDefine(repo, "parentExact257", 1, 1, sub257Flow("parentExact257", []sub257NodeSpec{
		{id: "start", typ: model.TypeStart}, sub257TaskSpec("apply", "applicant"),
		sub257SubSpec("subExact257", "versioned257", 1), {id: "end", typ: model.TypeEnd},
	}, [2]string{"start", "apply"}, [2]string{"apply", "subExact257"}, [2]string{"subExact257", "end"}))
	parent, apply = sub257StartParent(t, eng, repo, exactParent)
	child, _ = sub257OpenChild(t, eng, repo, parent, apply)
	if child.DefineID != v1.ID {
		t.Errorf("explicit child define = %d, want %d", child.DefineID, v1.ID)
	}
	sub257Task(t, repo, child.ID, "reviewV1", model.TaskStateDone)
}

func Test257RejectionCannotCrossSubprocessOrCountersign(t *testing.T) {
	var flow model.FlowModel
	if err := json.Unmarshal(sub257Linear("guard257", "child257", "start", "apply", "subGuard257", "after257", "end"), &flow); err != nil {
		t.Fatal(err)
	}
	if canRejected(&flow, "after257", "apply") {
		t.Error("rejection crossed a subprocess source")
	}

	for _, tc := range []struct {
		perform interface{}
		allow   bool
	}{
		{perform: nil, allow: true}, {perform: 0, allow: true}, {perform: "ANY", allow: true}, {perform: "NORMAL", allow: true},
		{perform: 1, allow: false}, {perform: "ALL", allow: false}, {perform: "COUNTERSIGN", allow: false}, {perform: 2, allow: false},
	} {
		props := map[string]interface{}{"assignee": "leader", "taskType": 0, "form": "csForm"}
		if tc.perform != nil {
			props["performType"] = tc.perform
		}
		content := sub257Flow("cs257", []sub257NodeSpec{
			{id: "start", typ: model.TypeStart}, sub257TaskSpec("apply", "applicant"),
			{id: "cs", typ: model.TypeTask, props: props}, sub257TaskSpec("after", "manager"), {id: "end", typ: model.TypeEnd},
		}, [2]string{"start", "apply"}, [2]string{"apply", "cs"}, [2]string{"cs", "after"}, [2]string{"after", "end"})
		if err := json.Unmarshal(content, &flow); err != nil {
			t.Fatal(err)
		}
		if got := canRejected(&flow, "after", "cs"); got != tc.allow {
			t.Errorf("performType=%v canRejected=%v, want %v", tc.perform, got, tc.allow)
		}
	}
}

func Test257ChildRejectPersistsThenContinuesParent(t *testing.T) {
	eng, repo := newSub257Harness()
	sub257AddDefine(repo, "childReject257", 1, 1,
		sub257Linear("childReject257", "", "start", "apply", "childReview", "end"))
	parentDef := sub257AddDefine(repo, "parentReject257", 1, 1,
		sub257Linear("parentReject257", "childReject257", "start", "apply", "subReject257", "parentAfter", "end"))
	events, endStates := sub257RecordEvents(eng, repo)
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	child, _ := sub257OpenChild(t, eng, repo, parent, apply)
	childReview := sub257Task(t, repo, child.ID, "childReview", model.TaskStateDoing)
	if err := repo.AddTaskActor(context.Background(), childReview.ID, []string{"leader"}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.ExecuteAndJumpToEnd(context.Background(), childReview.ID, "leader",
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeReject)}); err != nil {
		t.Fatalf("reject child: %v", err)
	}
	childFresh, _ := repo.FindInstanceByID(context.Background(), child.ID)
	if childFresh.State != model.InstanceStateReject || endStates[child.ID] != model.InstanceStateReject {
		t.Errorf("child reject state/event mismatch: state=%d observed=%d", childFresh.State, endStates[child.ID])
	}
	parentAfter := sub257Task(t, repo, parent.ID, "parentAfter", model.TaskStateDoing)
	if parentAfter.ParentTaskID == nil || *parentAfter.ParentTaskID != 0 {
		t.Errorf("parent task inherited child lineage: %+v", parentAfter.ParentTaskID)
	}
	if sub257CountEvent(*events, EventProcessInstanceEnd, child.ID, "") != 1 {
		t.Error("child reject END not exactly once")
	}
	sub257Approve(t, eng, repo, parentAfter, "manager", nil)
	parentFresh, _ := repo.FindInstanceByID(context.Background(), parent.ID)
	if parentFresh.State != model.InstanceStateDone {
		t.Errorf("parent did not close after child reject: %d", parentFresh.State)
	}
}

func sub257ForkChildFlow() []byte {
	return sub257Flow("childGuard257", []sub257NodeSpec{
		{id: "start", typ: model.TypeStart}, sub257TaskSpec("apply", "applicant"), {id: "fork", typ: model.TypeFork},
		sub257TaskSpec("taskA", "leader"), sub257TaskSpec("taskB", "manager"), {id: "end", typ: model.TypeEnd},
	}, [2]string{"start", "apply"}, [2]string{"apply", "fork"}, [2]string{"fork", "taskA"},
		[2]string{"fork", "taskB"}, [2]string{"taskA", "end"}, [2]string{"taskB", "end"})
}

func sub257GuardParent(t *testing.T, eng *EngineImpl, repo *sub257Repo) (*model.ProcessInstance, *model.ProcessInstance) {
	t.Helper()
	sub257AddDefine(repo, "childGuard257", 1, 1, sub257ForkChildFlow())
	parentDef := sub257AddDefine(repo, "parentGuard257", 1, 1,
		sub257Linear("parentGuard257", "childGuard257", "start", "apply", "subGuard257", "parentAfter", "end"))
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	child, _ := sub257OpenChild(t, eng, repo, parent, apply)
	return parent, child
}

func Test257ChildEndRejectsUnfinishedMajorTask(t *testing.T) {
	eng, repo := newSub257Harness()
	events, _ := sub257RecordEvents(eng, repo)
	parent, child := sub257GuardParent(t, eng, repo)
	taskA := sub257Task(t, repo, child.ID, "taskA", model.TaskStateDoing)
	if err := repo.AddTaskActor(context.Background(), taskA.ID, []string{"leader"}); err != nil {
		t.Fatal(err)
	}
	_, err := eng.ExecuteProcessTask(context.Background(), taskA.ID, "leader",
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeAgree)})
	if err == nil || err.Error() != "存在未完成的主办任务,请确认." {
		t.Fatalf("unfinished major guard error = %v", err)
	}
	sub257Task(t, repo, child.ID, "taskB", model.TaskStateDoing)
	childFresh, _ := repo.FindInstanceByID(context.Background(), child.ID)
	if childFresh.State != model.InstanceStateDoing || len(sub257Doing(t, repo, parent.ID)) != 0 {
		t.Error("failed child end advanced child/parent")
	}
	if sub257CountEvent(*events, EventProcessInstanceEnd, child.ID, "") != 0 {
		t.Error("failed child end fired END")
	}
}

func Test257ChildEndAutoCompletesSecondaryTask(t *testing.T) {
	eng, repo := newSub257Harness()
	events, _ := sub257RecordEvents(eng, repo)
	parent, child := sub257GuardParent(t, eng, repo)
	taskA := sub257Task(t, repo, child.ID, "taskA", model.TaskStateDoing)
	taskB := sub257Task(t, repo, child.ID, "taskB", model.TaskStateDoing)
	taskB.TaskType = int(model.TaskTypeSecondary)
	if err := repo.UpdateTask(context.Background(), taskB); err != nil {
		t.Fatal(err)
	}
	if err := repo.AddTaskActor(context.Background(), taskA.ID, []string{"leader"}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), taskA.ID, "leader",
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeAgree)}); err != nil {
		t.Fatalf("child end with secondary: %v", err)
	}
	secondary := sub257Task(t, repo, child.ID, "taskB", model.TaskStateDone)
	if secondary.ActorID != KeyAutoExecute {
		t.Errorf("secondary actor = %s, want flow.auto", secondary.ActorID)
	}
	sub257Task(t, repo, parent.ID, "parentAfter", model.TaskStateDoing)
	if sub257CountEvent(*events, EventProcessInstanceEnd, child.ID, "") != 1 {
		t.Error("child END not exactly once after secondary completion")
	}
}

func Test257SynchronousChildContinuationOnceAndResultChannel(t *testing.T) {
	eng, repo := newSub257Harness()
	sub257AddDefine(repo, "childSync257", 1, 1,
		sub257Linear("childSync257", "", "start", "apply", "end"))
	parentDef := sub257AddDefine(repo, "parentSync257", 1, 1,
		sub257Linear("parentSync257", "childSync257", "start", "apply", "subSync257", "parentAfter", "end"))
	events, _ := sub257RecordEvents(eng, repo)
	parent, apply := sub257StartParent(t, eng, repo, parentDef)
	result, err := eng.ExecuteProcessTask(context.Background(), apply.ID, KeyAutoExecute,
		map[string]interface{}{KeySubmitType: int(model.SubmitTypeApply)})
	if err != nil {
		t.Fatalf("start synchronous child: %v", err)
	}
	child := sub257OnlyChild(t, repo, parent.ID, "applicant")
	parentAfter := sub257Task(t, repo, parent.ID, "parentAfter", model.TaskStateDoing)
	childApply := sub257Task(t, repo, child.ID, "apply", model.TaskStateDone)
	if len(result) != 2 {
		t.Fatalf("synchronous persisted result size = %d, want child apply + parentAfter", len(result))
	}
	seen := make(map[int64]bool)
	for _, task := range result {
		if seen[task.ID] {
			t.Errorf("duplicate persisted task %d", task.ID)
		}
		seen[task.ID] = true
	}
	if !seen[childApply.ID] || !seen[parentAfter.ID] {
		t.Errorf("synchronous persisted result missing tasks: %v", seen)
	}
	if repo.saveCalls[childApply.ID] != 1 || repo.saveCalls[parentAfter.ID] != 1 ||
		sub257CountEvent(*events, EventProcessTaskStart, parent.ID, "parentAfter") != 1 ||
		sub257CountEvent(*events, EventProcessInstanceEnd, child.ID, "") != 1 {
		t.Error("synchronous child caused duplicate task/event/continuation")
	}
}

func Test257NonSubprocessRegression(t *testing.T) {
	eng, repo := newSub257Harness()
	events, _ := sub257RecordEvents(eng, repo)
	def := sub257AddDefine(repo, "plain257", 1, 1,
		sub257Linear("plain257", "", "start", "apply", "review", "end"))
	inst, apply := sub257StartParent(t, eng, repo, def)
	result, err := eng.ExecuteProcessTask(context.Background(), apply.ID, "applicant", nil)
	if err != nil || len(result) != 1 || result[0].TaskName != "review" {
		t.Fatalf("plain apply result=%+v err=%v", result, err)
	}
	review := sub257Task(t, repo, inst.ID, "review", model.TaskStateDoing)
	sub257Approve(t, eng, repo, review, "manager", nil)
	fresh, _ := repo.FindInstanceByID(context.Background(), inst.ID)
	if fresh.State != model.InstanceStateDone || sub257CountEvent(*events, EventProcessInstanceEnd, inst.ID, "") != 1 {
		t.Error("plain flow regression")
	}
}

func sub257IntPtr(v int) *int { return &v }
