package jeeflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// issues/259：任务节点关联岗位与表单指定岗位。
// 断言直接落在 handler 返回值、SPI 调用顺序与内存仓运行时注册上。
type postAssignmentProvider struct {
	members map[string][]string
	errors  map[string]error
	calls   []string
}

func newPostAssignmentProvider() *postAssignmentProvider {
	return &postAssignmentProvider{
		members: map[string][]string{
			"postA":     {"userA", "shared"},
			"postB":     {"userB", "shared"},
			"postC":     {"userC1", "userC2"},
			"postDirty": {" userA ", "", "shared", "userA", "0", " 0 "},
			"postRaw":   {" userB ", "userA", "userB", ""},
			"0":         {"zeroUser"},
		},
		errors: map[string]error{"postErr": errors.New("post lookup failed")},
	}
}

func (p *postAssignmentProvider) FindByPost(postCode string) ([]string, error) {
	p.calls = append(p.calls, postCode)
	if err := p.errors[postCode]; err != nil {
		return nil, err
	}
	return p.members[postCode], nil
}

func postAssignmentHandler(t *testing.T, name string, provider spi.PostUserProvider) engine.IAssignmentHandler {
	t.Helper()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, nil, nil, provider)
	h := reg.ResolveAssignment(name)
	if h == nil {
		t.Fatalf("注册名 %s 解析不到处理器", name)
	}
	return h
}

func assignPostField(t *testing.T, name, nodeKey string, vars map[string]interface{}, provider spi.PostUserProvider) []string {
	t.Helper()
	h := postAssignmentHandler(t, name, provider)
	var node *model.FlowNode
	if nodeKey != "" {
		node = &model.FlowNode{ID: nodeKey}
	}
	return h.Assign(node, &model.ProcessInstance{Variables: vars}, "clerk")
}

func postAssignmentEq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("实得 %q，want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("实得 %q，want %q", got, want)
		}
	}
}

func TestFormPostAssignmentFieldLookup259(t *testing.T) {
	cases := []struct {
		name    string
		nodeKey string
		vars    map[string]interface{}
		want    []string
		calls   []string
	}{
		{"f_前缀优先于裸名", "posts", map[string]interface{}{"f_posts": "postA", "posts": "postB"}, []string{"userA", "shared"}, []string{"postA"}},
		{"裸名回落", "posts", map[string]interface{}{"posts": "postB"}, []string{"userB", "shared"}, []string{"postB"}},
		{"#右半裸名优先", "manager#postCodes", map[string]interface{}{"postCodes": "postA", "f_postCodes": "postB"}, []string{"userA", "shared"}, []string{"postA"}},
		{"#右半f_回落", "manager#postCodes", map[string]interface{}{"f_postCodes": "postB"}, []string{"userB", "shared"}, []string{"postB"}},
		{"取最后一个#右半", "a#b#postCodes", map[string]interface{}{"postCodes": "postA"}, []string{"userA", "shared"}, []string{"postA"}},
		{"_NN的f_优先", "posts_01", map[string]interface{}{"f_posts": "postA", "posts": "postB"}, []string{"userA", "shared"}, []string{"postA"}},
		{"_NN裸名回落", "posts_01", map[string]interface{}{"posts": "postA"}, []string{"userA", "shared"}, []string{"postA"}},
		{"_NN惰性base", "a_b_01", map[string]interface{}{"f_a_b": "postA", "a_b": "postB", "a": "postC"}, []string{"userA", "shared"}, []string{"postA"}},
		{"空base不匹配", "_01", map[string]interface{}{"f_": "postA"}, nil, nil},
		{"非法后缀不匹配", "posts_01x", map[string]interface{}{"posts": "postA"}, nil, nil},
		{"尾部空#字段不匹配", "manager#", map[string]interface{}{"": "postA", "f_": "postB"}, nil, nil},
		{"键存在即命中null", "posts", map[string]interface{}{"f_posts": nil, "posts": "postB"}, nil, nil},
		{"缺失键", "posts", map[string]interface{}{"other": "postB"}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			provider := newPostAssignmentProvider()
			got := assignPostField(t, engine.HandlerFormPostAssignment, c.nodeKey, c.vars, provider)
			postAssignmentEq(t, got, c.want)
			postAssignmentEq(t, provider.calls, c.calls)
		})
	}
}

func TestFormPostAssignmentInputs259(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  []string
		calls []string
	}{
		{"逗号串多岗位保序", " postA, ,postB, ", []string{"userA", "shared", "userB"}, []string{"postA", "postB"}},
		{"字符串切片多岗位", []string{"postA", "postB"}, []string{"userA", "shared", "userB"}, []string{"postA", "postB"}},
		{"接口切片多岗位", []interface{}{"postA", "postB"}, []string{"userA", "shared", "userB"}, []string{"postA", "postB"}},
		{"数字切片逐元素读取", []int64{0}, []string{"zeroUser"}, []string{"0"}},
		{"数组逐元素读取", [1]string{"postA"}, []string{"userA", "shared"}, []string{"postA"}},
		{"切片元素不二次拆", []string{"postA,postB"}, nil, []string{"postA,postB"}},
		{"接口切片元素不二次拆", []interface{}{"postA,postB", "postC"}, []string{"userC1", "userC2"}, []string{"postA,postB", "postC"}},
		{"岗位去重保序", "postA,postB,postA", []string{"userA", "shared", "userB"}, []string{"postA", "postB"}},
		{"无成员岗位跳过", "ghost,postB", []string{"userB", "shared"}, []string{"ghost", "postB"}},
		{"数字0合法", int64(0), []string{"zeroUser"}, []string{"0"}},
		{"字符串0合法", "0", []string{"zeroUser"}, []string{"0"}},
		{"nil空值丢弃且0合法", []interface{}{nil, "", " ", int64(0), "postA", "0"}, []string{"zeroUser", "userA", "shared"}, []string{"0", "postA"}},
		{"nil值为空", nil, nil, nil},
		{"空串值为空", "", nil, nil},
		{"纯空白值为空", "   ", nil, nil},
		{"逗号串全空为empty", " , , ", nil, nil},
		{"空切片值为空", []string{}, nil, nil},
		{"切片全空值为空", []interface{}{nil, "", "  "}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			provider := newPostAssignmentProvider()
			got := assignPostField(t, engine.HandlerFormPostAssignment, "posts", map[string]interface{}{"f_posts": c.value}, provider)
			postAssignmentEq(t, got, c.want)
			postAssignmentEq(t, provider.calls, c.calls)
		})
	}
}

func TestTaskPostAssignment259(t *testing.T) {
	provider := newPostAssignmentProvider()
	h := postAssignmentHandler(t, engine.HandlerTaskPostAssignee, provider)

	node := &model.FlowNode{ID: "postC"}
	node.Text.Value = "postA"
	postAssignmentEq(t, h.Assign(node, nil, "clerk"), []string{"userC1", "userC2"})
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "0"}, nil, "clerk"), []string{"zeroUser"})
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "ghost"}, nil, "clerk"), nil)
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "postErr"}, nil, "clerk"), nil)
	postAssignmentEq(t, h.Assign(nil, nil, "clerk"), nil)
	postAssignmentEq(t, h.Assign(&model.FlowNode{}, nil, "clerk"), nil)
	postAssignmentEq(t, provider.calls, []string{"postC", "0", "ghost", "postErr"})

	// TaskPost 只保证 Provider 返回顺序，不套 FormPost 的 trim/去重链。
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "postRaw"}, nil, "clerk"),
		[]string{" userB ", "userA", "userB", ""})
}

func TestFormPostAssignmentFailures259(t *testing.T) {
	provider := newPostAssignmentProvider()
	h := postAssignmentHandler(t, engine.HandlerFormPostAssignment, provider)

	postAssignmentEq(t, h.Assign(nil, &model.ProcessInstance{Variables: map[string]interface{}{"f_": "postA"}}, "clerk"), nil)
	postAssignmentEq(t, h.Assign(&model.FlowNode{}, &model.ProcessInstance{Variables: map[string]interface{}{"f_": "postA"}}, "clerk"), nil)
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "posts"}, nil, "clerk"), nil)
	postAssignmentEq(t, h.Assign(&model.FlowNode{ID: "posts"}, &model.ProcessInstance{}, "clerk"), nil)
	postAssignmentEq(t, provider.calls, nil)

	// 单岗位异常跳过，后续岗位继续；全部无成员/全部异常均折空。
	postAssignmentEq(t, assignPostField(t, engine.HandlerFormPostAssignment, "posts",
		map[string]interface{}{"posts": "postErr,postB"}, newPostAssignmentProvider()), []string{"userB", "shared"})
	postAssignmentEq(t, assignPostField(t, engine.HandlerFormPostAssignment, "posts",
		map[string]interface{}{"posts": "postErr"}, newPostAssignmentProvider()), nil)
	postAssignmentEq(t, assignPostField(t, engine.HandlerFormPostAssignment, "posts",
		map[string]interface{}{"posts": "ghost"}, newPostAssignmentProvider()), nil)

	// 最终 userId trim 后保序去重，"0" 合法。
	postAssignmentEq(t, assignPostField(t, engine.HandlerFormPostAssignment, "posts",
		map[string]interface{}{"posts": "postDirty"}, newPostAssignmentProvider()), []string{"userA", "shared", "0"})
}

func TestPostAssignmentMissingProviderAndNames259(t *testing.T) {
	// 三参数旧调用仍可编译运行：两枚岗位 handler 照常注册，但无 SPI 时返回空。
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, nil, nil)
	taskHandler := reg.ResolveAssignment(engine.HandlerTaskPostAssignee)
	formHandler := reg.ResolveAssignment(engine.HandlerFormPostAssignment)
	if _, ok := taskHandler.(*engine.TaskPostAssigneeHandler); !ok {
		t.Fatalf("TaskPost 注册名应解析到 *TaskPostAssigneeHandler，实得 %T", taskHandler)
	}
	if _, ok := formHandler.(*engine.FormPostAssignmentHandler); !ok {
		t.Fatalf("FormPost 注册名应解析到 *FormPostAssignmentHandler，实得 %T", formHandler)
	}
	postAssignmentEq(t, taskHandler.Assign(&model.FlowNode{ID: "postA"}, nil, "clerk"), nil)
	postAssignmentEq(t, formHandler.Assign(&model.FlowNode{ID: "posts"},
		&model.ProcessInstance{Variables: map[string]interface{}{"posts": "postA"}}, "clerk"), nil)

	// 四参数调用把可选 SPI 注入两枚 handler。
	provider := newPostAssignmentProvider()
	reg = engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, nil, nil, provider)
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerTaskPostAssignee).Assign(&model.FlowNode{ID: "postA"}, nil, "clerk"),
		[]string{"userA", "shared"})
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerFormPostAssignment).Assign(&model.FlowNode{ID: "posts"},
		&model.ProcessInstance{Variables: map[string]interface{}{"posts": "postB"}}, "clerk"), []string{"userB", "shared"})
}

func TestPostAssignmentLeavesExistingHandlersUnchanged259(t *testing.T) {
	post := newPostAssignmentProvider()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, &fuUserProv{}, &testOrgUserProv{}, post)

	oldNames := []string{
		engine.HandlerOperatorAssignment,
		engine.HandlerDeptLeader,
		engine.HandlerDeptMainLeader,
		engine.HandlerApplicantDeptLeader,
		engine.HandlerApplicantDeptMainLeader,
		engine.HandlerFormFieldAssignee,
		engine.HandlerTaskRoleAssignee,
		engine.HandlerFormUserDeptLeader,
		engine.HandlerFormUserDeptMainLeader,
		engine.HandlerFormDeptLeader,
		engine.HandlerFormDeptMainLeader,
	}
	for _, name := range oldNames {
		if reg.ResolveAssignment(name) == nil {
			t.Fatalf("既有 handler 注册名丢失：%s", name)
		}
	}

	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerTaskRoleAssignee).Assign(
		&model.FlowNode{ID: "task4"}, nil, "clerk"), []string{"roleA", "roleB"})
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerFormFieldAssignee).Assign(
		&model.FlowNode{ID: "audit#f_userId"},
		&model.ProcessInstance{Variables: map[string]interface{}{"f_userId": "userA,userB"}}, "clerk"), nil)
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerFormFieldAssignee).Assign(
		&model.FlowNode{ID: "task_01"},
		&model.ProcessInstance{Variables: map[string]interface{}{"f_task": "userA"}}, "clerk"), nil)
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerFormFieldAssignee).Assign(
		&model.FlowNode{ID: "task_01"},
		&model.ProcessInstance{Variables: map[string]interface{}{"task": "userA"}}, "clerk"), []string{"userA"})
	postAssignmentEq(t, reg.ResolveAssignment(engine.HandlerFormUserDeptLeader).Assign(
		&model.FlowNode{ID: "manager"},
		&model.ProcessInstance{Variables: map[string]interface{}{"f_manager": fuEmpWithDept}}, "clerk"),
		[]string{"leader1", "leader2"})

	deptReg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(deptReg, &fuUserProv{}, newDeptAssignmentOrgProv(), post)
	postAssignmentEq(t, deptReg.ResolveAssignment(engine.HandlerFormDeptLeader).Assign(
		&model.FlowNode{ID: "depts"},
		&model.ProcessInstance{Variables: map[string]interface{}{"f_depts": "deptA,deptB"}}, "clerk"),
		[]string{"leaderA", "shared", "leaderB"})

	if len(post.calls) != 0 {
		t.Fatalf("既有 handler 不得调用岗位 SPI，实得调用 %q", post.calls)
	}
}

func TestPostAssignmentEngineFlow259(t *testing.T) {
	provider := newPostAssignmentProvider()
	repo := memory.New()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, &fuUserProv{}, &testOrgUserProv{}, provider)
	eng := engine.New(repo, &fuUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetRegistry(reg)

	const taskNode, formNode = "postA", "posts"
	def := registerFlowContent(repo, "post-259-e2e", fuFlowContent("post-259-e2e",
		fuNode{taskNode, engine.HandlerTaskPostAssignee},
		fuNode{formNode, engine.HandlerFormPostAssignment}))
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "clerk",
		map[string]interface{}{"f_posts": "postB,postA"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, []string{taskNode})
	if len(doing) != 1 {
		t.Fatalf("TaskPost 节点应恰好一条待办，实得 %d", len(doing))
	}
	actors, _ := repo.FindTaskActors(context.Background(), doing[0].ID)
	postAssignmentEq(t, actors, []string{"userA", "shared"})
	if err := repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userA"}); err != nil {
		t.Fatalf("add task post actor: %v", err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userA", nil); err != nil {
		t.Fatalf("TaskPost 节点办结: %v", err)
	}

	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, []string{formNode})
	if len(doing) != 1 {
		t.Fatalf("FormPost 节点应恰好一条待办，实得 %d", len(doing))
	}
	actors, _ = repo.FindTaskActors(context.Background(), doing[0].ID)
	postAssignmentEq(t, actors, []string{"userB", "shared", "userA"})
	postAssignmentEq(t, provider.calls, []string{"postA", "postB", "postA"})
	if err := repo.AddTaskActor(context.Background(), doing[0].ID, []string{"userB"}); err != nil {
		t.Fatalf("add form post actor: %v", err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "userB", nil); err != nil {
		t.Fatalf("FormPost 节点办结: %v", err)
	}
	updated, _ := repo.FindInstanceByID(context.Background(), inst.ID)
	if updated.State != model.InstanceStateDone {
		t.Fatalf("两枚岗位 handler 办结后实例应完成，实得 %v", updated.State)
	}
}

// 编译期确认可选 SPI 形状独立于 OrgUserProvider；存量 org provider 无需增加方法。
var _ spi.PostUserProvider = (*postAssignmentProvider)(nil)
