package jeeflow_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// issues/258：表单指定 1–N 个部门，取各部门负责人／分管领导。
// 断言直接落在 handler 返回值与 SPI 调用顺序上；另有一条内存仓端到端腿验证注册名可解析、参与者可落库。
type deptAssignmentOrgProv struct {
	leaders      map[string][]string
	mainLeaders  map[string][]string
	leaderErrors map[string]error
	mainErrors   map[string]error
	leaderCalls  []string
	mainCalls    []string
}

func newDeptAssignmentOrgProv() *deptAssignmentOrgProv {
	return &deptAssignmentOrgProv{
		leaders: map[string][]string{
			"deptA":     {"leaderA", "shared"},
			"deptB":     {"leaderB", "shared"},
			"deptC":     {"leaderC1", "leaderC2"},
			"deptDirty": {" leaderD ", "", "leaderD", "0", " 0 "},
			"0":         {"zeroLeader"},
		},
		mainLeaders: map[string][]string{
			"deptA": {"mainA"},
			"deptB": {"mainB"},
			"deptC": {"mainC1", "mainC2"},
			"0":     {"zeroMain"},
		},
		leaderErrors: map[string]error{"deptErr": errors.New("leader lookup failed")},
		mainErrors:   map[string]error{"deptErr": errors.New("main leader lookup failed")},
	}
}

func (p *deptAssignmentOrgProv) FindDeptLeaders(deptID string) ([]string, error) {
	p.leaderCalls = append(p.leaderCalls, deptID)
	if err := p.leaderErrors[deptID]; err != nil {
		return nil, err
	}
	return p.leaders[deptID], nil
}

func (p *deptAssignmentOrgProv) FindDeptMainLeaders(deptID string) ([]string, error) {
	p.mainCalls = append(p.mainCalls, deptID)
	if err := p.mainErrors[deptID]; err != nil {
		return nil, err
	}
	return p.mainLeaders[deptID], nil
}

func (p *deptAssignmentOrgProv) FindByRole(string) ([]string, error) { return nil, nil }

func deptAssignmentHandler(t *testing.T, name string, org spi.OrgUserProvider) engine.IAssignmentHandler {
	t.Helper()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, nil, org)
	h := reg.ResolveAssignment(name)
	if h == nil {
		t.Fatalf("注册名 %s 解析不到处理器", name)
	}
	return h
}

func assignDeptField(t *testing.T, name, nodeKey string, vars map[string]interface{}, org spi.OrgUserProvider) []string {
	t.Helper()
	h := deptAssignmentHandler(t, name, org)
	var node *model.FlowNode
	if nodeKey != "" {
		node = &model.FlowNode{ID: nodeKey}
	}
	return h.Assign(node, &model.ProcessInstance{Variables: vars}, "clerk")
}

func deptAssignmentEq(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("实得 %q，want %q", got, want)
	}
}

func TestFormDeptAssignmentFieldLookup258(t *testing.T) {
	cases := []struct {
		name    string
		nodeKey string
		vars    map[string]interface{}
		want    []string
		calls   []string
	}{
		{"f_前缀优先于裸名", "depts", map[string]interface{}{"f_depts": "deptA", "depts": "deptB"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"裸名回落", "depts", map[string]interface{}{"depts": "deptB"}, []string{"leaderB", "shared"}, []string{"deptB"}},
		{"#右半裸名优先", "manager#deptIds", map[string]interface{}{"deptIds": "deptA", "f_deptIds": "deptB"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"#右半f_回落", "manager#deptIds", map[string]interface{}{"f_deptIds": "deptB"}, []string{"leaderB", "shared"}, []string{"deptB"}},
		{"取最后一个#右半", "a#b#deptIds", map[string]interface{}{"deptIds": "deptA"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"_NN的f_优先", "depts_01", map[string]interface{}{"f_depts": "deptA", "depts": "deptB"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"_NN裸名回落", "depts_01", map[string]interface{}{"depts": "deptA"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"_NN惰性base", "a_b_01", map[string]interface{}{"f_a_b": "deptA", "a_b": "deptB", "a": "deptC"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"空base不匹配", "_01", map[string]interface{}{"f_": "deptA"}, nil, nil},
		{"非法后缀不匹配", "depts_01x", map[string]interface{}{"depts": "deptA"}, nil, nil},
		{"键存在即命中null", "depts", map[string]interface{}{"f_depts": nil, "depts": "deptB"}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			org := newDeptAssignmentOrgProv()
			got := assignDeptField(t, engine.HandlerFormDeptLeader, c.nodeKey, c.vars, org)
			deptAssignmentEq(t, got, c.want)
			deptAssignmentEq(t, org.leaderCalls, c.calls)
		})
	}
}

func TestFormDeptAssignmentDepartmentInputs258(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  []string
		calls []string
	}{
		{"逗号串多部门保序", " deptA, ,deptB, ", []string{"leaderA", "shared", "leaderB"}, []string{"deptA", "deptB"}},
		{"字符串切片多部门", []string{"deptA", "deptB"}, []string{"leaderA", "shared", "leaderB"}, []string{"deptA", "deptB"}},
		{"接口切片多部门", []interface{}{"deptA", "deptB"}, []string{"leaderA", "shared", "leaderB"}, []string{"deptA", "deptB"}},
		{"数字切片逐元素读取", []int64{0}, []string{"zeroLeader"}, []string{"0"}},
		{"数组逐元素读取", [1]string{"deptA"}, []string{"leaderA", "shared"}, []string{"deptA"}},
		{"切片元素不二次拆", []string{"deptA,deptB"}, nil, []string{"deptA,deptB"}},
		{"接口切片元素不二次拆", []interface{}{"deptA,deptB", "deptC"}, []string{"leaderC1", "leaderC2"}, []string{"deptA,deptB", "deptC"}},
		{"部门去重保序", "deptA,deptB,deptA", []string{"leaderA", "shared", "leaderB"}, []string{"deptA", "deptB"}},
		{"非法部门跳过", "ghost,deptB", []string{"leaderB", "shared"}, []string{"ghost", "deptB"}},
		{"数字0合法", int64(0), []string{"zeroLeader"}, []string{"0"}},
		{"字符串0合法", "0", []string{"zeroLeader"}, []string{"0"}},
		{"切片nil空值丢弃且0合法", []interface{}{nil, "", " ", int64(0), "deptA", "0"}, []string{"zeroLeader", "leaderA", "shared"}, []string{"0", "deptA"}},
		{"nil值为空", nil, nil, nil},
		{"空串值为空", "", nil, nil},
		{"纯空白值为空", "   ", nil, nil},
		{"空切片值为空", []string{}, nil, nil},
		{"切片全空值为空", []interface{}{nil, "", "  "}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			org := newDeptAssignmentOrgProv()
			got := assignDeptField(t, engine.HandlerFormDeptLeader, "depts", map[string]interface{}{"f_depts": c.value}, org)
			deptAssignmentEq(t, got, c.want)
			deptAssignmentEq(t, org.leaderCalls, c.calls)
		})
	}
}

func TestFormDeptAssignmentLeaderTiers258(t *testing.T) {
	t.Run("负责人档", func(t *testing.T) {
		org := newDeptAssignmentOrgProv()
		got := assignDeptField(t, engine.HandlerFormDeptLeader, "depts", map[string]interface{}{"depts": "deptA,deptB,0"}, org)
		deptAssignmentEq(t, got, []string{"leaderA", "shared", "leaderB", "zeroLeader"})
		deptAssignmentEq(t, org.leaderCalls, []string{"deptA", "deptB", "0"})
		deptAssignmentEq(t, org.mainCalls, nil)
	})
	t.Run("分管领导档", func(t *testing.T) {
		org := newDeptAssignmentOrgProv()
		got := assignDeptField(t, engine.HandlerFormDeptMainLeader, "depts", map[string]interface{}{"depts": "deptA,deptB,0"}, org)
		deptAssignmentEq(t, got, []string{"mainA", "mainB", "zeroMain"})
		deptAssignmentEq(t, org.mainCalls, []string{"deptA", "deptB", "0"})
		deptAssignmentEq(t, org.leaderCalls, nil)
	})
	t.Run("领导trim去重且0合法", func(t *testing.T) {
		org := newDeptAssignmentOrgProv()
		got := assignDeptField(t, engine.HandlerFormDeptLeader, "depts", map[string]interface{}{"depts": "deptDirty"}, org)
		deptAssignmentEq(t, got, []string{"leaderD", "0"})
	})
}

func TestFormDeptAssignmentFailures258(t *testing.T) {
	handlers := []struct {
		name string
		want []string
	}{
		{engine.HandlerFormDeptLeader, []string{"leaderB", "shared"}},
		{engine.HandlerFormDeptMainLeader, []string{"mainB"}},
	}
	for _, h := range handlers {
		t.Run(h.name+"/缺字段", func(t *testing.T) {
			deptAssignmentEq(t, assignDeptField(t, h.name, "depts", nil, newDeptAssignmentOrgProv()), nil)
		})
		t.Run(h.name+"/全部部门无领导", func(t *testing.T) {
			deptAssignmentEq(t, assignDeptField(t, h.name, "depts", map[string]interface{}{"depts": "ghost"}, newDeptAssignmentOrgProv()), nil)
		})
		t.Run(h.name+"/缺SPI", func(t *testing.T) {
			deptAssignmentEq(t, assignDeptField(t, h.name, "depts", map[string]interface{}{"depts": "deptA"}, nil), nil)
		})
		t.Run(h.name+"/无节点", func(t *testing.T) {
			handler := deptAssignmentHandler(t, h.name, newDeptAssignmentOrgProv())
			deptAssignmentEq(t, handler.Assign(nil, &model.ProcessInstance{Variables: map[string]interface{}{"f_": "deptA"}}, "clerk"), nil)
		})
		t.Run(h.name+"/空节点ID", func(t *testing.T) {
			handler := deptAssignmentHandler(t, h.name, newDeptAssignmentOrgProv())
			deptAssignmentEq(t, handler.Assign(&model.FlowNode{}, &model.ProcessInstance{Variables: map[string]interface{}{"f_": "deptA"}}, "clerk"), nil)
		})
		t.Run(h.name+"/无实例", func(t *testing.T) {
			handler := deptAssignmentHandler(t, h.name, newDeptAssignmentOrgProv())
			deptAssignmentEq(t, handler.Assign(&model.FlowNode{ID: "depts"}, nil, "clerk"), nil)
		})
		t.Run(h.name+"/SPI错误跳过继续", func(t *testing.T) {
			org := newDeptAssignmentOrgProv()
			got := assignDeptField(t, h.name, "depts", map[string]interface{}{"depts": "deptErr,deptB"}, org)
			deptAssignmentEq(t, got, h.want)
			if h.name == engine.HandlerFormDeptLeader {
				deptAssignmentEq(t, org.leaderCalls, []string{"deptErr", "deptB"})
			} else {
				deptAssignmentEq(t, org.mainCalls, []string{"deptErr", "deptB"})
			}
		})
	}
}

func TestFormDeptAssignmentNamesResolvable258(t *testing.T) {
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, nil, newDeptAssignmentOrgProv())
	if _, ok := reg.ResolveAssignment("com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler").(*engine.FormDeptLeaderAssignmentHandler); !ok {
		t.Fatal("Java FQCN 应解析到 FormDeptLeaderAssignmentHandler")
	}
	if _, ok := reg.ResolveAssignment("com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler").(*engine.FormDeptMainLeaderAssignmentHandler); !ok {
		t.Fatal("Java FQCN 应解析到 FormDeptMainLeaderAssignmentHandler")
	}
}

func TestFormDeptAssignmentEngineFlow258(t *testing.T) {
	org := newDeptAssignmentOrgProv()
	repo := memory.New()
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, &fuUserProv{}, org)
	eng := engine.New(repo, &fuUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetRegistry(reg)

	const managerNode, mainNode = "manager#f_deptIds", "main#f_deptIds"
	def := registerFlowContent(repo, "fd258-e2e", fuFlowContent("fd258-e2e",
		fuNode{managerNode, engine.HandlerFormDeptLeader},
		fuNode{mainNode, engine.HandlerFormDeptMainLeader}))
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "clerk",
		map[string]interface{}{"f_deptIds": "deptA,deptB"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	doing, _ := repo.FindDoingTasks(context.Background(), inst.ID, []string{managerNode})
	if len(doing) != 1 {
		t.Fatalf("负责人节点应恰好一条待办，实得 %d", len(doing))
	}
	actors, _ := repo.FindTaskActors(context.Background(), doing[0].ID)
	deptAssignmentEq(t, actors, []string{"leaderA", "shared", "leaderB"})
	if err := repo.AddTaskActor(context.Background(), doing[0].ID, []string{"leaderA"}); err != nil {
		t.Fatalf("add leader actor: %v", err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "leaderA", nil); err != nil {
		t.Fatalf("负责人办结: %v", err)
	}

	doing, _ = repo.FindDoingTasks(context.Background(), inst.ID, []string{mainNode})
	if len(doing) != 1 {
		t.Fatalf("分管领导节点应恰好一条待办，实得 %d", len(doing))
	}
	actors, _ = repo.FindTaskActors(context.Background(), doing[0].ID)
	deptAssignmentEq(t, actors, []string{"mainA", "mainB"})
	if err := repo.AddTaskActor(context.Background(), doing[0].ID, []string{"mainA"}); err != nil {
		t.Fatalf("add main actor: %v", err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), doing[0].ID, "mainA", nil); err != nil {
		t.Fatalf("分管领导办结: %v", err)
	}
	updated, _ := repo.FindInstanceByID(context.Background(), inst.ID)
	if updated.State != model.InstanceStateDone {
		t.Fatalf("两档办结后实例应完成，实得 %v", updated.State)
	}
}
