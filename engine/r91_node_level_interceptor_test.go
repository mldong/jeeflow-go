// audit8 R91 节点级 pre/postInterceptors 解析档（issues/234 §4.2／spec02 §3 三条）。
// 基准＝java NodeModel.java:49-59（节点串空才取流程根同名键，两档不叠加）；
// 真被引擎调到的端到端腿在 persist/r91_node_level_e2e_test.go（本栈 pre/post 派发点在 executeNode）。
package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

type r91Rec struct {
	tag string
}

func (r *r91Rec) PreHandle(_ context.Context, node *model.FlowNode, _ *model.ProcessInstance) bool {
	return true
}
func (r *r91Rec) PostHandle(_ context.Context, node *model.FlowNode, _ *model.ProcessInstance) {}
func (r *r91Rec) Order() int                                                                   { return 0 }

type r91CountRepo struct {
	spi.ProcessRepository
	reads int
}

func (r *r91CountRepo) FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error) {
	r.reads++
	return r.ProcessRepository.FindDefineByID(ctx, id)
}

// r91Engine 造一台只挂了扩展注册表的引擎（解析档不经执行链，直接量 resolveInterceptors 的取串形状）
func r91Engine(t *testing.T, rootPost, rootPre string, reg map[string]FlowInterceptor) (*EngineImpl, int64) {
	t.Helper()
	repo := memory.New()
	content := []byte(fmt.Sprintf(`{"name": "r91", "displayName": "r91", "type": "approval",
		"postInterceptors": %q, "preInterceptors": %q,
		"nodes": [{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
		          {"id": "apply", "type": "snaker:task", "properties": {"assignee": "user1"}, "text": {"value": "申请"}},
		          {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
		"edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
		          {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "finish", "properties": {}}]}`,
		rootPost, rootPre))
	def := &model.ProcessDefine{Name: "r91", DisplayName: "r91", Type: "approval", State: 1, Version: 1, Content: content}
	repo.AddDefine(def)
	e := &EngineImpl{repo: repo, ext: &Extensions{InterceptorRegistry: reg}}
	e.SetExtensions(e.ext)
	return e, def.ID
}

func r91Node(id string, props map[string]interface{}) *model.FlowNode {
	return &model.FlowNode{ID: id, Type: "snaker:task", Properties: props}
}

func r91Tags(list []FlowInterceptor) string {
	var out []string
	for _, ic := range list {
		if r, ok := ic.(*r91Rec); ok {
			out = append(out, r.tag)
		}
	}
	return strings.Join(out, ",")
}

func TestR9101NodeDeclarationWinsNoStacking(t *testing.T) {
	e, defineID := r91Engine(t, "root", "", map[string]FlowInterceptor{
		"root": &r91Rec{"root"}, "node": &r91Rec{"node"}})
	inst := &model.ProcessInstance{DefineID: defineID}
	node := r91Node("apply", map[string]interface{}{"postInterceptors": "node"})
	list, err := e.resolveInterceptors(context.Background(), node, inst, "post")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got := r91Tags(list); got != "node" {
		t.Fatalf("节点声明优先且两档不叠加（want node, got %q；旧形状＝只读流程根 ⇒ got root）", got)
	}
}

func TestR9102FallsBackToProcessRoot(t *testing.T) {
	e, defineID := r91Engine(t, "root", "", map[string]FlowInterceptor{"root": &r91Rec{"root"}})
	inst := &model.ProcessInstance{DefineID: defineID}
	list, err := e.resolveInterceptors(context.Background(), r91Node("apply", map[string]interface{}{}), inst, "post")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got := r91Tags(list); got != "root" {
		t.Fatalf("节点不声明时应回落到流程根那一档（got %q）", got)
	}
}

func TestR9103RootPreInterceptorsAreRead(t *testing.T) {
	e, defineID := r91Engine(t, "", "rootpre", map[string]FlowInterceptor{"rootpre": &r91Rec{"rootpre"}})
	inst := &model.ProcessInstance{DefineID: defineID}
	list, err := e.resolveInterceptors(context.Background(), r91Node("finish", map[string]interface{}{}), inst, "pre")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got := r91Tags(list); got != "rootpre" {
		t.Fatalf("根级 preInterceptors 仍不被解析（修前即此形状，got %q）", got)
	}
}

func TestR9104NodePreUsesSamePriority(t *testing.T) {
	e, defineID := r91Engine(t, "", "rootpre", map[string]FlowInterceptor{
		"rootpre": &r91Rec{"rootpre"}, "nodepre": &r91Rec{"nodepre"}})
	inst := &model.ProcessInstance{DefineID: defineID}
	node := r91Node("finish", map[string]interface{}{"preInterceptors": "nodepre"})
	list, err := e.resolveInterceptors(context.Background(), node, inst, "pre")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got := r91Tags(list); got != "nodepre" {
		t.Fatalf("节点级 pre 未走同一优先序（got %q）", got)
	}
}

func TestR9105UnregisteredDeclarationRaises(t *testing.T) {
	e, defineID := r91Engine(t, "", "", map[string]FlowInterceptor{})
	inst := &model.ProcessInstance{DefineID: defineID}
	node := r91Node("apply", map[string]interface{}{
		"postInterceptors": "com.mldong.jeeflow.ghost.GhostInterceptor"})
	_, err := e.resolveInterceptors(context.Background(), node, inst, "post")
	if err == nil {
		t.Fatalf("节点档声明未注册的拦截器必须显式报错（修前＝节点声明被静默忽略，压根不报错）")
	}
	msg := err.Error()
	if !strings.Contains(msg, "未注册") || !strings.Contains(msg, "postInterceptors") {
		t.Fatalf("错误应含档位与未注册提示: %v", msg)
	}
}

func TestR9106CacheKeyIncludesNodeAndPhase(t *testing.T) {
	e, defineID := r91Engine(t, "", "", map[string]FlowInterceptor{
		"a": &r91Rec{"a"}, "b": &r91Rec{"b"}})
	inst := &model.ProcessInstance{DefineID: defineID}
	na := r91Node("apply", map[string]interface{}{"postInterceptors": "a"})
	nb := r91Node("apply2", map[string]interface{}{"postInterceptors": "b"})
	la, err := e.resolveInterceptors(context.Background(), na, inst, "post")
	if err != nil {
		t.Fatalf("a 解析失败: %v", err)
	}
	lb, err := e.resolveInterceptors(context.Background(), nb, inst, "post")
	if err != nil {
		t.Fatalf("b 解析失败: %v", err)
	}
	if r91Tags(la) != "a" || r91Tags(lb) != "b" {
		t.Fatalf("两节点各自命中自己的声明（got %q / %q；旧缓存按 defineId 单键 ⇒ 第二条串到第一条）", r91Tags(la), r91Tags(lb))
	}
	// 再取一次走缓存分支，结果必须仍是本节点那一档
	again, err := e.resolveInterceptors(context.Background(), na, inst, "post")
	if err != nil || r91Tags(again) != "a" {
		t.Fatalf("缓存分支串档: %q err=%v", r91Tags(again), err)
	}
}

func TestR9107RootDeclsReadDefineOnce(t *testing.T) {
	// 拆节点档后"节点未声明⇒回查流程根"这条腿若逐节点读定义行，就把 issue 34 的"读一次"放大成逐任务读。
	repo := memory.New()
	content := []byte(`{"name": "r91-reads", "displayName": "r91-reads", "type": "approval",
		"postInterceptors": "", "preInterceptors": "",
		"nodes": [{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
		          {"id": "apply", "type": "snaker:task", "properties": {"assignee": "user1"}, "text": {"value": "申请"}},
		          {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
		"edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}}]}`)
	def := &model.ProcessDefine{Name: "r91-reads", DisplayName: "r91-reads", Type: "approval", State: 1, Version: 1, Content: content}
	repo.AddDefine(def)
	count := &r91CountRepo{ProcessRepository: repo}
	e := &EngineImpl{repo: count, ext: &Extensions{InterceptorRegistry: map[string]FlowInterceptor{}}}
	e.SetExtensions(e.ext)
	inst := &model.ProcessInstance{DefineID: def.ID}
	for _, n := range []string{"start", "apply", "finish"} {
		for _, ph := range []string{"pre", "post"} {
			if _, err := e.resolveInterceptors(context.Background(), r91Node(n, map[string]interface{}{}), inst, ph); err != nil {
				t.Fatalf("%s/%s 解析失败: %v", n, ph, err)
			}
		}
	}
	if count.reads > 1 {
		t.Fatalf("定义行被逐节点回查：3 节点 × 2 档 ⇒ %d 次（根声明须每定义缓存一行）", count.reads)
	}
}
