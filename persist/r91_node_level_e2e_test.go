// audit8 R91 go 腿的端到端判据：节点级 pre/postInterceptors 声明**真被引擎调到**
// （engine/r91_node_level_interceptor_test.go 量的是解析档；这里量派发点 engine_impl.go:169/823/832）。
// 反空转：先断"calls 非空且看到目标节点"，再断优先序/回落形状。
package persist_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

type r91Rec struct {
	tag   string
	calls *[]string
}

func (r *r91Rec) PreHandle(_ context.Context, node *model.FlowNode, _ *model.ProcessInstance) bool {
	*r.calls = append(*r.calls, "pre:"+r.tag+":"+node.ID)
	return true
}
func (r *r91Rec) PostHandle(_ context.Context, node *model.FlowNode, _ *model.ProcessInstance) {
	*r.calls = append(*r.calls, "post:"+r.tag+":"+node.ID)
}
func (r *r91Rec) Order() int { return 0 }

func r91Flow(name, rootPost, rootPre string, nodeProps string) *model.ProcessDefine {
	content := fmt.Sprintf(`{"name": %q, "displayName": %q, "type": "approval",
		"relTableName": "biz_r91", "persistMode": "SYNC",
		"postInterceptors": %q, "preInterceptors": %q,
		"nodes": [{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
		          {"id": "apply", "type": "snaker:task", "properties": {"assignee": "user1"%s}, "text": {"value": "申请"}},
		          {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
		"edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
		          {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "finish", "properties": {}}]}`,
		name, name, rootPost, rootPre, nodeProps)
	return &model.ProcessDefine{Name: name, DisplayName: name, Type: "approval", State: 1, Version: 1, Content: []byte(content)}
}

func TestR91E2ENodeLevelPostActuallyFires(t *testing.T) {
	var calls []string
	reg := map[string]engine.FlowInterceptor{"node": &r91Rec{"node", &calls}, "root": &r91Rec{"root", &calls}}
	def := r91Flow("r91-e2e-priority", "root", "", `, "postInterceptors": "node"`)
	runFlowWithInterceptors(t, reg, def)
	if !hasCall(calls, "post:node:apply") {
		t.Fatalf("节点级 postInterceptors 没被引擎调到（修前即此形状＝静默忽略）: %v", calls)
	}
	if hasCall(calls, "post:root:apply") {
		t.Fatalf("apply 上节点声明与根声明叠加了（两档不叠加）: %v", calls)
	}
}

func TestR91E2ERootFallbackStillFires(t *testing.T) {
	var calls []string
	reg := map[string]engine.FlowInterceptor{"root": &r91Rec{"root", &calls}}
	def := r91Flow("r91-e2e-fallback", "root", "", "")
	runFlowWithInterceptors(t, reg, def)
	if !hasCall(calls, "post:root:apply") {
		t.Fatalf("节点不声明时应回落到流程根那一档（回归既有形状）: %v", calls)
	}
}

func TestR91E2ERootPreActuallyFires(t *testing.T) {
	// 根级 pre：修前本栈声明层零命中（只读根级 post）⇒ 这一档专治"根级 pre 不解析/不派发"
	var calls []string
	reg := map[string]engine.FlowInterceptor{"rootpre": &r91Rec{"rootpre", &calls}}
	def := r91Flow("r91-e2e-rootpre", "", "rootpre", "")
	runFlowWithInterceptors(t, reg, def)
	if !strings.Contains(strings.Join(calls, ","), "pre:rootpre:") {
		t.Fatalf("根级 preInterceptors 仍不被解析/派发: %v", calls)
	}
}

func TestR91E2ENodePostUnregisteredSurfaces(t *testing.T) {
	// 节点档 post 声明未注册 ⇒ 必须显式报错。这一档专治拆档后的静默面：defer 里 `_ =` 丢弃 firePost 的
	// 错误，而 firePre 只校验 pre 档（R91 之前两档同串、校验 pre 顺带校验了 post）。
	var calls []string
	reg := map[string]engine.FlowInterceptor{"ok": &r91Rec{"ok", &calls}}
	def := r91Flow("r91-e2e-ghost", "", "", `, "postInterceptors": "com.mldong.jeeflow.ghost.GhostInterceptor"`)
	repo := memory.New()
	repo.AddDefine(def)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: nil, InterceptorRegistry: reg})
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1",
		map[string]interface{}{"f_title": "幽灵节点档"})
	if err != nil {
		t.Fatalf("发起阶段不该解析 apply 节点的声明（幽灵挂在 apply 上）: %v", err)
	}
	doing, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil || len(doing) == 0 {
		t.Fatalf("前置失效：应有一条进行中任务（%v / %d 条）", err, len(doing))
	}
	_, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeAgree)})
	if err == nil || !strings.Contains(err.Error(), "未注册") {
		t.Fatalf("节点档未注册的 postInterceptors 必须显式报错，实得: %v", err)
	}
}

func runFlowWithInterceptors(t *testing.T, reg map[string]engine.FlowInterceptor, def *model.ProcessDefine) {
	t.Helper()
	repo := memory.New()
	repo.AddDefine(def)
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{Interceptors: nil, InterceptorRegistry: reg})
	inst, err := eng.StartProcessInstanceByID(context.Background(), def.ID, "user1",
		map[string]interface{}{"f_title": "R91 单"})
	if err != nil {
		t.Fatalf("发起失败: %v", err)
	}
	doing, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil || len(doing) == 0 {
		t.Fatalf("前置失效：应有一条进行中任务（%v / %d 条）", err, len(doing))
	}
	if _, err = eng.ExecuteProcessTask(context.Background(), doing[0].ID, "user1",
		map[string]interface{}{engine.KeySubmitType: int(model.SubmitTypeAgree)}); err != nil {
		t.Fatalf("办理失败: %v", err)
	}
}

func hasCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}
