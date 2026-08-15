package demo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

type Controller struct {
	mu     sync.RWMutex
	repo   *memory.Repository      // stats 等 demo 特有统计用
	ext    *memory.ExtRepository   // 扩展仓储（内存实现）：流程设计/历史/委托
	facade *facade.Facade          // /wf/** 统一门面转发（v1.5.0 重构）
}

func New() *Controller {
	c := &Controller{}
	c.rebuild()
	return c
}

// rebuild 组装引擎与门面（启动与 /api/reset 复用）：
// 接入内存扩展仓储（design/surrogate 可用）+ 用户搜索/组织提供者（candidatePage 闭环）
func (c *Controller) rebuild() {
	repo := memory.New()
	ext := memory.NewExt()
	userProv := &demoUserProvider{}
	eng := engine.New(repo, userProv, &demoIDGen{}, &demoExprEval{})
	// 内置参与者 handler（部门领导/角色取人等，assignment-handler 流程依赖）
	reg := engine.NewHandlerRegistry()
	engine.RegisterBuiltinAssignments(reg, userProv, demoOrgProvider{})
	eng.SetRegistry(reg)
	loadFlows(repo)
	c.repo, c.ext = repo, ext
	c.facade = facade.New(eng, repo, ext).
		SetUserSearch(demoUserSearch).
		SetOrgUserProvider(demoOrgProvider{})
}

// flowAny 门面通配转发：POST /wf/{action}（action 多段，如 processDefine/page），
// body JSON 原样透传，返回 {code, msg, data}。对齐 Java/Python/Node 的通配模式，40 action 全可达。
func (c *Controller) flowAny(r *ghttp.Request) {
	action := strings.TrimPrefix(r.URL.Path, "/wf/")
	body := map[string]interface{}{}
	if b := r.GetBody(); len(b) > 0 {
		_ = json.Unmarshal(b, &body)
	}
	c.mu.RLock()
	f := c.facade
	c.mu.RUnlock()
	r.Response.WriteJson(f.Flow(action, body))
}

func loadFlows(repo *memory.Repository) {
	// 候选路径链：优先环境变量 JEEFLOW_FLOWS_DIR（容器部署挂载），再兼容不同启动目录
	candidates := []string{}
	if dir := os.Getenv("JEEFLOW_FLOWS_DIR"); dir != "" {
		candidates = append(candidates, dir)
	}
	candidates = append(candidates,
		filepath.Join("..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("..", "..", "..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("jeeflow-hub", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
	)
	var flowsDir string
	var entries []os.DirEntry
	var err error
	for _, cand := range candidates {
		if e, e2 := os.ReadDir(cand); e2 == nil {
			flowsDir, entries, err = cand, e, nil
			break
		}
	}
	if err != nil || flowsDir == "" {
		panic("cannot find flows directory")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for i, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(flowsDir, e.Name()))
		if err != nil {
			continue
		}
		var raw map[string]interface{}
		json.Unmarshal(data, &raw)
		name, _ := raw["name"].(string)
		display, _ := raw["displayName"].(string)
		typ, _ := raw["type"].(string)
		if typ == "" {
			typ = "approval"
		}
		repo.AddDefine(&model.ProcessDefine{
			ID: int64(i + 1), Name: name, DisplayName: display, Type: typ, State: 1, Content: data,
		})
	}
}

func (c *Controller) RegisterRoutes(s *ghttp.Server) {
	s.BindHandler("POST:/wf/*action", c.flowAny)
	s.BindHandler("GET:/healthz", c.healthz)
	s.Group("/api", func(g *ghttp.RouterGroup) {
		g.GET("/stats", c.stats)
		g.POST("/reset", c.reset)
	})
}

type M = map[string]interface{}

// healthz 健康检查（四端对齐）
func (c *Controller) healthz(r *ghttp.Request) {
	r.Response.WriteJson(M{"status": "UP", "backend": "go"})
}

// stats 统计（四端统一口径：todoCount 按任务参与者，myInstanceCount 按 instance.operator）
func (c *Controller) stats(r *ghttp.Request) {
	userID := r.Get("userId", "user1").String()
	c.mu.RLock()
	repo := c.repo
	c.mu.RUnlock()
	count := 0
	for _, t := range repo.AllTasks() {
		if t.TaskState == model.TaskStateDoing {
			actors, _ := repo.FindTaskActors(r.Context(), t.ID)
			if contains(t.ActorIDs, userID) || contains(actors, userID) {
				count++
			}
		}
	}
	instCount := 0
	for _, inst := range repo.AllInstances() {
		if inst.Operator == userID {
			instCount++
		}
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"todoCount": count, "myInstanceCount": instCount}})
}

// reset 一键重置演示数据（对齐 Python /api/reset）：重建内存库与扩展仓储 + 重载种子流程定义
func (c *Controller) reset(r *ghttp.Request) {
	c.mu.Lock()
	c.rebuild()
	c.mu.Unlock()
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": nil})
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

type demoIDGen struct{ n int64 }

func (g *demoIDGen) NextID() int64 { g.n++; return g.n }

type demoExprEval struct{}

func (e *demoExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	// 简单数值比较：amount > 1000, amount <= 1000
	v, ok := vars["amount"]
	if !ok {
		return false, nil
	}
	amt, ok := toFloat(v)
	if !ok {
		return false, nil
	}
	switch expr {
	case "amount > 1000":
		return amt > 1000, nil
	case "amount >= 1000":
		return amt >= 1000, nil
	case "amount < 1000":
		return amt < 1000, nil
	case "amount <= 1000":
		return amt <= 1000, nil
	case "amount == 1000":
		return amt == 1000, nil
	case "amount != 1000":
		return amt != 1000, nil
	}
	return false, nil
}

func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		f, err := val.Float64()
		return f, err == nil
	}
	return 0, false
}

var _ = time.Now // suppress import error
