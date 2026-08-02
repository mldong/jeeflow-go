package demo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

type Controller struct {
	repo   *memory.Repository // stats 等 demo 特有统计用
	facade *facade.Facade     // /wf/** 统一门面转发（v1.5.0 重构）
}

func New() *Controller {
	repo := memory.New()
	eng := engine.New(repo, &demoUserProvider{}, &demoIDGen{}, &demoExprEval{})
	loadFlows(repo)
	return &Controller{repo: repo, facade: facade.New(eng, repo, nil)}
}

// flow 门面转发：action 固定，body JSON 原样透传，返回 {code, msg, data}
func (c *Controller) flow(action string) func(r *ghttp.Request) {
	return func(r *ghttp.Request) {
		body := map[string]interface{}{}
		if b := r.GetBody(); len(b) > 0 {
			_ = json.Unmarshal(b, &body)
		}
		r.Response.WriteJson(c.facade.Flow(action, body))
	}
}

func loadFlows(repo *memory.Repository) {
	// 候选路径链：兼容不同启动目录（go run cmd/demo / go run . / 仓库根）
	candidates := []string{
		filepath.Join("..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("..", "..", "..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
		filepath.Join("jeeflow-hub", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows"),
	}
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
	s.Group("/wf", func(g *ghttp.RouterGroup) {
		g.POST("/processDefine/page", c.flow("processDefine/page"))
		g.POST("/processDefine/detail", c.flow("processDefine/detail"))
		g.POST("/processDefine/startAndExecute", c.flow("processDefine/startAndExecute"))
		g.POST("/processInstance/startAndExecute", c.flow("processInstance/startAndExecute"))
		g.POST("/processInstance/page", c.flow("processInstance/page"))
		g.POST("/processInstance/detail", c.flow("processInstance/detail"))
		g.POST("/processInstance/highLight", c.flow("processInstance/highLight"))
		g.POST("/processInstance/approvalRecord", c.flow("processInstance/approvalRecord"))
		g.POST("/processTask/todoList", c.flow("processTask/todoList"))
		g.POST("/processTask/doneList", c.flow("processTask/doneList"))
		g.POST("/processTask/execute", c.flow("processTask/execute"))
		g.POST("/processTask/jumpAbleTaskNameList", c.flow("processTask/jumpAbleTaskNameList"))
	})
	s.Group("/api", func(g *ghttp.RouterGroup) {
		g.GET("/stats", c.stats)
	})
}

type M = map[string]interface{}

func (c *Controller) stats(r *ghttp.Request) {
	userID := r.Get("userId", "user1").String()
	count := 0
	for _, t := range c.repo.AllTasks() {
		if t.TaskState == model.TaskStateDoing {
			actors, _ := c.repo.FindTaskActors(r.Context(), t.ID)
			if contains(t.ActorIDs, userID) || contains(actors, userID) {
				count++
			}
		}
	}
	instCount := 0
	for _, inst := range c.repo.AllInstances() {
		if inst.Operator == userID {
			instCount++
		}
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"todoCount": count, "myInstanceCount": instCount}})
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

type demoUserProvider struct{}

func (p *demoUserProvider) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: "用户" + userID}, nil
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
