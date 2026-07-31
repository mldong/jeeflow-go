package demo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

type Controller struct {
	engine *engine.EngineImpl
	repo   *memory.Repository
}

func New() *Controller {
	repo := memory.New()
	eng := engine.New(repo, &demoUserProvider{}, &demoIDGen{}, &demoExprEval{})
	loadFlows(repo)
	return &Controller{engine: eng, repo: repo}
}

func loadFlows(repo *memory.Repository) {
	flowsDir := filepath.Join("..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows")
	entries, err := os.ReadDir(flowsDir)
	if err != nil {
		// fallback: relative to working dir
		flowsDir = filepath.Join("jeeflow-hub", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows")
		entries, err = os.ReadDir(flowsDir)
	}
	if err != nil {
		panic("cannot find flows directory: " + err.Error())
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
		g.POST("/processDefine/page", c.definePage)
		g.POST("/processDefine/detail", c.defineDetail)
		g.POST("/processInstance/startAndExecute", c.startFlow)
		g.POST("/processInstance/page", c.instancePage)
		g.POST("/processInstance/detail", c.instanceDetail)
		g.POST("/processTask/todoList", c.todoList)
		g.POST("/processTask/execute", c.executeTask)
	})
	s.Group("/api", func(g *ghttp.RouterGroup) {
		g.GET("/stats", c.stats)
	})
}

type M = map[string]interface{}

func (c *Controller) definePage(r *ghttp.Request) {
	defs := c.repo.AllDefines()
	rows := make([]M, len(defs))
	for i, d := range defs {
		rows[i] = M{"id": d.ID, "name": d.Name, "displayName": d.DisplayName}
	}
	r.Response.WriteJson(M{"code": 200, "data": M{"rows": rows}})
}

func (c *Controller) defineDetail(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	def, err := c.repo.FindDefineByID(id)
	if err != nil || def == nil {
		r.Response.WriteJson(M{"code": 500, "message": "流程定义不存在"})
		return
	}
	graphData := M{}
	json.Unmarshal(def.Content, &graphData)
	r.Response.WriteJson(M{"code": 200, "data": M{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
		"graphData": graphData,
	}})
}

func (c *Controller) startFlow(r *ghttp.Request) {
		var p struct {
			ProcessDefineID string `json:"processDefineId"`
			Operator        string `json:"operator"`
			Amount          string `json:"amount"`
		}
		r.Parse(&p)
		defineID, _ := strconv.ParseInt(p.ProcessDefineID, 10, 64)
		args := M{"BUSINESS_NO": "BIZ-1000000"}
		if v, err := strconv.ParseFloat(p.Amount, 64); err == nil {
			args["amount"] = v
		}
		inst, err := c.engine.StartProcessInstanceByID(defineID, p.Operator, args)
		if err != nil {
			r.Response.WriteJson(M{"code": 500, "message": err.Error()})
			return
		}
		// boot2 契约：自动完成申请节点（assignee="applicant"）
		doing, _ := c.repo.FindDoingTasks(inst.ID, nil)
		for _, task := range doing {
			c.repo.AddTaskActor(task.ID, []string{p.Operator})
			args["submitType"] = 0 // APPLY
			c.engine.ExecuteProcessTask(task.ID, p.Operator, args)
		}
		r.Response.WriteJson(M{"code": 200, "data": M{"processInstanceId": strconv.FormatInt(inst.ID, 10)}})
	}

func (c *Controller) instancePage(r *ghttp.Request) {
	insts := c.repo.AllInstances()
	rows := make([]M, len(insts))
	for i, inst := range insts {
		def, _ := c.repo.FindDefineByID(inst.DefineID)
		dn := ""
		if def != nil { dn = def.DisplayName }
		rows[i] = M{"id": inst.ID, "processDefineId": inst.DefineID, "state": int(inst.State),
			"operator": inst.Operator, "createTime": inst.CreateTime.Format("2006-01-02T15:04:05"),
			"processDefineName": dn, "processDefineDisplayName": dn}
	}
	r.Response.WriteJson(M{"code": 200, "data": M{"rows": rows, "pageNum": 1, "pageSize": 100, "recordCount": len(rows)}})
}

func (c *Controller) instanceDetail(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	inst, err := c.repo.FindInstanceByID(id)
	if err != nil || inst == nil {
		r.Response.WriteJson(M{"code": 500, "message": "实例不存在"})
		return
	}
	tasks, _ := c.repo.FindHistoryTasks(id)
	records := make([]M, len(tasks))
	for i, t := range tasks {
		ft := ""
		if t.FinishTime != nil { ft = t.FinishTime.Format("2006-01-02T15:04:05") }
		records[i] = M{"id": t.ID, "taskName": t.TaskName, "displayName": t.DisplayName,
			"taskState": int(t.TaskState), "operator": t.ActorID,
			"createTime": t.CreateTime.Format("2006-01-02T15:04:05"), "finishTime": ft}
	}
	r.Response.WriteJson(M{"code": 200, "data": M{
		"id": strconv.FormatInt(id, 10), "state": int(inst.State),
		"operator": inst.Operator, "createTime": inst.CreateTime.Format("2006-01-02T15:04:05"),
		"approvalRecords": records,
	}})
}

func (c *Controller) todoList(r *ghttp.Request) {
	var p struct{ UserID string `json:"userId"` }
	r.Parse(&p)
	var rows []M
	for _, t := range c.repo.AllTasks() {
		if t.TaskState != model.TaskStateDoing { continue }
		actors, _ := c.repo.FindTaskActors(t.ID)
		if !contains(t.ActorIDs, p.UserID) && !contains(actors, p.UserID) { continue }
		inst, _ := c.repo.FindInstanceByID(t.ProcessInstanceID)
		def, _ := c.repo.FindDefineByID(inst.DefineID)
		dn := ""
		if def != nil { dn = def.DisplayName }
		rows = append(rows, M{"id": t.ID, "taskName": t.TaskName, "displayName": t.DisplayName,
			"taskState": int(t.TaskState), "processInstanceId": t.ProcessInstanceID,
			"createTime": t.CreateTime.Format("2006-01-02T15:04:05"),
			"processDefineName": dn, "processDefineDisplayName": dn})
	}
	if rows == nil { rows = []M{} }
	r.Response.WriteJson(M{"code": 200, "data": M{"rows": rows, "pageNum": 1, "pageSize": 100, "recordCount": len(rows)}})
}

func (c *Controller) executeTask(r *ghttp.Request) {
		var p struct {
			ProcessTaskID string `json:"processTaskId"`
			Operator      string `json:"operator"`
			SubmitType    string `json:"submitType"`
		}
		r.Parse(&p)
		taskID, _ := strconv.ParseInt(p.ProcessTaskID, 10, 64)
		submitType, _ := strconv.Atoi(p.SubmitType)
		var err error
		args := M{"submitType": submitType}
		switch submitType {
		case 0, 1: // APPLY or AGREE
			_, err = c.engine.ExecuteProcessTask(taskID, p.Operator, args)
		case 2: // REJECT → jump back to apply node
			_, err = c.engine.ExecuteAndJumpTask(taskID, p.Operator, args, "apply")
		}
		if err != nil { r.Response.WriteJson(M{"code": 500, "message": err.Error()}); return }
		r.Response.WriteJson(M{"code": 200, "data": M{"message": "处理成功"}})
	}

func (c *Controller) stats(r *ghttp.Request) {
	userID := r.Get("userId", "user1").String()
	count := 0
	for _, t := range c.repo.AllTasks() {
		if t.TaskState == model.TaskStateDoing {
			actors, _ := c.repo.FindTaskActors(t.ID)
			if contains(t.ActorIDs, userID) || contains(actors, userID) { count++ }
		}
	}
	instCount := 0
	for _, inst := range c.repo.AllInstances() {
		if inst.Operator == userID { instCount++ }
	}
	r.Response.WriteJson(M{"code": 200, "data": M{"todoCount": count, "myInstanceCount": instCount}})
}

func contains(slice []string, item string) bool {
	for _, s := range slice { if s == item { return true } }
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
	if !ok { return false, nil }
	amt, ok := toFloat(v)
	if !ok { return false, nil }
	switch expr {
	case "amount > 1000": return amt > 1000, nil
	case "amount >= 1000": return amt >= 1000, nil
	case "amount < 1000": return amt < 1000, nil
	case "amount <= 1000": return amt <= 1000, nil
	case "amount == 1000": return amt == 1000, nil
	case "amount != 1000": return amt != 1000, nil
	}
	return false, nil
}

func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64: return val, true
	case int: return float64(val), true
	case int64: return float64(val), true
	case json.Number:
		f, err := val.Float64()
		return f, err == nil
	}
	return 0, false
}

var _ = time.Now // suppress import error
