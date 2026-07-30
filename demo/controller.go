package demo

import (
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
	eng := engine.New(repo, &demoUserProvider{}, &demoIDGen{}, nil)
	loadFlows(repo)
	return &Controller{engine: eng, repo: repo}
}

func loadFlows(repo *memory.Repository) {
	type f struct {
		name, display, content string
	}
	flows := []f{
		{"leave", "请假审批", `{"name":"leave","displayName":"请假审批","type":"approval","nodes":[{"id":"start","type":"snaker:start","x":100,"y":200,"properties":{},"text":{"value":"开始"}},{"id":"task1","type":"snaker:task","x":300,"y":200,"properties":{"form":"leave-form","assignee":"leader","taskType":0,"performType":0},"text":{"value":"组长审批"}},{"id":"end","type":"snaker:end","x":500,"y":200,"properties":{},"text":{"value":"结束"}}],"edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"task1","properties":{}},{"id":"e2","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`},
		{"three-level", "三级审批", `{"name":"three-level","displayName":"三级审批","type":"approval","nodes":[{"id":"start","type":"snaker:start","x":100,"y":200,"properties":{},"text":{"value":"开始"}},{"id":"t1","type":"snaker:task","x":250,"y":200,"properties":{"form":"approval-form","assignee":"leader","taskType":0,"performType":0},"text":{"value":"组长审批"}},{"id":"t2","type":"snaker:task","x":400,"y":200,"properties":{"form":"approval-form","assignee":"manager","taskType":0,"performType":0},"text":{"value":"经理审批"}},{"id":"t3","type":"snaker:task","x":550,"y":200,"properties":{"form":"approval-form","assignee":"boss","taskType":0,"performType":0},"text":{"value":"总监审批"}},{"id":"end","type":"snaker:end","x":700,"y":200,"properties":{},"text":{"value":"结束"}}],"edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"t1","properties":{}},{"id":"e2","sourceNodeId":"t1","targetNodeId":"t2","properties":{}},{"id":"e3","sourceNodeId":"t2","targetNodeId":"t3","properties":{}},{"id":"e4","sourceNodeId":"t3","targetNodeId":"end","properties":{}}]}`},
		{"expense", "报销审批", `{"name":"expense","displayName":"报销审批","type":"finance","nodes":[{"id":"start","type":"snaker:start","x":100,"y":200,"properties":{},"text":{"value":"开始"}},{"id":"apply","type":"snaker:task","x":300,"y":200,"properties":{"form":"expense-form","assignee":"leader","taskType":0,"performType":0},"text":{"value":"填写报销单"}},{"id":"decision","type":"snaker:decision","x":500,"y":200,"properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000?"}},{"id":"manager","type":"snaker:task","x":700,"y":100,"properties":{"form":"expense-form","assignee":"manager","taskType":0,"performType":0},"text":{"value":"经理审批"}},{"id":"director","type":"snaker:task","x":700,"y":300,"properties":{"form":"expense-form","assignee":"director","taskType":0,"performType":0},"text":{"value":"总监审批"}},{"id":"end","type":"snaker:end","x":900,"y":200,"properties":{},"text":{"value":"结束"}}],"edges":[{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},{"id":"e2","sourceNodeId":"apply","targetNodeId":"decision","properties":{}},{"id":"e3","sourceNodeId":"decision","targetNodeId":"manager","properties":{"expr":"amount > 1000"},"text":{"value":"金额>1000"}},{"id":"e4","sourceNodeId":"decision","targetNodeId":"director","properties":{"expr":"amount <= 1000"},"text":{"value":"金额≤1000"}},{"id":"e5","sourceNodeId":"manager","targetNodeId":"end","properties":{}},{"id":"e6","sourceNodeId":"director","targetNodeId":"end","properties":{}}]}`},
	}
	for i, f := range flows {
		repo.AddDefine(&model.ProcessDefine{
			ID: int64(i + 1), Name: f.name, DisplayName: f.display, Type: "approval", State: 1, Content: []byte(f.content),
		})
	}
}

func (c *Controller) RegisterRoutes(s *ghttp.Server) {
	s.Group("/wf", func(g *ghttp.RouterGroup) {
		g.POST("/processDefine/page", c.definePage)
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
	case 1: _, err = c.engine.ExecuteProcessTask(taskID, p.Operator, args)
	case 2: _, err = c.engine.ExecuteAndJumpToEnd(taskID, p.Operator, args)
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

var _ = time.Now // suppress import error
