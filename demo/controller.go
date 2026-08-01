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
		g.POST("/processDefine/page", c.definePage)
		g.POST("/processDefine/detail", c.defineDetail)
		g.POST("/processDefine/startAndExecute", c.startFlow)
		g.POST("/processInstance/startAndExecute", c.startFlow)
		g.POST("/processInstance/page", c.instancePage)
		g.POST("/processInstance/detail", c.instanceDetail)
		g.POST("/processInstance/highLight", c.instanceHighLight)
		g.POST("/processInstance/approvalRecord", c.instanceApprovalRecord)
		g.POST("/processTask/todoList", c.todoList)
		g.POST("/processTask/doneList", c.doneList)
		g.POST("/processTask/execute", c.executeTask)
		g.POST("/processTask/jumpAbleTaskNameList", c.jumpAbleTaskNameList)
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
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"rows": rows}})
}

func (c *Controller) defineDetail(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	def, err := c.repo.FindDefineByID(r.Context(), id)
	if err != nil || def == nil {
		r.Response.WriteJson(M{"code": 99999999, "msg": "流程定义不存在"})
		return
	}
	graphData := M{}
	json.Unmarshal(def.Content, &graphData)
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
		"jsonObject": graphData,
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
		inst, err := c.engine.StartProcessInstanceByID(r.Context(), defineID, p.Operator, args)
		if err != nil {
			r.Response.WriteJson(M{"code": 99999999, "msg": err.Error()})
			return
		}
		// boot2 startAndExecute：自动完成申请节点
		doing, _ := c.repo.FindDoingTasks(r.Context(), inst.ID, nil)
		for _, task := range doing {
			c.repo.AddTaskActor(r.Context(), task.ID, []string{p.Operator})
			args["submitType"] = 0 // APPLY
			c.engine.ExecuteProcessTask(r.Context(), task.ID, p.Operator, args)
		}
		r.Response.WriteJson(M{"code": 0, "msg": "成功"})
	}

func (c *Controller) instancePage(r *ghttp.Request) {
	var p struct{ Operator string `json:"operator"` }
	r.Parse(&p)
	insts := c.repo.AllInstances()
	rows := make([]M, 0)
	for _, inst := range insts {
		if inst.Operator != p.Operator { continue }
		def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID)
		dn := ""
		if def != nil { dn = def.DisplayName }
		rows = append(rows, M{"id": inst.ID, "processDefineId": inst.DefineID, "state": int(inst.State),
			"operator": inst.Operator, "businessNo": inst.BusinessNo,
			"createTime": inst.CreateTime.Format("2006-01-02 15:04:05"),
			"displayName": dn, "processDefineDisplayName": dn})
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"rows": rows, "pageNum": 1, "pageSize": 100, "recordCount": len(rows), "totalPage": 1}})
}

func (c *Controller) instanceDetail(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	inst, err := c.repo.FindInstanceByID(r.Context(), id)
	if err != nil || inst == nil {
		r.Response.WriteJson(M{"code": 99999999, "msg": "实例不存在"})
		return
	}
	def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID)
	dn := ""
	if def != nil { dn = def.DisplayName }
	// activeTaskList：进行中任务
	active := make([]M, 0)
	for _, t := range inst.Tasks {
		if t.TaskState == model.TaskStateDoing {
			active = append(active, c.taskVO(t, inst, def))
		}
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{
		"id": inst.ID, "processDefineId": inst.DefineID, "state": int(inst.State),
		"operator": inst.Operator, "businessNo": inst.BusinessNo,
		"createTime": inst.CreateTime.Format("2006-01-02 15:04:05"),
		"displayName": dn, "activeTaskList": active,
	}})
}

func (c *Controller) instanceHighLight(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	inst, err := c.repo.FindInstanceByID(r.Context(), id)
	if err != nil || inst == nil {
		r.Response.WriteJson(M{"code": 99999999, "msg": "实例不存在"})
		return
	}
	finished := make([]string, 0)
	active := make([]string, 0)
	for _, t := range inst.Tasks {
		if t.TaskState == model.TaskStateDone {
			finished = append(finished, t.TaskName)
		} else if t.TaskState == model.TaskStateDoing {
			active = append(active, t.TaskName)
		}
	}
	// 已完成边
	finishedEdges := make([]string, 0)
	var graph M
	if def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID); def != nil {
		json.Unmarshal(def.Content, &graph)
	}
	if edges, ok := graph["edges"].([]interface{}); ok {
		for _, e := range edges {
			em := e.(map[string]interface{})
			src, _ := em["sourceNodeId"].(string)
			tgt, _ := em["targetNodeId"].(string)
			if contains(finished, src) && contains(finished, tgt) {
				finishedEdges = append(finishedEdges, em["id"].(string))
			}
		}
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{
		"historyNodeNames": finished, "historyEdgeNames": finishedEdges, "activeNodeNames": active,
	}})
}

func (c *Controller) instanceApprovalRecord(r *ghttp.Request) {
	var p struct{ ID string `json:"id"` }
	r.Parse(&p)
	id, _ := strconv.ParseInt(p.ID, 10, 64)
	inst, err := c.repo.FindInstanceByID(r.Context(), id)
	if err != nil || inst == nil {
		r.Response.WriteJson(M{"code": 99999999, "msg": "实例不存在"})
		return
	}
	def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID)
	rows := make([]M, 0)
	for _, t := range inst.Tasks {
		rows = append(rows, c.taskVO(t, inst, def))
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": rows})
}

func (c *Controller) taskVO(t *model.ProcessTask, inst *model.ProcessInstance, def *model.ProcessDefine) M {
	ft := ""
	if t.FinishTime != nil { ft = t.FinishTime.Format("2006-01-02 15:04:05") }
	vo := M{"id": t.ID, "processInstanceId": t.ProcessInstanceID,
		"taskName": t.TaskName, "displayName": t.DisplayName,
		"taskType": t.TaskType, "performType": t.PerformType,
		"taskState": int(t.TaskState), "operator": t.ActorID,
		"finishTime": ft, "formKey": t.FormKey,
		"createTime": t.CreateTime.Format("2006-01-02 15:04:05"),
		"taskActorIdList": t.ActorIDs}
	if inst != nil && def != nil {
		vo["processDefineName"] = def.Name
		vo["processDefineDisplayName"] = def.DisplayName
	}
	return vo
}

func (c *Controller) todoList(r *ghttp.Request) {
	var p struct{ UserID string `json:"userId"` }
	r.Parse(&p)
	var rows []M
	for _, t := range c.repo.AllTasks() {
		if t.TaskState != model.TaskStateDoing { continue }
		actors, _ := c.repo.FindTaskActors(r.Context(), t.ID)
		if !contains(t.ActorIDs, p.UserID) && !contains(actors, p.UserID) { continue }
		inst, _ := c.repo.FindInstanceByID(r.Context(), t.ProcessInstanceID)
		def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID)
		rows = append(rows, c.taskVO(t, inst, def))
	}
	if rows == nil { rows = []M{} }
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"rows": rows, "pageNum": 1, "pageSize": 100, "recordCount": len(rows), "totalPage": 1}})
}

func (c *Controller) doneList(r *ghttp.Request) {
	var p struct{ UserID string `json:"userId"` }
	r.Parse(&p)
	var rows []M
	for _, t := range c.repo.AllTasks() {
		if t.TaskState != model.TaskStateDone { continue }
		if !contains(t.ActorIDs, p.UserID) && t.ActorID != p.UserID { continue }
		inst, _ := c.repo.FindInstanceByID(r.Context(), t.ProcessInstanceID)
		def, _ := c.repo.FindDefineByID(r.Context(), inst.DefineID)
		rows = append(rows, c.taskVO(t, inst, def))
	}
	if rows == nil { rows = []M{} }
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"rows": rows, "pageNum": 1, "pageSize": 100, "recordCount": len(rows), "totalPage": 1}})
}

// boot2 submitType: 0=APPLY 1=AGREE 2=REJECT 3=ROLLBACK 4=JUMP 5=RE_APPLY 6=ROLLBACK_TO_OPERATOR 20=COUNTERSIGN_DISAGREE
func (c *Controller) executeTask(r *ghttp.Request) {
		var p struct {
			ProcessTaskID string `json:"processTaskId"`
			Operator      string `json:"operator"`
			SubmitType    string `json:"submitType"`
			TaskName      string `json:"taskName"`
		}
		r.Parse(&p)
		taskID, _ := strconv.ParseInt(p.ProcessTaskID, 10, 64)
		submitType, _ := strconv.Atoi(p.SubmitType)
		var err error
		args := M{"submitType": submitType}
		switch submitType {
		case 0, 1, 5: // APPLY / AGREE / RE_APPLY
			_, err = c.engine.ExecuteProcessTask(r.Context(), taskID, p.Operator, args)
		case 2: // REJECT → 跳结束
			_, err = c.engine.ExecuteAndJumpToEnd(r.Context(), taskID, p.Operator, args)
		case 3: // ROLLBACK → 退回上一步
			_, err = c.engine.ExecuteAndJumpTask(r.Context(), taskID, p.Operator, args, "")
		case 4: // JUMP → 跳指定节点
			_, err = c.engine.ExecuteAndJumpTask(r.Context(), taskID, p.Operator, args, p.TaskName)
		case 6: // ROLLBACK_TO_OPERATOR → 退回发起人
			_, err = c.engine.ExecuteAndJumpToFirstTaskNode(r.Context(), taskID, p.Operator, args)
		case 20: // COUNTERSIGN_DISAGREE
			args["countersignDisagreeFlag"] = 1
			_, err = c.engine.ExecuteProcessTask(r.Context(), taskID, p.Operator, args)
		default:
			_, err = c.engine.ExecuteProcessTask(r.Context(), taskID, p.Operator, args)
		}
		if err != nil { r.Response.WriteJson(M{"code": 99999999, "msg": err.Error()}); return }
		r.Response.WriteJson(M{"code": 0, "msg": "成功"})
	}

func (c *Controller) jumpAbleTaskNameList(r *ghttp.Request) {
	var p struct{ ProcessInstanceID string `json:"processInstanceId"` }
	r.Parse(&p)
	instID, _ := strconv.ParseInt(p.ProcessInstanceID, 10, 64)
	done, _ := c.repo.FindDoneTasks(r.Context(), instID, nil)
	seen := map[string]bool{}
	rows := make([]M, 0)
	for _, t := range done {
		if !seen[t.TaskName] {
			seen[t.TaskName] = true
			rows = append(rows, M{"label": t.DisplayName, "value": t.TaskName})
		}
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": rows})
}

func (c *Controller) stats(r *ghttp.Request) {
	userID := r.Get("userId", "user1").String()
	count := 0
	for _, t := range c.repo.AllTasks() {
		if t.TaskState == model.TaskStateDoing {
			actors, _ := c.repo.FindTaskActors(r.Context(), t.ID)
			if contains(t.ActorIDs, userID) || contains(actors, userID) { count++ }
		}
	}
	instCount := 0
	for _, inst := range c.repo.AllInstances() {
		if inst.Operator == userID { instCount++ }
	}
	r.Response.WriteJson(M{"code": 0, "msg": "成功", "data": M{"todoCount": count, "myInstanceCount": instCount}})
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
