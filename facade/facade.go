// 统一门面（v1.1.0）——"接口即 POST + JSON body"风格的单入口
//
// 集成方只实现一个转发 controller/endpoint：把 body JSON 转成 map 传入 Flow，
// 所有流程能力按 action（boot2/boot3 端点短名）路由。返回统一结构 {code, msg, data}
// （code=0 成功 / 99999999 失败）。
//
// 操作人约定：门面不感知登录态，args["operator"] 显式传入。
package facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// UserSearch 用户搜索钩子（v1.2.0，可选）——candidatePage 无模型候选时的用户分页搜索。
// 返回 (rows, total, err)；query 透传 pageNum/pageSize/搜索条件。
type UserSearch func(query map[string]interface{}) ([]map[string]interface{}, int, error)

// Facade 统一门面
type Facade struct {
	engine     *engine.EngineImpl
	repo       spi.ProcessRepository
	extRepo    spi.ProcessExtRepository // 可空：未接入时设计/委托 action 报错
	userSearch UserSearch               // 可空：candidatePage 用户搜索依赖
}

// SetUserSearch 注入用户搜索钩子
func (f *Facade) SetUserSearch(fn UserSearch) *Facade {
	f.userSearch = fn
	return f
}

// New 构造门面
func New(e *engine.EngineImpl, repo spi.ProcessRepository, ext spi.ProcessExtRepository) *Facade {
	return &Facade{engine: e, repo: repo, extRepo: ext}
}

// Flow 统一入口（action 清单见 spec §11.2）
func (f *Facade) Flow(action string, args map[string]interface{}) (r map[string]interface{}) {
	// 扩展仓储未配置等内部 panic 收敛为业务错误
	defer func() {
		if p := recover(); p != nil {
			r = errorResult(fmt.Sprintf("%v", p))
		}
	}()
	if args == nil {
		args = map[string]interface{}{}
	}
	var err error
	var data interface{}
	switch action {
	case "processDefine/page":
		data, err = f.definePage(args)
	case "processDefine/detail":
		data, err = f.defineDetail(args)
	case "processDefine/startAndExecute":
		data, err = f.startAndExecute(args)
	case "processDefine/deploy":
		data, err = f.deploy(args)
	case "processDefine/redeploy":
		err = f.redeploy(args)
	case "processDefine/remove":
		err = f.removeDefine(args)
	case "processDefine/upAndDown":
		err = f.upAndDown(args)
	case "processInstance/page":
		data, err = f.instancePage(args)
	case "processInstance/detail":
		data, err = f.instanceDetail(args)
	case "processInstance/startAndExecute":
		data, err = f.startAndExecute(args)
	case "processInstance/withdraw":
		err = f.withdraw(args)
	case "processTask/execute":
		err = f.execute(args)
	case "processTask/todoList":
		data, err = f.todoList(args)
	case "processTask/doneList":
		data, err = f.doneList(args)
	case "processDesign/page":
		data, err = f.designPage(args)
	case "processDesign/detail":
		data, err = f.designDetail(args)
	case "processDesign/save":
		data, err = f.designSave(args)
	case "processDesign/remove":
		err = f.designRemove(args)
	case "processDesign/deploy":
		data, err = f.designDeploy(args)
	case "processSurrogate/page":
		data, err = f.surrogatePage(args)
	case "processSurrogate/save":
		data, err = f.surrogateSave(args)
	case "processSurrogate/remove":
		err = f.surrogateRemove(args)
	case "processDefine/getLastByName":
		data, err = f.getLastByName(args)
	case "processInstance/highLight":
		data, err = f.highLight(args)
	case "processInstance/approvalRecord":
		data, err = f.approvalRecord(args)
	case "processInstance/getAssigneeTextData":
		data, err = f.getAssigneeTextData(args)
	case "processInstance/createCCInstance":
		err = f.createCCInstance(args)
	case "processInstance/updateCCStatus":
		err = f.updateCCStatus(args)
	case "processInstance/ccList":
		data, err = f.ccList(args)
	case "processTask/detail":
		data, err = f.taskDetail(args)
	case "processTask/jumpAbleTaskNameList":
		data, err = f.jumpAbleTaskNameList(args)
	case "processTask/candidatePage":
		data, err = f.candidatePage(args)
	case "processTask/surrogate":
		err = f.taskAddActor(args)
	case "processTask/addCandidate":
		err = f.taskAddActor(args)
	case "processTask/latest":
		data, err = f.taskLatest(args)
	default:
		return errorResult("未知 action: " + action)
	}
	if err != nil {
		return errorResult(err.Error())
	}
	return okResult(data)
}

// ═══ 流程定义 / 实例 ═══

func (f *Facade) startAndExecute(args map[string]interface{}) (interface{}, error) {
	defineID, err := toInt64(args["processDefineId"])
	if err != nil {
		return nil, errors.New("processDefineId 缺失或非法")
	}
	operator := toStr(args["operator"], "user1")
	flowArgs := map[string]interface{}{}
	for k, v := range args {
		if k == "processDefineId" || k == "operator" {
			continue
		}
		flowArgs[k] = v
	}
	inst, err := f.engine.StartProcessInstanceByID(context.Background(), defineID, operator, flowArgs)
	if err != nil {
		return nil, err
	}
	// startAndExecute：自动完成申请节点（assignee="applicant" → 发起人）
	doing, err := f.repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil {
		return nil, err
	}
	for _, task := range doing {
		_ = f.repo.AddTaskActor(context.Background(), task.ID, []string{operator})
		flowArgs["submitType"] = 0 // APPLY
		if _, err := f.engine.ExecuteProcessTask(context.Background(), task.ID, operator, flowArgs); err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{"processInstanceId": inst.ID}, nil
}

// deploy 版本管理（对齐 boot3）：按 name 查最新定义，存在 version+1 插新记录，否则从 0 起
func (f *Facade) deploy(args map[string]interface{}) (interface{}, error) {
	content, err := contentBytes(args)
	if err != nil {
		return nil, err
	}
	var flow model.FlowModel
	if err := json.Unmarshal(content, &flow); err != nil {
		return nil, fmt.Errorf("流程定义 JSON 解析失败: %w", err)
	}
	if flow.Name == "" {
		return nil, errors.New("流程定义缺少 name")
	}
	version := 0
	latest, err := f.repo.FindDefineByName(context.Background(), flow.Name)
	if err != nil {
		return nil, err
	}
	if latest != nil {
		version = latest.Version + 1
	}
	def := &model.ProcessDefine{
		Name:        flow.Name,
		DisplayName: flow.DisplayName,
		Type:        flow.Type,
		State:       1,
		Content:     content,
		Version:     version,
		CreateUser:  toStr(args["operator"], "system"),
		UpdateUser:  toStr(args["operator"], "system"),
	}
	if err := f.repo.SaveDefine(context.Background(), def); err != nil {
		return nil, err
	}
	return map[string]interface{}{"processDefineId": def.ID}, nil
}

func (f *Facade) redeploy(args map[string]interface{}) error {
	defineID, err := toInt64(args["processDefineId"])
	if err != nil {
		return errors.New("processDefineId 缺失或非法")
	}
	content, err := contentBytes(args)
	if err != nil {
		return err
	}
	var flow model.FlowModel
	if err := json.Unmarshal(content, &flow); err != nil {
		return fmt.Errorf("流程定义 JSON 解析失败: %w", err)
	}
	return f.repo.UpdateDefine(context.Background(), &model.ProcessDefine{
		ID:          defineID,
		Name:        flow.Name,
		DisplayName: flow.DisplayName,
		Type:        flow.Type,
		Content:     content,
		UpdateUser:  toStr(args["operator"], "system"),
	})
}

func (f *Facade) removeDefine(args map[string]interface{}) error {
	id, err := toInt64(args["id"])
	if err != nil {
		return errors.New("id 缺失或非法")
	}
	return f.repo.RemoveDefine(context.Background(), id)
}

func (f *Facade) upAndDown(args map[string]interface{}) error {
	id, err := toInt64(args["id"])
	if err != nil {
		return errors.New("id 缺失或非法")
	}
	state, err := toInt(args["state"])
	if err != nil {
		return errors.New("state 缺失或非法")
	}
	return f.repo.UpdateDefineState(context.Background(), id, state)
}

func (f *Facade) withdraw(args map[string]interface{}) error {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return errors.New("id 缺失或非法")
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), instanceID)
	if err != nil || inst == nil {
		return errors.New("流程实例不存在")
	}
	// 撤回：废弃全部 doing 任务 + 实例状态（v1.0.1：updateInstance 级联落库）
	operator := toStr(args["operator"], "user1")
	now := time.Now()
	abandoned := inst.AbandonAllDoing(now)
	inst.Reject(now)
	inst.UpdateUser = operator
	for _, t := range abandoned {
		if err := f.repo.UpdateTask(context.Background(), t); err != nil {
			return err
		}
	}
	return f.repo.UpdateInstance(context.Background(), inst)
}

// ═══ 流程任务 ═══

func (f *Facade) execute(args map[string]interface{}) error {
	taskID, err := toInt64(args["processTaskId"])
	if err != nil {
		return errors.New("processTaskId 缺失或非法")
	}
	operator := toStr(args["operator"], "user1")
	submitType, err := toInt(args["submitType"])
	if err != nil {
		submitType = 1 // AGREE
	}
	flowArgs := map[string]interface{}{}
	for k, v := range args {
		if k == "processTaskId" || k == "operator" {
			continue
		}
		flowArgs[k] = v
	}
	flowArgs["submitType"] = submitType
	// boot3 execute 分发（spec §11.2）
	switch submitType {
	case 2: // REJECT
		_, err = f.engine.ExecuteAndJumpToEnd(context.Background(), taskID, operator, flowArgs)
	case 3: // ROLLBACK
		_, err = f.engine.ExecuteAndJumpTask(context.Background(), taskID, operator, flowArgs, "")
	case 4: // JUMP
		_, err = f.engine.ExecuteAndJumpTask(context.Background(), taskID, operator, flowArgs, toStr(args["taskName"], ""))
	case 6: // ROLLBACK_TO_OPERATOR
		_, err = f.engine.ExecuteAndJumpToFirstTaskNode(context.Background(), taskID, operator, flowArgs)
	case 20: // COUNTERSIGN_DISAGREE
		flowArgs["countersignDisagreeFlag"] = 1
		_, err = f.engine.ExecuteProcessTask(context.Background(), taskID, operator, flowArgs)
	default: // 0 APPLY / 1 AGREE / 5 重新提交
		_, err = f.engine.ExecuteProcessTask(context.Background(), taskID, operator, flowArgs)
	}
	return err
}

// ═══ 流程设计（需扩展仓储） ═══

func (f *Facade) designPage(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	rows, total, err := ext.PageDesigns(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return pageData(query.PageNum, query.PageSize, total, rows), nil
}

func (f *Facade) designDetail(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	design, err := ext.FindDesignByID(context.Background(), id)
	if err != nil || design == nil {
		return nil, errors.New("流程设计不存在")
	}
	data := map[string]interface{}{
		"id": design.ID, "name": design.Name, "displayName": design.DisplayName,
		"type": design.Type, "icon": design.Icon, "isDeployed": design.IsDeployed,
		"remark": design.Remark,
	}
	hisList, err := ext.ListDesignHis(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if len(hisList) > 0 {
		var graph map[string]interface{}
		if json.Unmarshal(hisList[0].Content, &graph) == nil {
			data["jsonObject"] = graph
		}
	}
	data["his"] = hisList
	return data, nil
}

func (f *Facade) designSave(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	operator := toStr(args["operator"], "user1")
	id, _ := toInt64(args["id"])
	var design *model.ProcessDesign
	var err error
	if id == 0 {
		design = &model.ProcessDesign{
			Name:        toStr(args["name"], ""),
			DisplayName: toStr(args["displayName"], ""),
			Type:        toStr(args["type"], "approval"),
			Icon:        toStr(args["icon"], ""),
			Remark:      toStr(args["remark"], ""),
			CreateUser:  operator,
			UpdateUser:  operator,
		}
		err = ext.SaveDesign(context.Background(), design)
	} else {
		design, err = ext.FindDesignByID(context.Background(), id)
		if err != nil || design == nil {
			return nil, errors.New("流程设计不存在")
		}
		if v, ok := args["displayName"]; ok {
			design.DisplayName = toStr(v, "")
		}
		if v, ok := args["type"]; ok {
			design.Type = toStr(v, "")
		}
		if v, ok := args["icon"]; ok {
			design.Icon = toStr(v, "")
		}
		if v, ok := args["remark"]; ok {
			design.Remark = toStr(v, "")
		}
		design.UpdateUser = operator
		err = ext.UpdateDesign(context.Background(), design)
	}
	if err != nil {
		return nil, err
	}
	// 内容快照（设计稿内容存历史表）
	if content, cerr := contentBytes(args); cerr == nil && content != nil {
		if herr := ext.SaveDesignHis(context.Background(), &model.ProcessDesignHis{
			ProcessDesignID: design.ID,
			Content:         content,
			CreateUser:      operator,
		}); herr != nil {
			return nil, herr
		}
	}
	return map[string]interface{}{"id": design.ID}, nil
}

func (f *Facade) designRemove(args map[string]interface{}) error {
	id, err := toInt64(args["id"])
	if err != nil {
		return errors.New("id 缺失或非法")
	}
	return f.ext().RemoveDesign(context.Background(), id)
}

func (f *Facade) designDeploy(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	design, err := ext.FindDesignByID(context.Background(), id)
	if err != nil || design == nil {
		return nil, errors.New("流程设计不存在")
	}
	hisList, err := ext.ListDesignHis(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if len(hisList) == 0 {
		return nil, errors.New("流程设计没有内容，无法发布")
	}
	// 发布：复用 deploy 版本管理
	defineID, err := f.deploy(map[string]interface{}{
		"content":  hisList[0].Content,
		"operator": toStr(args["operator"], "system"),
	})
	if err != nil {
		return nil, err
	}
	design.IsDeployed = 1
	design.UpdateUser = toStr(args["operator"], "system")
	if err := ext.UpdateDesign(context.Background(), design); err != nil {
		return nil, err
	}
	return defineID, nil
}

// ═══ 委托代理（需扩展仓储） ═══

func (f *Facade) surrogatePage(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	if v, ok := args["operator"]; ok {
		query.Filters = map[string]interface{}{"operator": toStr(v, "")}
	}
	rows, total, err := ext.PageSurrogates(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return pageData(query.PageNum, query.PageSize, total, rows), nil
}

func (f *Facade) surrogateSave(args map[string]interface{}) (interface{}, error) {
	ext := f.ext()
	operator := toStr(args["operator"], "user1")
	id, _ := toInt64(args["id"])
	var surrogate *model.ProcessSurrogate
	var err error
	if id == 0 {
		surrogate = &model.ProcessSurrogate{
			Operator:    operator, // 授权人 = 操作人
			Surrogate:   toStr(args["surrogate"], ""),
			ProcessName: toStr(args["processName"], ""),
			CreateUser:  operator,
			UpdateUser:  operator,
		}
		if v, ok := args["startTime"].(string); ok && v != "" {
			if t, perr := time.Parse("2006-01-02T15:04:05", v); perr == nil {
				surrogate.StartTime = &t
			}
		}
		if v, ok := args["endTime"].(string); ok && v != "" {
			if t, perr := time.Parse("2006-01-02T15:04:05", v); perr == nil {
				surrogate.EndTime = &t
			}
		}
		surrogate.Enabled = toIntDef(args["enabled"], 1)
		err = ext.SaveSurrogate(context.Background(), surrogate)
	} else {
		surrogate, err = ext.FindSurrogateByID(context.Background(), id)
		if err != nil || surrogate == nil {
			return nil, errors.New("委托记录不存在")
		}
		surrogate.Surrogate = toStr(args["surrogate"], surrogate.Surrogate)
		if v, ok := args["processName"]; ok {
			surrogate.ProcessName = toStr(v, "")
		}
		if v, ok := args["enabled"]; ok {
			surrogate.Enabled = toIntDef(v, 1)
		}
		surrogate.UpdateUser = operator
		err = ext.UpdateSurrogate(context.Background(), surrogate)
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"id": surrogate.ID}, nil
}

func (f *Facade) surrogateRemove(args map[string]interface{}) error {
	id, err := toInt64(args["id"])
	if err != nil {
		return errors.New("id 缺失或非法")
	}
	return f.ext().RemoveSurrogate(context.Background(), id)
}

// ═══ 视图端点（v1.2.0） ═══

func (f *Facade) getLastByName(args map[string]interface{}) (interface{}, error) {
	name := toStr(args["processDefineName"], "")
	def, err := f.repo.FindDefineByName(context.Background(), name)
	if err != nil || def == nil {
		return nil, fmt.Errorf("流程定义不存在: %s", name)
	}
	graph := map[string]interface{}{}
	if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
		// 前端表单渲染/流程图依赖（issues/05）
		return map[string]interface{}{
			"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
			"type": def.Type, "state": def.State, "version": def.Version,
			"jsonObject": graph,
		}, nil
	}
	return map[string]interface{}{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
	}, nil
}

func (f *Facade) highLight(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), instanceID)
	if err != nil || inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	active := []string{}
	history := []string{}
	edges := []string{}
	// 活跃节点 = 进行中任务
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, t := range doing {
		if !containsStr(active, t.TaskName) {
			active = append(active, t.TaskName)
		}
	}
	// 历史节点 = 全部任务（排除活跃）+ 模型路径补全
	his, _ := f.repo.FindHistoryTasks(context.Background(), instanceID)
	for _, t := range his {
		if !containsStr(active, t.TaskName) && !containsStr(history, t.TaskName) {
			history = append(history, t.TaskName)
		}
	}
	// 路径补全：start 沿 edges 递归，遇活跃节点停止；决策分支按表达式求值过滤（issues/06）
	def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
	if def != nil {
		var flow model.FlowModel
		if json.Unmarshal(def.Content, &flow) == nil {
			f.collectPath(&flow, "start", "", active, &history, &edges, map[string]bool{}, inst.Variables, his)
		}
	}
	return map[string]interface{}{
		"activeNodeNames":  active,
		"historyNodeNames": history,
		"historyEdgeNames": edges,
	}, nil
}

// collectPath 从节点沿输出边递归（遇活跃节点停止），补全历史节点与边；
// 决策节点输出边带 expr 时用表达式求值过滤（对齐 boot3 recursionModel，issues/06）
func (f *Facade) collectPath(flow *model.FlowModel, nodeID, edgeName string, active []string,
	history *[]string, edges *[]string, visited map[string]bool,
	vars map[string]interface{}, historyTasks []*model.ProcessTask) {
	if visited[nodeID] {
		return
	}
	visited[nodeID] = true
	if edgeName != "" && !containsStr(*edges, edgeName) {
		*edges = append(*edges, edgeName)
	}
	src := findNodeIn(flow, nodeID)
	for _, e := range flow.Edges {
		if e.SourceNodeID != nodeID {
			continue
		}
		// 决策节点：输出边表达式求值过滤——false 的分支未实际执行，不收集
		if src != nil && src.Type == "snaker:decision" {
			if expr, ok := e.Properties["expr"].(string); ok && expr != "" {
				if ok2, _ := f.evalDecisionExpr(flow, src, expr, vars, historyTasks); !ok2 {
					continue
				}
			}
		}
		target := findNodeIn(flow, e.TargetNodeID)
		if target == nil {
			continue
		}
		if !containsStr(active, target.ID) && !containsStr(*history, target.ID) {
			*history = append(*history, target.ID)
		}
		if containsStr(active, target.ID) {
			continue // 遇活跃节点停止深入
		}
		f.collectPath(flow, target.ID, e.ID, active, history, edges, visited, vars, historyTasks)
	}
}

// evalDecisionExpr 决策输出边表达式求值（args = 实例变量 + 决策节点前置任务变量，与引擎同源）
func (f *Facade) evalDecisionExpr(flow *model.FlowModel, decision *model.FlowNode, expr string,
	vars map[string]interface{}, historyTasks []*model.ProcessTask) (bool, error) {
	args := map[string]interface{}{}
	for k, v := range vars {
		args[k] = v
	}
	// 前置任务变量：决策节点输入的第一个源节点对应的历史任务
	for _, e := range flow.Edges {
		if e.TargetNodeID == decision.ID {
			for _, t := range historyTasks {
				if t.TaskName == e.SourceNodeID {
					for k, v := range t.Variables {
						args[k] = v
					}
					break
				}
			}
			break
		}
	}
	result, err := f.engine.EvalExpr(expr, args)
	if err != nil {
		return false, nil
	}
	b, _ := result.(bool)
	return b, nil
}

func findNodeIn(flow *model.FlowModel, id string) *model.FlowNode {
	for i := range flow.Nodes {
		if flow.Nodes[i].ID == id {
			return &flow.Nodes[i]
		}
	}
	return nil
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (f *Facade) approvalRecord(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	his, err := f.repo.FindHistoryTasks(context.Background(), instanceID)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]interface{}, 0, len(his))
	for _, t := range his {
		rows = append(rows, map[string]interface{}{
			"taskName": t.TaskName, "displayName": t.DisplayName,
			"taskType": t.TaskType, "performType": t.PerformType,
			"taskState": t.TaskState, "operator": t.ActorID,
			"finishTime": t.FinishTime, "variable": t.Variables,
		})
	}
	return rows, nil
}

func (f *Facade) getAssigneeTextData(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	includeNodeName := true
	if v, ok := args["includeNodeName"].(bool); ok {
		includeNodeName = v
	}
	rows := []map[string]interface{}{}
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, t := range doing {
		actors, _ := f.repo.FindTaskActors(context.Background(), t.ID)
		for _, actor := range actors {
			label := actor
			if includeNodeName {
				label = t.DisplayName + ":" + actor
			}
			rows = append(rows, map[string]interface{}{"label": label, "value": actor})
		}
	}
	return rows, nil
}

func (f *Facade) createCCInstance(args map[string]interface{}) error {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return errors.New("processInstanceId 缺失或非法")
	}
	operator := toStr(args["operator"], "user1")
	actors := toStringSlice2(args["actorIds"])
	if len(actors) == 0 {
		return errors.New("actorIds 缺失")
	}
	return f.repo.CreateCcInstance(context.Background(), instanceID, operator, actors...)
}

func (f *Facade) updateCCStatus(args map[string]interface{}) error {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return errors.New("processInstanceId 缺失或非法")
	}
	operator := toStr(args["operator"], "user1")
	return f.repo.UpdateCcStatus(context.Background(), instanceID, operator)
}

// ccList 我的抄送分页（v1.3.0）：operator 作为抄送人过滤
func (f *Facade) ccList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	actorID := toStr(args["operator"], "user1")
	rows, total, err := f.repo.PageCcInstances(context.Background(), query, actorID)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, ccRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

func (f *Facade) taskDetail(args map[string]interface{}) (interface{}, error) {
	taskID, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	operator := toStr(args["operator"], "user1")
	task, err := f.repo.FindTaskByID(context.Background(), taskID)
	if err != nil || task == nil {
		return nil, errors.New("任务不存在")
	}
	actors, _ := f.repo.FindTaskActors(context.Background(), taskID)
	vo := map[string]interface{}{
		"id": task.ID, "processInstanceId": task.ProcessInstanceID,
		"taskName": task.TaskName, "displayName": task.DisplayName,
		"taskType": task.TaskType, "performType": task.PerformType,
		"taskState": task.TaskState, "operator": task.ActorID,
		"formKey": task.FormKey, "taskActorIdList": actors,
		"executable": task.IsAllowed(operator),
	}
	// taskModel：流程定义中对应节点
	inst, _ := f.repo.FindInstanceByID(context.Background(), task.ProcessInstanceID)
	if inst != nil {
		def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
		if def != nil {
			graph := map[string]interface{}{}
			if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
				vo["jsonObject"] = graph // issues/05
			}
			var flow model.FlowModel
			if json.Unmarshal(def.Content, &flow) == nil {
				for _, n := range flow.Nodes {
					if n.ID == task.TaskName {
						vo["taskModel"] = map[string]interface{}{
							"name": n.ID, "displayName": n.Text.Value, "type": n.Type,
						}
						break
					}
				}
			}
		}
	}
	return vo, nil
}

func (f *Facade) jumpAbleTaskNameList(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return nil, errors.New("processInstanceId 缺失或非法")
	}
	done, _ := f.repo.FindDoneTasks(context.Background(), instanceID, nil)
	rows := []map[string]interface{}{}
	seen := map[string]bool{}
	for _, t := range done {
		if t.PerformType == 1 { // COUNTERSIGN
			continue
		}
		if !seen[t.TaskName] {
			seen[t.TaskName] = true
			rows = append(rows, map[string]interface{}{"label": t.DisplayName, "value": t.TaskName})
		}
	}
	return rows, nil
}

func (f *Facade) candidatePage(args map[string]interface{}) (interface{}, error) {
	taskID, err := toInt64(args["processTaskId"])
	if err != nil {
		taskID, err = toInt64(args["id"])
	}
	if err != nil {
		return nil, errors.New("processTaskId 缺失")
	}
	task, err := f.repo.FindTaskByID(context.Background(), taskID)
	if err != nil || task == nil {
		return nil, errors.New("任务不存在")
	}
	inst, _ := f.repo.FindInstanceByID(context.Background(), task.ProcessInstanceID)
	if inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	// 模型候选解析：当前任务的后继任务节点的 candidateUsers 配置
	var candidates []string
	def, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID)
	if def != nil {
		var flow model.FlowModel
		if json.Unmarshal(def.Content, &flow) == nil {
			candidates = nextTaskCandidates(&flow, task.TaskName)
		}
	}
	if len(candidates) > 0 {
		// 候选命中 → 用户信息映射（UserProvider 兜底）
		rows := []map[string]interface{}{}
		for _, c := range candidates {
			rows = append(rows, map[string]interface{}{"userId": c, "realName": c})
		}
		return pageData(1, 10, len(rows), rows), nil
	}
	// 无模型候选 → 用户分页搜索（依赖 UserSearch 钩子）
	if f.userSearch == nil {
		return nil, errors.New("未配置 UserSearch（用户搜索钩子）")
	}
	rows, total, err := f.userSearch(args)
	if err != nil {
		return nil, err
	}
	return pageData(toIntDef(args["pageNum"], 1), toIntDef(args["pageSize"], 10), total, rows), nil
}

// nextTaskCandidates 找当前任务节点的后继任务节点，收集 candidateUsers（逗号分割）
func nextTaskCandidates(flow *model.FlowModel, taskName string) []string {
	var result []string
	collect := func(node *model.FlowNode) {
		if v, ok := node.Properties["candidateUsers"].(string); ok && v != "" {
			for _, s := range strings.Split(v, ",") {
				s = strings.TrimSpace(s)
				if s != "" && !containsStr(result, s) {
					result = append(result, s)
				}
			}
		}
	}
	visited := map[string]bool{}
	var walk func(id string)
	walk = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		for _, e := range flow.Edges {
			if e.SourceNodeID != id {
				continue
			}
			target := findNodeIn(flow, e.TargetNodeID)
			if target == nil {
				continue
			}
			if target.Type == model.TypeTask || target.Type == model.TypeCustom {
				collect(target)
				continue
			}
			if target.Type == model.TypeFork || target.Type == model.TypeJoin ||
				target.Type == model.TypeDecision {
				walk(target.ID)
			}
		}
	}
	walk(taskName)
	return result
}

func (f *Facade) taskAddActor(args map[string]interface{}) error {
	taskID, err := toInt64(args["processTaskId"])
	if err != nil {
		return errors.New("processTaskId 缺失或非法")
	}
	actors := toStringSlice2(args["actorIds"])
	if len(actors) == 0 {
		return errors.New("actorIds 缺失")
	}
	return f.repo.AddTaskActor(context.Background(), taskID, actors)
}

func (f *Facade) taskLatest(args map[string]interface{}) (interface{}, error) {
	instanceID, err := toInt64(args["processInstanceId"])
	if err != nil {
		return nil, errors.New("processInstanceId 缺失或非法")
	}
	doing, _ := f.repo.FindDoingTasks(context.Background(), instanceID, nil)
	if len(doing) == 0 {
		return nil, nil
	}
	t := doing[0]
	return map[string]interface{}{
		"id": t.ID, "taskName": t.TaskName, "displayName": t.DisplayName,
		"taskState": t.TaskState, "operator": t.ActorID,
	}, nil
}

// toStringSlice2 把 actorIds（数组或逗号串）转列表
func toStringSlice2(v interface{}) []string {
	var list []string
	switch t := v.(type) {
	case []string:
		list = append(list, t...)
	case []interface{}:
		for _, s := range t {
			list = append(list, fmt.Sprintf("%v", s))
		}
	case string:
		for _, s := range strings.Split(t, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				list = append(list, s)
			}
		}
	}
	return list
}

// ═══ 工具 ═══

func (f *Facade) ext() spi.ProcessExtRepository {
	if f.extRepo == nil {
		panic("未配置 ProcessExtRepository（扩展仓储）")
	}
	return f.extRepo
}

func contentBytes(args map[string]interface{}) ([]byte, error) {
	content, ok := args["content"]
	if !ok || content == nil {
		return nil, errors.New("content 缺失")
	}
	switch v := content.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, errors.New("content 必须是字符串或字节数组")
	}
}

func pageData(pageNum, pageSize, total int, rows interface{}) map[string]interface{} {
	totalPage := 0
	if total > 0 {
		totalPage = (total + pageSize - 1) / pageSize
	}
	return map[string]interface{}{
		"pageNum": pageNum, "pageSize": pageSize,
		"recordCount": total, "totalPage": totalPage, "rows": rows,
	}
}

func okResult(data interface{}) map[string]interface{} {
	return map[string]interface{}{"code": 0, "msg": "成功", "data": data}
}

func errorResult(msg string) map[string]interface{} {
	return map[string]interface{}{"code": 99999999, "msg": msg}
}

func toStr(v interface{}, def string) string {
	if v == nil {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	case nil:
		return 0, errors.New("缺失")
	default:
		return 0, fmt.Errorf("非法数值: %v", v)
	}
}

func toInt(v interface{}) (int, error) {
	n, err := toInt64(v)
	return int(n), err
}

func toIntDef(v interface{}, def int) int {
	if v == nil {
		return def
	}
	if n, err := toInt(v); err == nil {
		return n
	}
	return def
}

// ═══ 基础分页/详情（v1.5.0 补齐，对齐 Java 门面）═══

// definePage 流程定义分页
func (f *Facade) definePage(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	rows, total, err := f.repo.PageDefines(context.Background(), query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, defineRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// defineDetail 流程定义详情
func (f *Facade) defineDetail(args map[string]interface{}) (interface{}, error) {
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	def, err := f.repo.FindDefineByID(context.Background(), id)
	if err != nil || def == nil {
		return nil, errors.New("流程定义不存在")
	}
	graph := map[string]interface{}{}
	if json.Unmarshal(def.Content, &graph) == nil && len(graph) > 0 {
		// 前端表单渲染/流程图依赖（issues/05）
		return map[string]interface{}{
			"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
			"type": def.Type, "state": def.State, "version": def.Version,
			"jsonObject": graph,
		}, nil
	}
	return map[string]interface{}{
		"id": def.ID, "name": def.Name, "displayName": def.DisplayName,
		"type": def.Type, "state": def.State, "version": def.Version,
	}, nil
}

// instancePage 我发起的流程实例分页（operator 过滤）
func (f *Facade) instancePage(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	operator := toStr(args["operator"], "user1")
	rows, total, err := f.repo.PageInstances(context.Background(), query, operator)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, instanceRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// instanceDetail 流程实例详情（含任务列表）
func (f *Facade) instanceDetail(args map[string]interface{}) (interface{}, error) {
	id, err := toInt64(args["id"])
	if err != nil {
		return nil, errors.New("id 缺失或非法")
	}
	inst, err := f.repo.FindInstanceByID(context.Background(), id)
	if err != nil || inst == nil {
		return nil, errors.New("流程实例不存在")
	}
	data := map[string]interface{}{
		"id": inst.ID, "parentId": inst.ParentID, "processDefineId": inst.DefineID,
		"state": inst.State, "parentNodeName": inst.ParentNodeName,
		"businessNo": inst.BusinessNo, "operator": inst.Operator,
		"variables": inst.Variables, "createTime": inst.CreateTime, "createUser": inst.CreateUser,
	}
	var graph map[string]interface{}
	if def0, _ := f.repo.FindDefineByID(context.Background(), inst.DefineID); def0 != nil {
		graph = map[string]interface{}{}
		if json.Unmarshal(def0.Content, &graph) == nil && len(graph) > 0 {
			data["jsonObject"] = graph // issues/05
		}
	}
	// 任务列表（issues/05-4）：全量 tasks + activeTaskList（仅 DOING）+ 任务行 ext/isFirstTaskNode
	firstTaskNodeID := firstTaskNodeIDOf(graph)
	tasks := make([]map[string]interface{}, 0, len(inst.Tasks))
	activeTaskList := make([]map[string]interface{}, 0)
	for _, t := range inst.Tasks {
		vo := f.taskVo(t)
		ext := map[string]interface{}{}
		for k, v := range t.Variables {
			ext[k] = v
		}
		doing := t.TaskState == model.TaskStateDoing
		ext["isFirstTaskNode"] = doing && t.TaskName == firstTaskNodeID
		vo["ext"] = ext
		tasks = append(tasks, vo)
		if doing {
			activeTaskList = append(activeTaskList, vo)
		}
	}
	data["tasks"] = tasks
	data["activeTaskList"] = activeTaskList
	return data, nil
}

// firstTaskNodeIDOf 流程 JSON 中第一个任务节点 id（issues/05-4 isFirstTaskNode 用）
func firstTaskNodeIDOf(graph map[string]interface{}) string {
	nodes, _ := graph["nodes"].([]interface{})
	for _, n := range nodes {
		node, _ := n.(map[string]interface{})
		if node != nil && node["type"] == "snaker:task" {
			id, _ := node["id"].(string)
			return id
		}
	}
	return ""
}

// todoList 我的待办分页（operator 作为抄送…待办人过滤，对齐 Java pta.actor_id EQ）
func (f *Facade) todoList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	actorID := toStr(args["operator"], "user1")
	rows, total, err := f.repo.PageTodoTasks(context.Background(), query, actorID)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, taskRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// doneList 我的已办分页（operator 过滤，非进行中任务）
func (f *Facade) doneList(args map[string]interface{}) (interface{}, error) {
	query := spi.PageQuery{PageNum: toIntDef(args["pageNum"], 1), PageSize: toIntDef(args["pageSize"], 10)}
	operator := toStr(args["operator"], "user1")
	rows, total, err := f.repo.PageDoneTasks(context.Background(), query, operator)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		out = append(out, taskRowToMap(r))
	}
	return pageData(query.PageNum, query.PageSize, total, out), nil
}

// taskVo 任务行 VO（instanceDetail 任务列表用，对齐 Java taskVo）
func (f *Facade) taskVo(t *model.ProcessTask) map[string]interface{} {
	return map[string]interface{}{
		"id": t.ID, "processInstanceId": t.ProcessInstanceID, "taskName": t.TaskName,
		"displayName": t.DisplayName, "taskType": t.TaskType, "performType": t.PerformType,
		"taskState": t.TaskState, "operator": t.ActorID, "finishTime": t.FinishTime,
		"expireTime": t.ExpireTime, "formKey": t.FormKey, "taskParentId": t.ParentTaskID,
		"variable": t.Variables, "createTime": t.CreateTime, "createUser": t.CreateUser,
		"updateTime": t.UpdateTime, "updateUser": t.UpdateUser, "taskActorIdList": t.ActorIDs,
	}
}

// ═══ 行输出转换（issues/05-2 字段契约 + 05-3 时间格式）═══

const timeFmt = "2006-01-02 15:04:05"

func fmtTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.Format(timeFmt)
}

func fmtTimeV(t time.Time) string {
	return t.Format(timeFmt)
}

// defineRowToMap 定义行：时间格式化
func defineRowToMap(r *model.DefineRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "name": r.Name, "displayName": r.DisplayName, "type": r.Type,
		"state": r.State, "version": r.Version,
		"createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
	}
}

// instanceRowToMap 实例行：ext（实例变量对象）+ displayName/version（定义）
func instanceRowToMap(r *model.InstanceRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "parentId": r.ParentID, "processDefineId": r.DefineID,
		"state": r.State, "parentNodeName": r.ParentNodeName, "businessNo": r.BusinessNo,
		"operator": r.Operator, "expireTime": fmtTime(r.ExpireTime),
		"variable": r.Variables, "createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": r.DefineName, "processDefineDisplayName": r.DefineDisplayName,
		"processDefineVersion": r.DefineVersion,
		"ext":                  r.Variables, "displayName": r.DefineDisplayName, "version": r.DefineVersion,
	}
}

// taskRowToMap 任务行：ext（任务变量，空回退实例变量）+ instanceExt + version
func taskRowToMap(r *model.TaskRow) map[string]interface{} {
	instanceExt := parseVarMap(r.InstanceVariable)
	ext := r.Variables
	if len(ext) == 0 {
		ext = instanceExt
	}
	return map[string]interface{}{
		"id": r.ID, "processInstanceId": r.ProcessInstanceID, "taskName": r.TaskName,
		"displayName": r.DisplayName, "taskType": r.TaskType, "performType": r.PerformType,
		"taskState": r.TaskState, "operator": r.Operator, "finishTime": fmtTime(r.FinishTime),
		"expireTime": fmtTime(r.ExpireTime), "formKey": r.FormKey, "taskParentId": r.TaskParentID,
		"variable": r.Variables, "createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": r.ProcessDefineName, "processDefineDisplayName": r.ProcessDefineDisplayName,
		"instanceVariable": r.InstanceVariable, "instanceCreateTime": fmtTimeV(r.InstanceCreateTime),
		"ext": ext, "instanceExt": instanceExt, "version": r.DefineVersion,
	}
}

// ccRowToMap 抄送行：ext（实例变量对象）+ displayName/version（定义）
func ccRowToMap(r *model.CcInstanceRow) map[string]interface{} {
	return map[string]interface{}{
		"id": r.ID, "parentId": r.ParentID, "processDefineId": r.DefineID,
		"state": r.State, "parentNodeName": r.ParentNodeName, "businessNo": r.BusinessNo,
		"operator": r.Operator, "expireTime": fmtTime(r.ExpireTime),
		"variable": r.Variables, "createTime": fmtTimeV(r.CreateTime), "createUser": r.CreateUser,
		"updateTime": fmtTimeV(r.UpdateTime), "updateUser": r.UpdateUser,
		"processDefineName": r.DefineName, "processDefineDisplayName": r.DefineDisplayName,
		"processDefineVersion": r.DefineVersion,
		"ext":                  r.Variables, "displayName": r.DefineDisplayName, "version": r.DefineVersion,
	}
}

// parseVarMap JSON 字符串 → map（坏 JSON 返回空 map）
func parseVarMap(s string) map[string]interface{} {
	m := map[string]interface{}{}
	if s == "" {
		return m
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}
