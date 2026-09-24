package demo

// T003：业务数据种子 driver——引擎真实启动（startAndExecute + execute），不直插 repo。
// 矩阵 = 八语言共用 canonical（day-shift 已在 Rust demo 实测全绿，照 rust seed_business.rs 移植）：
// 16 进行中(state=10) + 9 已完成(advance 推到 state=20) + 8 委托。
// 8 用户 × 5 菜单（待办/已办/发起/抄送/委托）全覆盖。rebuild()（启动 + /api/reset）尾部复跑。

import (
	"fmt"
	"log"
	"reflect"

	"github.com/mldong/jeeflow-go/facade"
)

type seedRow struct {
	defineID int64
	operator string
	extra    map[string]interface{}
	cc       []string
}

// 进行中 16 条：发起后停 state=10（I3/I15 冻结在决策/驳回前，I14 发起后再办两节点停 boss）
var seedInProgress = []seedRow{
	{1, "user1", nil, []string{"userA", "userB"}},
	{2, "user1", nil, nil},
	{3, "userA", map[string]interface{}{"amount": 500}, nil},
	{4, "manager", nil, []string{"userC", "leader"}},
	{5, "userB", nil, nil},
	{6, "director", nil, []string{"manager", "boss"}},
	{7, "userC", nil, []string{"user1"}},
	{1, "boss", nil, nil},
	{12, "user1", map[string]interface{}{"deptLeader": "manager"}, nil},
	{12, "userC", map[string]interface{}{"deptLeader": "director"}, nil},
	{12, "userB", map[string]interface{}{"deptLeader": "user1"}, nil},
	{15, "userA", nil, []string{"boss"}},
	{14, "leader", nil, []string{"director", "userC"}},
	{2, "userA", nil, nil}, // I14 发起后再办 leader/manager → 停 boss
	{10, "userB", nil, nil},
	{8, "user1", nil, nil},
}

// 已完成 9 条：advance 推到 state=20（分支无关）
var seedFinished = []seedRow{
	{1, "userA", nil, []string{"user1", "director"}},
	{8, "userB", nil, []string{"boss", "manager"}},
	{2, "manager", nil, []string{"boss"}},
	{10, "director", nil, nil},
	{12, "userC", map[string]interface{}{"deptLeader": "leader"}, nil},
	{1, "director", nil, nil},
	{5, "manager", nil, nil},
	{12, "userA", map[string]interface{}{"deptLeader": "director"}, nil},
	{12, "userB", map[string]interface{}{"deptLeader": "user1"}, nil},
}

// 委托 8 条：processSurrogate/page 无 operator 过滤 → 8 用户委托菜单全非空
var seedSurrogates = [][2]string{
	{"user1", "userA"}, {"userA", "userB"}, {"userB", "userC"}, {"userC", "leader"},
	{"leader", "manager"}, {"manager", "director"}, {"director", "boss"}, {"boss", "user1"},
}

// 申请信息表单：f_ 前缀 = 实例变量，前端「申请信息」区按 apply 节点 formKey 取 f_* 回显。
// 键 = defineId（13 = 11-assignment-handler 无 apply 节点，故表里没有 13）。
// ⚠️ 八语言 demo 同表同值同序（canonical 表），勿改措辞、勿重排。
// ⚠️ 日期一律写死字面量，不按当前时钟算：八栈机器时区/系统时间各异，算出来会漂。
// ⚠️ 字段名严禁 amount / finalAmount：它们是 03-decision-expr、10-mixed-mode 条件表达式里的
// 判定变量（下面 row.extra 的 amount 走的就是这条线），撞上会改变流程走向。
var seedFormByDefine = map[int64]map[string]interface{}{
	1:  {"f_reason": "家中有事需请假", "f_days": 3, "f_leaveType": "annual", "f_startDate": "2026-09-01", "f_endDate": "2026-09-03"},
	2:  {"f_reason": "项目上线后调休", "f_days": 2, "f_leaveType": "annual", "f_startDate": "2026-09-07", "f_endDate": "2026-09-08"},
	3:  {"f_reason": "出差报销申请", "f_days": 1, "f_leaveType": "personal", "f_startDate": "2026-09-10", "f_endDate": "2026-09-10"},
	4:  {"f_reason": "培训进修请假", "f_days": 5, "f_leaveType": "sick", "f_startDate": "2026-09-14", "f_endDate": "2026-09-18"},
	5:  {"f_reason": "年假出行", "f_days": 4, "f_leaveType": "annual", "f_startDate": "2026-09-21", "f_endDate": "2026-09-24"},
	6:  {"f_reason": "婚假申请", "f_days": 10, "f_leaveType": "personal", "f_startDate": "2026-09-28", "f_endDate": "2026-10-07"},
	7:  {"f_reason": "病假休养", "f_days": 6, "f_leaveType": "sick", "f_startDate": "2026-10-12", "f_endDate": "2026-10-17"},
	8:  {"f_reason": "产检假", "f_days": 3, "f_leaveType": "sick", "f_startDate": "2026-10-19", "f_endDate": "2026-10-21"},
	9:  {"f_reason": "陪产假", "f_days": 5, "f_leaveType": "personal", "f_startDate": "2026-10-26", "f_endDate": "2026-10-30"},
	10: {"f_reason": "事假处理家务", "f_days": 2, "f_leaveType": "personal", "f_startDate": "2026-11-02", "f_endDate": "2026-11-03"},
	11: {"f_bizType": "purchase", "f_budget": 12000, "f_urgency": "normal", "f_desc": "采购一批开发板与传感器"},
	12: {"f_reason": "部门例行调休", "f_days": 1, "f_leaveType": "annual", "f_startDate": "2026-11-09", "f_endDate": "2026-11-09"},
	14: {"f_reason": "外派学习请假", "f_days": 7, "f_leaveType": "annual", "f_startDate": "2026-11-16", "f_endDate": "2026-11-22"},
	15: {"f_reason": "丧假", "f_days": 3, "f_leaveType": "personal", "f_startDate": "2026-11-23", "f_endDate": "2026-11-25"},
}

// 办理表单：tf_ 前缀 = 任务变量，前端「办理表单」区读 taskFormData 回显。
// 键 = 审批节点的 formKey；表里没有的 formKey 只落通用审批意见，不臆造字段。
// 同样八栈同表同值同序——勿改勿重排。
var seedTfByForm = map[string]map[string]interface{}{
	"leave-form":       {"tf_approvedDays": 3, "tf_needExtra": "no", "tf_remark": "按项目排期核准，注意工作交接"},
	"review-form":      {"tf_riskLevel": "low", "tf_needLegalDoc": "no", "tf_reviewOpinion": "条款与预算均无风险"},
	"boss-form":        {"tf_finalDecision": "agree", "tf_finalAmount": 8000, "tf_bossNote": "同意，走年度预算"},
	"check-form":       {"tf_invoiceOk": "yes", "tf_amountChecked": 8000, "tf_checkNote": "票据齐全，计入差旅科目"},
	"countersign-form": {"tf_signVote": "support", "tf_signAmount": 5000, "tf_signOpinion": "本条线无异议"},
	"seq-form":         {"tf_seqStage": "first", "tf_seqVote": "pass", "tf_seqOpinion": "初审通过，转下一人"},
	"approve-form":     {"tf_approveResult": "ok", "tf_approveAmount": 8000, "tf_approveNote": "审批通过"},
	"ratio-form":       {"tf_ratioVote": "agree", "tf_ratioOpinion": "达到比例即可通过"},
	"veto-form":        {"tf_vetoResult": "pass", "tf_vetoReason": "无异议"},
	"form-a":           {"tf_branchA": "a1", "tf_branchANote": "A 分支选方案 A1"},
	"form-b":           {"tf_branchB": "b1", "tf_branchBNote": "B 分支选方案 B1"},
	"field-form":       {"tf_ownerName": "张三", "tf_field": "tech", "tf_fieldNote": "技术域评估通过"},
	"operator-form":    {"tf_selfCheck": "done", "tf_operatorNote": "发起人自查无误"},
	"dept-form":        {"tf_deptAgree": "yes", "tf_deptQuota": 8000, "tf_deptNote": "同意占用本部门额度"},
	"role-form":        {"tf_roleResult": "pass", "tf_roleNote": "角色审批通过"},
}

// seedWithTaskForm 办理表单落库：先给通用审批意见，再按该节点 formKey 覆盖专属字段。
// 抽成 helper 是因为两处 execute 调用点（seedAdvance 循环 / I14 特例）必须同口径，
// 否则八栈横评里同一节点会填出不一样的数据。
func seedWithTaskForm(ex map[string]interface{}, formKey interface{}) {
	ex["tf_approvalComment"] = "同意，情况已核实"
	for k, v := range seedTfByForm[fmt.Sprint(formKey)] {
		ex[k] = v
	}
}

// seedBusiness 种业务数据；失败逐条打日志不 panic（demo 启动不被单条卡死）。
func seedBusiness(f *facade.Facade) {
	okIn, okFin, okSurr := 0, 0, 0
	for _, row := range seedInProgress {
		args := map[string]interface{}{"processDefineId": row.defineID, "operator": row.operator}
		// 先铺申请信息 f_*，再铺 row.extra：已有的流程变量（amount / deptLeader）优先，不被表单值盖掉
		for k, v := range seedFormByDefine[row.defineID] {
			args[k] = v
		}
		for k, v := range row.extra {
			args[k] = v
		}
		resp := f.Flow("processDefine/startAndExecute", args)
		data, _ := resp["data"].(map[string]interface{})
		iid := seedToInt64(data["processInstanceId"])
		if iid == 0 {
			log.Printf("[seed] startAndExecute define=%d op=%s 失败: %v", row.defineID, row.operator, resp)
			continue
		}
		// I14：发起后再办 leader、manager 两节点 → 停在 boss
		if row.defineID == 2 && row.operator == "userA" {
			for _, actor := range []string{"leader", "manager"} {
				if t := seedTodoRow(f, actor, iid); t != nil {
					// todoList 行同样带 formKey（facade taskRowToMap），照 advance 同口径填办理表单
					ex := map[string]interface{}{
						"processTaskId": t["id"], "operator": actor, "submitType": 1,
					}
					seedWithTaskForm(ex, t["formKey"])
					f.Flow("processTask/execute", ex)
				} else {
					log.Printf("[seed] I14 todoRow actor=%s iid=%d 未找到", actor, iid)
				}
			}
		}
		if len(row.cc) > 0 {
			f.Flow("processInstance/createCCInstance", map[string]interface{}{
				"processInstanceId": iid, "operator": row.operator, "actorIds": row.cc,
			})
		}
		okIn++
	}

	for _, row := range seedFinished {
		args := map[string]interface{}{"processDefineId": row.defineID, "operator": row.operator}
		// 同 in-progress：f_* 先铺、extra 后铺（流程变量 amount / deptLeader 优先）
		for k, v := range seedFormByDefine[row.defineID] {
			args[k] = v
		}
		for k, v := range row.extra {
			args[k] = v
		}
		resp := f.Flow("processDefine/startAndExecute", args)
		data, _ := resp["data"].(map[string]interface{})
		iid := seedToInt64(data["processInstanceId"])
		if iid == 0 {
			log.Printf("[seed] FIN startAndExecute define=%d op=%s 失败: %v", row.defineID, row.operator, resp)
			continue
		}
		if state := seedAdvance(f, iid); state != 20 {
			log.Printf("[seed] FIN define=%d op=%s iid=%d 终态=%d（期望 20）", row.defineID, row.operator, iid, state)
		}
		if len(row.cc) > 0 {
			f.Flow("processInstance/createCCInstance", map[string]interface{}{
				"processInstanceId": iid, "operator": row.operator, "actorIds": row.cc,
			})
		}
		okFin++
	}

	for _, s := range seedSurrogates {
		resp := f.Flow("processSurrogate/save", map[string]interface{}{
			"operator": s[0], "surrogate": s[1], "processName": "",
			"startTime": "2026-01-01 00:00:00", "endTime": "2027-12-31 23:59:59",
		})
		if seedIsOk(resp) {
			okSurr++
		} else {
			log.Printf("[seed] surrogate %s->%s 失败: %v", s[0], s[1], resp)
		}
	}
	log.Printf("[seedBusiness] done: in-progress %d/16, finished %d/9, surrogates %d/8", okIn, okFin, okSurr)
}

// seedAdvance 原语：循环读 detail，对每个 doing 任务以其自身 actor execute(submitType=1)。
// doing 任务 operator 为空串/null，actor 取 taskActorIdList[0]。
func seedAdvance(f *facade.Facade, iid int64) int64 {
	for i := 0; i < 30; i++ {
		resp := f.Flow("processInstance/detail", map[string]interface{}{"id": iid})
		data, _ := resp["data"].(map[string]interface{})
		if data == nil {
			return -1
		}
		state := seedToInt64(data["state"])
		if state != 10 {
			return state
		}
		tasks, _ := data["tasks"].([]interface{})
		progress := false
		for _, tObj := range tasks {
			t, _ := tObj.(map[string]interface{})
			if t == nil || seedToInt64(t["taskState"]) != 10 {
				continue
			}
			actor := seedToStr(t["operator"])
			if actor == "" {
				if actors, _ := t["taskActorIdList"].([]interface{}); len(actors) > 0 {
					actor = seedToStr(actors[0])
				}
			}
			if actor == "" {
				continue
			}
			// detail 任务行带 formKey（facade），统一走 helper 填办理表单
			ex := map[string]interface{}{
				"processTaskId": t["id"], "operator": actor, "submitType": 1,
			}
			seedWithTaskForm(ex, t["formKey"])
			r := f.Flow("processTask/execute", ex)
			if seedIsOk(r) {
				progress = true
			} else {
				log.Printf("[seed] advance execute iid=%d actor=%s 失败: %v", iid, actor, r)
			}
		}
		if !progress {
			return state
		}
	}
	return -1
}

// seedTodoRow 仅 I14 用：在该实例里找 op 的 doing 任务行。
func seedTodoRow(f *facade.Facade, op string, iid int64) map[string]interface{} {
	resp := f.Flow("processTask/todoList", map[string]interface{}{
		"operator": op, "pageNum": 1, "pageSize": 200,
	})
	data, _ := resp["data"].(map[string]interface{})
	rows, _ := data["rows"].([]interface{})
	for _, rObj := range rows {
		r, _ := rObj.(map[string]interface{})
		if r == nil {
			continue
		}
		if seedToInt64(r["processInstanceId"]) == iid && seedToInt64(r["taskState"]) == 10 {
			return r
		}
	}
	return nil
}

func seedIsOk(resp map[string]interface{}) bool {
	return seedToInt64(resp["code"]) == 0
}

// seedToInt64 兼容 int64/int/float64/string 及具名 int 类型
// （进程内直调无 JSON 序列化：state/taskState 是 model.InstanceState/TaskState 具名类型）
func seedToInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		var n int64
		_, err := fmt.Sscan(x, &n)
		if err != nil {
			return 0
		}
		return n
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(rv.Uint())
		case reflect.Float32, reflect.Float64:
			return int64(rv.Float())
		default:
			return 0
		}
	}
}

func seedToStr(v interface{}) string {
	if s, _ := v.(string); s != "" && s != "<nil>" {
		return s
	}
	return ""
}
