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

// seedBusiness 种业务数据；失败逐条打日志不 panic（demo 启动不被单条卡死）。
func seedBusiness(f *facade.Facade) {
	okIn, okFin, okSurr := 0, 0, 0
	for _, row := range seedInProgress {
		args := map[string]interface{}{"processDefineId": row.defineID, "operator": row.operator}
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
					f.Flow("processTask/execute", map[string]interface{}{
						"processTaskId": t["id"], "operator": actor, "submitType": 1,
					})
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
			r := f.Flow("processTask/execute", map[string]interface{}{
				"processTaskId": t["id"], "operator": actor, "submitType": 1,
			})
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
