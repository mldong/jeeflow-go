// 「已办撤回（issues/246）＋动态会签（issues/247）」的 **SQL 侧取证**（go 腿）——照
// `add_sign_sqlite_test.go` 同一条判据：账本键（tf_recallHistory / tf_addSignHistory）与
// 轮次戳必须值在 INSERT/UPDATE 那一刻序列化进 `variable` 列、之后**查得到才算做到**；
// 内存侧"同一 map 引用"证不出这一层。跑临时文件 sqlite，不连 MySQL/PG、不碰 160、不起服务：
//
//	go test ./repository/jdbc/ -run 'TestRecallCollectSql'
package jdbc_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// 本案专用 id 段（9246xxxx/9247xxxx，避开 9190xxxx(加签) 与 9142xxxx(142) 与 T1 的 900001）
const (
	rcSqlDefineID = int64(92460001)
	dcSqlDefineID = int64(92470001)
	rcSqlFlowJSON = `{"name":"recall_sql","displayName":"已办撤回 SQL 取证","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","taskType":0,"performType":0},"text":{"value":"发起申请"}},` +
		`{"id":"review","type":"snaker:task","properties":{"assignee":"userA","taskType":0,"performType":0,"allowRecall":true},"text":{"value":"审批"}},` +
		`{"id":"final","type":"snaker:task","properties":{"assignee":"userF","taskType":0,"performType":0},"text":{"value":"终审"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}` +
		`],"edges":[` +
		`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e2","sourceNodeId":"apply","targetNodeId":"review","properties":{}},` +
		`{"id":"e3","sourceNodeId":"review","targetNodeId":"final","properties":{}},` +
		`{"id":"e4","sourceNodeId":"final","targetNodeId":"end","properties":{}}]}`
	dcSqlFlowJSON = `{"name":"collect_sql","displayName":"动态会签 SQL 取证","type":"approval","nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","taskType":0,"performType":0},"text":{"value":"发起申请"}},` +
		`{"id":"review","type":"snaker:task","properties":{"assignee":"userA","taskType":0,"performType":0,"dynamicCountersign":true},"text":{"value":"审批"}},` +
		`{"id":"final","type":"snaker:task","properties":{"assignee":"userF","taskType":0,"performType":0},"text":{"value":"终审"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}` +
		`],"edges":[` +
		`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
		`{"id":"e2","sourceNodeId":"apply","targetNodeId":"review","properties":{}},` +
		`{"id":"e3","sourceNodeId":"review","targetNodeId":"final","properties":{}},` +
		`{"id":"e4","sourceNodeId":"final","targetNodeId":"end","properties":{}}]}`
)

// rcSqlPutDefine 放/清一份定义（幂等，开跑先删；INSERT 列形状照 addSignPutDefine）。
func rcSqlPutDefine(t *testing.T, db *sql.DB, id int64, content []byte) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DELETE FROM wf_process_define WHERE id=?`, id); err != nil {
		t.Fatalf("清定义: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO wf_process_define (id,name,display_name,type,state,content,version) VALUES (?,?,?, 'approval',1,?,1)`,
		id, rcNameOf(t, content), "取证", content); err != nil {
		t.Fatalf("放定义: %v", err)
	}
}

func rcNameOf(t *testing.T, content []byte) string {
	t.Helper()
	var graph map[string]interface{}
	if err := json.Unmarshal(content, &graph); err != nil {
		t.Fatalf("解析定义 name: %v", err)
	}
	name, _ := graph["name"].(string)
	return name
}

// rcSqlEngine 一份新引擎挂在 sqlite 仓上（与 addSignEngine 同形）。
func rcSqlEngine(db *sql.DB) *engine.EngineImpl {
	return engine.New(jdbc.New(db), &noopUserProvider{},
		&tsIDGen{base: time.Now().UnixMilli()*4000 + testStackSeq*1000}, nil)
}

// rcSqlStartToReview 发起并自动办结申请节点，返回实例 id。
func rcSqlStartToReview(t *testing.T, db *sql.DB, eng *engine.EngineImpl, defID int64) int64 {
	t.Helper()
	repo := jdbc.New(db)
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "applicant", map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起: %v", err)
	}
	rows, err := repo.FindDoingTasks(context.Background(), inst.ID, nil)
	if err != nil {
		t.Fatalf("读 DOING: %v", err)
	}
	for _, tk := range rows {
		if _, err := eng.ExecuteProcessTask(context.Background(), tk.ID, "applicant",
			map[string]interface{}{"submitType": int(model.SubmitTypeApply)}); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}
	return inst.ID
}

func rcSqlExecAgree(t *testing.T, eng *engine.EngineImpl, taskID int64, operator string, extra map[string]interface{}) {
	t.Helper()
	args := map[string]interface{}{"submitType": int(model.SubmitTypeAgree)}
	for k, v := range extra {
		args[k] = v
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), taskID, operator, args); err != nil {
		t.Fatalf("办理 %d: %v", taskID, err)
	}
}

// rcSqlLedger 直接查 variable 列读回账本（不经仓储结构体映射，键名有没有被写歪在 SQL 侧看得见）。
func rcSqlLedger(t *testing.T, db *sql.DB, taskID int64, key string) []map[string]interface{} {
	t.Helper()
	vars := addSignVars(t, db, "wf_process_task", taskID)
	return addSignHistoryFromVars(t, vars[key])
}

// addSignHistoryFromVars 账本两形都收（内存 []map、JSON 往返 []interface{}）。
func addSignHistoryFromVars(t *testing.T, v interface{}) []map[string]interface{} {
	t.Helper()
	switch list := v.(type) {
	case []map[string]interface{}:
		return list
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(list))
		for _, item := range list {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// rcSqlMemberIDs 直查成员行 id（task_parent_id＝主办行＝成员行的身份锚）。
func rcSqlMemberIDs(t *testing.T, db *sql.DB, instID, hostID int64) []int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id FROM wf_process_task WHERE process_instance_id=? AND task_parent_id=? ORDER BY id`,
		instID, hostID)
	if err != nil {
		t.Fatalf("读成员行: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan 成员行: %v", err)
		}
		out = append(out, id)
	}
	return out
}

// TestRecallSqlLedgerRoundTrip 撤回三件套在 SQL 列上的取证：原行 tf_recallHistory
// （recalledTaskId 指向复活行）经 INSERT/UPDATE 序列化后查得到；复活行参与者照原行名册；
// 复活行可正常重办推进。
func TestRecallCollectSqlRecallLedgerRoundTrip(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	repo := jdbc.New(db)
	eng := rcSqlEngine(db)
	rcSqlPutDefine(t, db, rcSqlDefineID, []byte(rcSqlFlowJSON))
	iid := rcSqlStartToReview(t, db, eng, rcSqlDefineID)

	review, err := repo.FindDoingTasks(context.Background(), iid, []string{"review"})
	if err != nil || len(review) != 1 {
		t.Fatalf("前置：review 应有 1 条待办，实得 %v/%v", review, err)
	}
	rcSqlExecAgree(t, eng, review[0].ID, "userA", nil)
	oldReview, err := repo.FindTaskByID(context.Background(), review[0].ID)
	if err != nil || oldReview == nil || !oldReview.IsFinished() {
		t.Fatal("前置：review 应办结")
	}

	resurrected, err := eng.RecallTask(context.Background(), oldReview.ID, "userA")
	if err != nil {
		t.Fatalf("撤回: %v", err)
	}
	// 原行账本从 SQL 列读回（不经内存引用）
	history := rcSqlLedger(t, db, oldReview.ID, engine.KeyRecallHistory)
	if len(history) != 1 {
		t.Fatalf("原行应有 1 条 tf_recallHistory（SQL 列读回），实得 %d", len(history))
	}
	// int → JSON number → float64 这条形状变化是 SQL 仓独有的坑（add_sign_sqlite_test 同注）：
	// 两形都收，按数值比。
	var recalledID int64
	switch v := history[0]["recalledTaskId"].(type) {
	case float64:
		recalledID = int64(v)
	case int64:
		recalledID = v
	}
	if recalledID != resurrected.ID {
		t.Errorf("recalledTaskId 应指向复活行 %d，实得 %#v", resurrected.ID, history[0]["recalledTaskId"])
	}
	if history[0]["operator"] != "userA" {
		t.Errorf("账本 operator 应为 userA，实得 %v", history[0]["operator"])
	}
	// 复活行参与者照原行名册（参与者表）
	actors, err := repo.FindTaskActors(context.Background(), resurrected.ID)
	if err != nil || len(actors) != 1 || actors[0] != "userA" {
		t.Errorf("复活行参与者应照原行名册 [userA]，实得 %v/%v", actors, err)
	}
	// 复活行可正常重办推进
	rcSqlExecAgree(t, eng, resurrected.ID, "userA", nil)
	final, err := repo.FindDoingTasks(context.Background(), iid, []string{"final"})
	if err != nil || len(final) != 1 {
		t.Errorf("重办后应推进到 final，实得 %v/%v", final, err)
	}
}

// TestRecallCollectSqlCollectLedgerRoundTrip 动态会签在 SQL 列上的取证：主办行 tf_addSignHistory
// ＋轮次戳、成员行轮次/reason 两戳经 SQL 往返仍在；门面撤回后账本两条（发起＋撤回）、
// 未答成员行废弃、已交行保留。
func TestRecallCollectSqlCollectLedgerRoundTrip(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	repo := jdbc.New(db)
	eng := rcSqlEngine(db)
	rcSqlPutDefine(t, db, dcSqlDefineID, []byte(dcSqlFlowJSON))
	iid := rcSqlStartToReview(t, db, eng, dcSqlDefineID)

	fd := facade.New(eng, repo, nil)
	hostRows, err := repo.FindDoingTasks(context.Background(), iid, []string{"review"})
	if err != nil || len(hostRows) != 1 {
		t.Fatalf("前置：review 应有 1 条待办，实得 %v/%v", hostRows, err)
	}
	hostID := hostRows[0].ID
	resp := fd.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", hostID), "operator": "userA",
		"submitType":                int(model.SubmitTypeAgree),
		engine.KeyAddSignOperators:  []interface{}{"userX", "userY"},
		engine.KeyAddSignReason:     "涉及两部门意见",
	})
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("发起征询: %v", resp)
	}
	// 成员行轮次/reason 从 SQL 列读回
	members := rcSqlMemberIDs(t, db, iid, hostID)
	if len(members) != 2 {
		t.Fatalf("应建两条成员行，实得 %d", len(members))
	}
	for _, m := range members {
		vars := addSignVars(t, db, "wf_process_task", m)
		if fmt.Sprint(vars["addSignRound_review"]) != "1" {
			t.Errorf("成员行轮次戳应经 SQL 往返为 1，实得 %v", vars["addSignRound_review"])
		}
		if vars[engine.KeyAddSignReason] != "涉及两部门意见" {
			t.Errorf("成员行 reason 应经 SQL 往返，实得 %v", vars[engine.KeyAddSignReason])
		}
	}
	// 主办行轮次戳＋账本一条
	hostVars := addSignVars(t, db, "wf_process_task", hostID)
	if fmt.Sprint(hostVars["addSignRound_review"]) != "1" {
		t.Errorf("主办行轮次戳应为 1，实得 %v", hostVars["addSignRound_review"])
	}
	if got := len(addSignHistoryFromVars(t, hostVars[engine.KeyAddSignHistory])); got != 1 {
		t.Fatalf("主办行账本应 1 条，实得 %d", got)
	}
	// userX 交意见 → userX 行 FINISHED；撤回 → 未答 userY 废弃、账本两条
	memberRows := addSignDoing(t, db, iid, "review")
	var xID int64
	for _, m := range memberRows {
		if m.Actor == "userX" {
			xID = m.ID
		}
	}
	if xID == 0 {
		t.Fatal("前置：应能按参与者找到 userX 的成员行")
	}
	rcSqlExecAgree(t, eng, xID, "userX", nil)
	rv := fd.Flow("processTask/revokeDynamicCountersign", map[string]interface{}{
		"processTaskId": fmt.Sprintf("%d", hostID), "operator": "userA", "reason": "选错部门",
	})
	if code, _ := rv["code"].(int); code != 0 {
		t.Fatalf("撤回: %v", rv)
	}
	hostVars = addSignVars(t, db, "wf_process_task", hostID)
	history := addSignHistoryFromVars(t, hostVars[engine.KeyAddSignHistory])
	if len(history) != 2 {
		t.Fatalf("主办行账本应 2 条（发起＋撤回，SQL 列读回），实得 %d", len(history))
	}
	if history[1]["revoked"] != true {
		t.Errorf("撤回条应带 revoked=true，实得 %v", history[1]["revoked"])
	}
	if history[1]["reason"] != "选错部门" {
		t.Errorf("撤回条应带 reason，实得 %v", history[1]["reason"])
	}
	// 已交保留（FINISHED）、未答废弃（99）
	xRow, err := repo.FindTaskByID(context.Background(), xID)
	if err != nil || xRow == nil || !xRow.IsFinished() {
		t.Errorf("已交意见行应保留 FINISHED，实得 %v/%v", xRow, err)
	}
	yID := members[0]
	if yID == xID {
		yID = members[1]
	}
	yRow, err := repo.FindTaskByID(context.Background(), yID)
	if err != nil || yRow == nil || yRow.TaskState != model.TaskStateAbandoned {
		t.Errorf("未答成员行应废弃（99），实得 %v/%v", yRow, err)
	}
}
