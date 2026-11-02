// 「后加签（可重复）」的 **SQL 侧取证**（go 腿，移植 jeeflow-java
// `jeeflow-repository-jdbc/.../AddSignAfterJdbcTest` 两格；java 那边跑嵌入式 H2，
// 本栈同档跑临时文件 sqlite——同一条判据：值必须在 INSERT/UPDATE 那一刻序列化进 `variable`
// 列、之后**查得到才算做到**，内存仓共用同一个 map 引用是证不出这一层的）。
//
// 两格各自钉住什么：
//  1. 轮次键 `addSignRound_{节点}` 与账本 `tf_addSignHistory` 经 SQL 列往返仍在（int → JSON number
//     → float64 这条形状变化是本栈独有的坑，内存侧永远绿），并且**换新引擎实例（等价服务重启）**
//     后仍能继续加签、轮次号不丢（spec 04 语义边界表「重启」行）。
//  2. 同圈两个加签任务并发办结，真库上后续节点只生成一行（放行只发生一次）。
//
// 跑法（不连 MySQL/PG、不碰 160、不起服务）：
//
//	go test ./repository/jdbc/ -run 'TestAddSign'
package jdbc_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// addSignDefineID 本案专用 id 段（9190xxxx，避开 142 的 9142xxxx 与 T1 真库用例的 900001）
const addSignDefineID = int64(91900001)

// addSignFlowJSON start → apply → review(开加签) → final → end，与 engine/add_sign_test.go 同形。
const addSignFlowJSON = `{"name":"addsign_sql","displayName":"后加签 SQL 取证","type":"approval","nodes":[` +
	`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
	`{"id":"apply","type":"snaker:task","properties":{"form":"apply-form","assignee":"applicant","taskType":0,"performType":0},"text":{"value":"发起申请"}},` +
	`{"id":"review","type":"snaker:task","properties":{"form":"form-r","assignee":"userA","taskType":0,"performType":0,"allowAddSign":true,"addSignMaxRounds":0},"text":{"value":"审批"}},` +
	`{"id":"final","type":"snaker:task","properties":{"form":"form-f","assignee":"userF","taskType":0,"performType":0},"text":{"value":"终审"}},` +
	`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}` +
	`],"edges":[` +
	`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
	`{"id":"e2","sourceNodeId":"apply","targetNodeId":"review","properties":{}},` +
	`{"id":"e3","sourceNodeId":"review","targetNodeId":"final","properties":{}},` +
	`{"id":"e4","sourceNodeId":"final","targetNodeId":"end","properties":{}}]}`

// addSignEngine 一份**新**引擎挂在同一个 SQL 仓上——换新实例即"等价服务重启"：
// 链上的轮次与账本只能靠仓储状态续下去，内存里留任何东西都不算。
func addSignEngine(db *sql.DB) *engine.EngineImpl {
	return engine.New(jdbc.New(db), &noopUserProvider{},
		&tsIDGen{base: time.Now().UnixMilli()*4000 + testStackSeq*1000}, nil)
}

func addSignPutDefine(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DELETE FROM wf_process_define WHERE id=?`, addSignDefineID); err != nil {
		t.Fatalf("清定义: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO wf_process_define (id,name,display_name,type,state,content,version) VALUES (?,?,?, 'approval',1,?,1)`,
		addSignDefineID, "addsign_sql", "后加签 SQL 取证", []byte(addSignFlowJSON)); err != nil {
		t.Fatalf("放定义: %v", err)
	}
	return addSignDefineID
}

// addSignVarColumn 直接查 `wf_process_variable` 表读回实例/任务那一份变量 JSON（列名由 caller 定），
// 解成 map 返回——**不经仓储的结构体映射**，这样"键名有没有被写歪"在 SQL 侧也看得见。
func addSignVars(t *testing.T, db *sql.DB, table string, id int64) map[string]interface{} {
	t.Helper()
	var raw sql.NullString
	q := fmt.Sprintf("SELECT variable FROM %s WHERE id=?", table)
	if err := db.QueryRowContext(context.Background(), q, id).Scan(&raw); err != nil {
		t.Fatalf("读 %s.variable (id=%d): %v", table, id, err)
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return map[string]interface{}{}
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal([]byte(raw.String), &out); err != nil {
		t.Fatalf("解析 %s.variable: %v", table, err)
	}
	return out
}

// addSignDoing 按实例＋节点读 DOING 行（id 升序）
// addSignDoingRow 一条 DOING 行与其第一个参与者（真库读回，不看引擎返回值）
type addSignDoingRow struct {
	ID    int64
	Actor string
}

func addSignDoing(t *testing.T, db *sql.DB, instID int64, node string) []addSignDoingRow {
	t.Helper()
	q := `SELECT t.id, IFNULL(MIN(a.actor_id),'') AS actor FROM wf_process_task t
	      LEFT JOIN wf_process_task_actor a ON a.process_task_id = t.id
	      WHERE t.process_instance_id=? AND t.task_state=10 AND t.task_name=? GROUP BY t.id ORDER BY t.id`
	rows, err := db.QueryContext(context.Background(), q, instID, node)
	if err != nil {
		t.Fatalf("读 DOING 行: %v", err)
	}
	defer rows.Close()
	var out []addSignDoingRow
	for rows.Next() {
		var r addSignDoingRow
		if err := rows.Scan(&r.ID, &r.Actor); err != nil {
			t.Fatalf("scan DOING 行: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历 DOING 行: %v", err)
	}
	return out
}

func addSignCount(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("计数 %q: %v", q, err)
	}
	return n
}

func addSignExecAgree(t *testing.T, eng *engine.EngineImpl, taskID int64, operator string, ops interface{}) {
	t.Helper()
	args := map[string]interface{}{"submitType": int(model.SubmitTypeAgree)}
	if ops != nil {
		args[engine.KeyAddSignOperators] = ops
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), taskID, operator, args); err != nil {
		t.Fatalf("办理任务 %d: %v", taskID, err)
	}
}

func addSignStartToReview(t *testing.T, db *sql.DB, eng *engine.EngineImpl, defID int64) int64 {
	t.Helper()
	ctx := context.Background()
	inst, err := eng.StartProcessInstanceByID(ctx, defID, "applicant", map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起流程: %v", err)
	}
	for _, r := range addSignDoing(t, db, inst.ID, "apply") {
		if _, err := eng.ExecuteProcessTask(ctx, r.ID, "applicant",
			map[string]interface{}{"submitType": int(model.SubmitTypeApply)}); err != nil {
			t.Fatalf("办结申请节点: %v", err)
		}
	}
	return inst.ID
}

// 格 1：轮次与账本经 SQL 列往返仍在；重启（换新引擎）后续办与放行都正确。
func TestAddSignRoundAndHistorySurviveSqlRoundTrip(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	defID := addSignPutDefine(t, db)
	eng := addSignEngine(db)
	iid := addSignStartToReview(t, db, eng, defID)

	review := addSignDoing(t, db, iid, "review")
	if len(review) != 1 {
		t.Fatalf("审批节点应有一条待办，实得 %d", len(review))
	}
	aID := review[0].ID
	addSignExecAgree(t, eng, aID, "userA", "userB")

	// 从库里重读：轮次键与账本都必须是"进了列再读回来"的形状
	bRows := addSignDoing(t, db, iid, "review")
	if len(bRows) != 1 {
		t.Fatalf("加签后应有一条新待办，实得 %d", len(bRows))
	}
	bVars := addSignVars(t, db, "wf_process_task", bRows[0].ID)
	if got := fmt.Sprint(bVars["addSignRound_review"]); got != "1" {
		t.Errorf("轮次号应经 SQL 往返读成 1，实得 %q（类型 %T）", got, bVars["addSignRound_review"])
	}
	aVars := addSignVars(t, db, "wf_process_task", aID)
	hist, _ := aVars[engine.KeyAddSignHistory].([]interface{})
	if len(hist) != 1 {
		t.Fatalf("已完成行的账本应读回一条，实得 %#v", aVars[engine.KeyAddSignHistory])
	}
	e0, _ := hist[0].(map[string]interface{})
	if e0["fromActor"] != "userA" {
		t.Errorf("账本 fromActor 应为 userA，实得 %#v", e0["fromActor"])
	}
	if !strings.Contains(fmt.Sprint(e0["toActors"]), "userB") {
		t.Errorf("账本 toActors 应含 userB，实得 %#v", e0["toActors"])
	}
	ts, _ := e0["time"].(string)
	if len(ts) != len("2026-01-02 15:04:05") || strings.Contains(ts, ".") {
		t.Errorf("账本 time 须是全生态格式串（无毫秒），实得 %q", ts)
	}

	// 重建引擎（等价服务重启）：仅凭仓储状态继续这条链
	eng2 := addSignEngine(db)
	addSignExecAgree(t, eng2, bRows[0].ID, bRows[0].Actor, "userC")
	cRows := addSignDoing(t, db, iid, "review")
	if len(cRows) != 1 {
		t.Fatalf("第二轮应有一条待办，实得 %d", len(cRows))
	}
	cVars := addSignVars(t, db, "wf_process_task", cRows[0].ID)
	if got := fmt.Sprint(cVars["addSignRound_review"]); got != "2" {
		t.Errorf("第二轮轮次应为 2，实得 %q", got)
	}
	var parent int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT task_parent_id FROM wf_process_task WHERE id=?`, cRows[0].ID).Scan(&parent); err != nil {
		t.Fatalf("读血缘: %v", err)
	}
	if parent != bRows[0].ID {
		t.Errorf("链上血缘应串到上一轮 %d，实得 %d", bRows[0].ID, parent)
	}

	// 再一次重启：不点人 → 流出本节点
	eng3 := addSignEngine(db)
	addSignExecAgree(t, eng3, cRows[0].ID, cRows[0].Actor, nil)
	if n := addSignCount(t, db, `SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id=? AND task_name='review'`, iid); n != 3 {
		t.Errorf("review 共三行，实得 %d", n)
	}
	fin := addSignDoing(t, db, iid, "final")
	if len(fin) != 1 {
		t.Fatalf("不点人后应流出本节点并建终审待办，实得 %d", len(fin))
	}
	addSignExecAgree(t, eng3, fin[0].ID, "userF", nil)
	if got := addSignCount(t, db, `SELECT state FROM wf_process_instance WHERE id=?`, iid); got != int(model.InstanceStateDone) {
		t.Errorf("实例应办结（20），实得 %d", got)
	}
}

// 格 2：真库上同圈两个加签任务并发办结 ⇒ 后续节点只生成一行。
func TestAddSignConcurrentSiblingsReleaseOnce(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	defID := addSignPutDefine(t, db)
	eng := addSignEngine(db)
	iid := addSignStartToReview(t, db, eng, defID)

	review := addSignDoing(t, db, iid, "review")
	addSignExecAgree(t, eng, review[0].ID, "userA", []interface{}{"userB", "userC"})
	sibs := addSignDoing(t, db, iid, "review")
	if len(sibs) != 2 {
		t.Fatalf("同圈应建两行待办，实得 %d", len(sibs))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, s := range sibs {
		wg.Add(1)
		go func(taskID int64, actor string) {
			defer wg.Done()
			args := map[string]interface{}{"submitType": int(model.SubmitTypeAgree)}
			if _, err := eng.ExecuteProcessTask(context.Background(), taskID, actor, args); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(s.ID, s.Actor)
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("并发办结不应报错: %v", errs)
	}
	if n := addSignCount(t, db,
		`SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id=? AND task_name='final'`, iid); n != 1 {
		t.Errorf("后续节点只应生成一行，实得 %d", n)
	}
	if n := len(addSignDoing(t, db, iid, "review")); n != 0 {
		t.Errorf("本节点不得留未完成加签待办，实得 %d", n)
	}
	if n := addSignCount(t, db,
		`SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id=? AND task_state=10`, iid); n != 1 {
		t.Errorf("全实例只应剩终审那一条待办，实得 %d", n)
	}
}
