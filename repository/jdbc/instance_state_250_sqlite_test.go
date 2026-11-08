// 「列表行带所属流程实例状态（issues/250）」的 **SQL 侧取证**（go 腿）。
// 内存仓 taskRow 也设了该字段 ⇒ 只打内存腿照不出 SQL 漏投那一列（issues/249 同形教训：
// java 侧摘掉 SQL 投影读之后内存仓 66 格全绿、只有真库腿报红）。跑临时文件 sqlite，
// 不连 160、不起服务：
//
//	go test ./repository/jdbc/ -run 'TestInstanceState250Sql'
package jdbc_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

// is250DefineID 本案专用 id 段（避开 9190xxxx 加签 / 9142xxxx / 9246-9247 / T1 的 900001）。
const is250DefineID = int64(92500101)

// is250FlowJSON start → apply(applicant) → task1(leader) → end（线性两支，末节点即 task1）。
const is250FlowJSON = `{"name":"is250","displayName":"实例态投影取证","type":"approval","nodes":[` +
	`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
	`{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","taskType":0,"performType":0},"text":{"value":"发起申请"}},` +
	`{"id":"task1","type":"snaker:task","properties":{"assignee":"leader","taskType":0,"performType":0},"text":{"value":"审批"}},` +
	`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],"edges":[` +
	`{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},` +
	`{"id":"e2","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},` +
	`{"id":"e3","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`

func TestInstanceState250Sql(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	eng := rcSqlEngine(db)
	rcSqlPutDefine(t, db, is250DefineID, []byte(is250FlowJSON))
	iid := rcSqlStartToReview(t, db, eng, is250DefineID)

	repo := jdbc.New(db)
	ctx := context.Background()
	q := spi.PageQuery{PageNum: 1, PageSize: 20}

	// ① 实例进行中：待办行 instanceState 必须是 10（pi.state 真被 SELECT 出来并映射进了行）
	todo, _, err := repo.PageTodoTasks(ctx, q, "leader")
	if err != nil {
		t.Fatalf("待办分页: %v", err)
	}
	var todoRow *model.TaskRow
	for _, r := range todo {
		if r.ProcessInstanceID == iid {
			todoRow = r
		}
	}
	if todoRow == nil {
		t.Fatalf("前置：leader 待办里应有本单，否则下面的断言恒真空（共 %d 行）", len(todo))
	}
	if todoRow.InstanceState == nil || *todoRow.InstanceState != int(model.InstanceStateDoing) {
		t.Fatalf("实例进行中 ⇒ 待办行 instanceState 应为 10，实得 %v（nil 即 SQL 漏投该列）",
			todoRow.InstanceState)
	}

	// 办结末节点 ⇒ 实例结束
	doing, err := repo.FindDoingTasks(ctx, iid, nil)
	if err != nil {
		t.Fatalf("读 DOING: %v", err)
	}
	lastTaskID := doing[0].ID
	rcSqlExecAgree(t, eng, lastTaskID, "leader", nil)

	// ② 实例已结束：任务态仍是 20，但 instanceState 必须 20 —— 这一格前端拿它置灰撤回按钮。
	done, _, err := repo.PageDoneTasks(ctx, q, "leader")
	if err != nil {
		t.Fatalf("已办分页: %v", err)
	}
	var doneRow *model.TaskRow
	for _, r := range done {
		if r.ID == lastTaskID {
			doneRow = r
		}
	}
	if doneRow == nil {
		t.Fatalf("前置：leader 已办里应有本单末节点行（共 %d 行）", len(done))
	}
	if doneRow.TaskState != model.TaskStateDone {
		t.Fatalf("前置：任务行自身应仍是已完成(20)，实得 %v", doneRow.TaskState)
	}
	if doneRow.InstanceState == nil || *doneRow.InstanceState != int(model.InstanceStateDone) {
		t.Fatalf("实例已结束 ⇒ 已办行 instanceState 应为 20，实得 %v（任务态照不出这一格）",
			doneRow.InstanceState)
	}
}
