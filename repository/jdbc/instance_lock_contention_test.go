// 实例级排他保护（spec 04 · 并发保护）的真库取证（T1，默认 160 MySQL）：
//  1. 另一事务持实例行锁时，引擎办理必须被阻塞（证明 FOR UPDATE 真生效、锁覆盖事务边界）；
//  2. 释放后办理成功，且同一任务第二次办理被拒（最终幂等凭证＝任务行状态条件）。
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

const lockDefineID = int64(900778)

func TestInstanceLockBlocksAndSerializes(t *testing.T) {
	db, err := sql.Open(testDriver(), testDSN())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := jdbc.New(db)
	eng := engine.New(repo, asLockUserProv{}, &lockIDGen{}, nil)
	ctx := context.Background()

	// 清理旧数据（只碰本用例固定 define 段）
	cleanup := func() {
		var ids []int64
		rows, _ := db.Query("SELECT id FROM wf_process_instance WHERE process_define_id = ?", lockDefineID)
		if rows != nil {
			for rows.Next() {
				var id int64
				_ = rows.Scan(&id)
				ids = append(ids, id)
			}
			rows.Close()
		}
		for _, id := range ids {
			_, _ = db.Exec("DELETE FROM wf_process_task_actor WHERE process_task_id IN (SELECT id FROM wf_process_task WHERE process_instance_id = ?)", id)
			_, _ = db.Exec("DELETE FROM wf_process_task WHERE process_instance_id = ?", id)
			_, _ = db.Exec("DELETE FROM wf_process_cc_instance WHERE process_instance_id = ?", id)
			_, _ = db.Exec("DELETE FROM wf_process_instance WHERE id = ?", id)
		}
		_, _ = db.Exec("DELETE FROM wf_process_define WHERE id = ?", lockDefineID)
	}
	cleanup()
	t.Cleanup(cleanup)

	content := `{"name":"lock_case","displayName":"实例锁取证","type":"approval","nodes":[
		{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
		{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","form":"f","taskType":0,"performType":0},"text":{"value":"申请"}},
		{"id":"review","type":"snaker:task","properties":{"assignee":"userA","form":"f","taskType":0,"performType":0},"text":{"value":"审批"}},
		{"id":"final","type":"snaker:task","properties":{"assignee":"userF","form":"f","taskType":0,"performType":0},"text":{"value":"终审"}},
		{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
		"edges":[
		{"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{}},
		{"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"review","properties":{}},
		{"id":"e3","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"final","properties":{}},
		{"id":"e4","type":"snaker:transition","sourceNodeId":"final","targetNodeId":"end","properties":{}}]}`
	def := &model.ProcessDefine{ID: lockDefineID, Name: "lock_case", DisplayName: "实例锁取证",
		Type: "approval", State: 1, Version: 1, Content: []byte(content)}
	if err := repo.SaveDefine(ctx, def); err != nil {
		t.Fatalf("建定义: %v", err)
	}
	inst, err := eng.StartProcessInstanceByID(ctx, lockDefineID, "applicant", map[string]interface{}{})
	if err != nil {
		t.Fatalf("发起: %v", err)
	}
	apply, _ := repo.FindDoingTasks(ctx, inst.ID, []string{"apply"})
	if len(apply) != 1 {
		t.Fatalf("期望申请待办")
	}
	if _, err := eng.ExecuteProcessTask(ctx, apply[0].ID, "applicant", map[string]interface{}{"submitType": 0}); err != nil {
		t.Fatalf("办结申请: %v", err)
	}
	review, _ := repo.FindDoingTasks(ctx, inst.ID, []string{"review"})
	if len(review) != 1 {
		t.Fatalf("期望审批待办")
	}

	// 1) 外部事务持实例锁 800ms；引擎办理必须被阻塞
	locked := make(chan struct{})
	release := make(chan struct{})
	var holdErr error
	go func() {
		holdErr = repo.WithTx(ctx, func(ctx context.Context) error {
			if err := repo.LockInstance(ctx, inst.ID); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	type result struct {
		err  error
		cost time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		_, err := eng.ExecuteProcessTask(ctx, review[0].ID, "userA", map[string]interface{}{"submitType": 1})
		done <- result{err: err, cost: time.Since(start)}
	}()

	select {
	case r := <-done:
		t.Fatalf("持锁期间办理不得完成（应被实例行锁阻塞），实得 err=%v cost=%s", r.err, r.cost)
	case <-time.After(300 * time.Millisecond):
		// 正确：仍在等待锁
	}
	close(release)
	r := <-done
	if holdErr != nil {
		t.Fatalf("持锁事务失败: %v", holdErr)
	}
	if r.err != nil {
		t.Fatalf("释放锁后办理应成功: %v", r.err)
	}
	if r.cost < 250*time.Millisecond {
		t.Fatalf("办理耗时 %s 未体现锁等待（预期 >= 300ms 量级）", r.cost)
	}

	// 2) 同一任务第二次办理必须被拒，且只产生一套后继
	if _, err := eng.ExecuteProcessTask(ctx, review[0].ID, "userA", map[string]interface{}{"submitType": 1}); err == nil {
		t.Fatalf("同一任务第二次办理应被拒（状态条件/已办判据）")
	}
	final, err := repo.FindDoingTasks(ctx, inst.ID, []string{"final"})
	if err != nil {
		t.Fatalf("读后继: %v", err)
	}
	if len(final) != 1 {
		t.Fatalf("后继终审待办应恰好 1 条，实得 %d", len(final))
	}
	t.Logf("锁等待实测 cost=%s；后继行=%d", r.cost, len(final))
}

// ── 最小 SPI 桩 ──

type asLockUserProv struct{}

func (asLockUserProv) GetUser(string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: "u", RealName: "u"}, nil
}

type lockIDGen struct{ n int64 }

func (g *lockIDGen) NextID() int64 {
	g.n++
	return time.Now().UnixMilli()*1000 + g.n // 与既有测试同一形状：时间戳量级，进程内唯一
}
