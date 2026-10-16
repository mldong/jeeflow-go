// audit8 R77 的**真库回滚探针**（本卡第②步指定的那一格：go 或 rust 上一桩真库腿）。
//
// 读法一（owner 10-16 拍）＝契约下限由监听器侧承担：事务内可以 fire，但接了整轮事务模板的栈
// 必须提供提交后钩子通道。php 那轮（issues/191）用同一形状照出过"回滚场景监听器真会收到码"，
// 本探针在 jdbc 真库上复现同一分界：
//
//	① 回滚腿：事务内落实例行 + FireEvent ⇒ 回调报错整段回滚 ⇒ 库里没行（证探针打在真事务上）
//	   且**监听器一条都不收到**（修前形状＝立即派发 ⇒ 这里会收到 1 条，正是本档要照红的东西）；
//	② 提交腿：同样形状正常提交 ⇒ 行落库且监听器在 Commit 之后恰好收到 1 条。
package jdbc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

type r77UserProv struct{}

func (r77UserProv) GetUser(string) (*model.UserInfo, error) { return nil, nil }

type r77IDGen struct{ n int64 }

func (g *r77IDGen) NextID() int64 { g.n++; return g.n }

func r77Inst(id int64) *model.ProcessInstance {
	now := time.Now()
	return &model.ProcessInstance{
		ID: id, DefineID: defineID, State: model.InstanceStateDoing, Operator: "zhangsan",
		BusinessNo: "R77-TXN", Variables: map[string]interface{}{"k": "v"},
		CreateTime: now, UpdateTime: now, CreateUser: "t", UpdateUser: "t",
	}
}

func TestR77AfterCommitRollbackProbe(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	cleanup(t, db)
	defer cleanup(t, db)
	insertDefine(t, db, "go-simple", loadFlow(t, "01-simple.json"))

	ctx := context.Background()
	repo := jdbc.New(db)
	// 监听器形状＝java 参考实现那一头：fire 仍在事务内到达（断 firedInsideTx 以证 145-3 没被动），
	// 而"必须看到已提交世界"的那一腿由监听器自己经 RunAfterCommit 声明。
	var firedInsideTx []bool
	var deferred []int64
	eng := engine.New(repo, r77UserProv{}, &r77IDGen{}, nil)
	eng.SetExtensions(&engine.Extensions{Listeners: []engine.ProcessEventListener{
		func(ctx context.Context, evt engine.ProcessEvent) {
			_, inTx := jdbc.TxFromContext(ctx)
			firedInsideTx = append(firedInsideTx, inTx)
			eng.RunAfterCommit(ctx, func(context.Context) { deferred = append(deferred, evt.InstanceID) })
		},
	}})

	// ① 回滚腿
	rollID := int64(977001)
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if err := repo.SaveInstance(ctx, r77Inst(rollID)); err != nil {
			return err
		}
		eng.FireEvent(ctx, engine.ProcessEvent{Type: engine.EventProcessStart, InstanceID: rollID})
		return errors.New("injected: R77 回滚探针")
	})
	if err == nil {
		t.Fatalf("前置失效：注入的回滚错误没传出来（这段事务根本没跑）")
	}
	var n int
	if err := db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_instance WHERE id = ?"), rollID).Scan(&n); err != nil {
		t.Fatalf("回滚腿查库失败: %v", err)
	}
	if n != 0 {
		t.Fatalf("回滚腿必须真回滚（否则「提交后腿没执行」是空转）：%d 行残留", n)
	}
	if len(firedInsideTx) != 1 || !firedInsideTx[0] {
		t.Fatalf("派发应仍在事务内到达并带动作 ctx（145-3 形状）：%v", firedInsideTx)
	}
	if len(deferred) != 0 {
		t.Fatalf("回滚场景提交后腿一条都不该执行（修前无通道＝这一腿只能在事务内跑，实得 %v）", deferred)
	}

	// ② 提交腿
	commitID := int64(977002)
	if err := repo.WithTx(ctx, func(ctx context.Context) error {
		if err := repo.SaveInstance(ctx, r77Inst(commitID)); err != nil {
			return err
		}
		eng.FireEvent(ctx, engine.ProcessEvent{Type: engine.EventProcessStart, InstanceID: commitID})
		if len(deferred) != 0 {
			return errors.New("提交前就跑了提交后腿")
		}
		return nil
	}); err != nil {
		t.Fatalf("提交腿失败: %v", err)
	}
	if err := db.QueryRowContext(ctx, ph("SELECT COUNT(*) FROM wf_process_instance WHERE id = ?"), commitID).Scan(&n); err != nil {
		t.Fatalf("提交腿查库失败: %v", err)
	}
	if n != 1 {
		t.Fatalf("提交腿应落库 1 行，实得 %d", n)
	}
	if len(deferred) != 1 || deferred[0] != commitID {
		t.Fatalf("提交后腿应恰好执行一次并带上已提交的实例，实得 %v", deferred)
	}
	// 提交后腿跑在**没有事务**的 ctx 上（连接已释放，反查须走池连接而非旧事务连接）
	var sawTx bool
	if err := repo.WithTx(context.Background(), func(ctx context.Context) error {
		probe := ctx
		eng.RunAfterCommit(probe, func(c context.Context) {
			_, sawTx = jdbc.TxFromContext(c)
		})
		return nil
	}); err != nil {
		t.Fatalf("ctx 形状探针失败: %v", err)
	}
	if sawTx {
		t.Fatalf("提交后腿不得再拿事务连接（已 COMMIT）")
	}
}
