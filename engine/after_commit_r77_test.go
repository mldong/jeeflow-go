// audit8 R77 读法一（issues/234 §4.3／spec12 §12.3.1）：可选"提交后腿"通道。
// 契约下限＝"监听器看到的世界＝已提交"由**监听器侧**承担：fire 的发起点不动（仍在事务内、仍带动作
// ctx，145-3 那条既有判据不许被本轮改动），监听器要提交后语义就自己经 RunAfterCommit 声明。
// php 侧 R25（issues/191）同形；真库回滚腿在 repository/jdbc/after_commit_r77_test.go。
package engine

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
)

type r77Repo struct {
	*memory.Repository
	inTx    bool
	pending []func(context.Context)
}

func (r *r77Repo) AfterCommit(_ context.Context, fn func(ctx context.Context)) bool {
	if !r.inTx {
		return false
	}
	r.pending = append(r.pending, fn)
	return true
}

func (r *r77Repo) flushAll() {
	cbs := r.pending
	r.pending = nil
	for _, fn := range cbs {
		fn(context.Background())
	}
}

func r77Engine(t *testing.T, repo *r77Repo) (*EngineImpl, *[]int) {
	t.Helper()
	var got []int
	e := &EngineImpl{repo: repo}
	e.SetExtensions(&Extensions{Listeners: []ProcessEventListener{
		func(_ context.Context, evt ProcessEvent) { got = append(got, int(evt.Type)) },
	}})
	return e, &got
}

func TestR7701FireShapeUnchangedInTx(t *testing.T) {
	// 载体在事务中、监听器没接通道 ⇒ 派发仍立即发生且拿到原 ctx（145-3 的形状不能被 R77 改动）
	repo := &r77Repo{Repository: memory.New(), inTx: true}
	e, got := r77Engine(t, repo)
	e.FireEvent(context.Background(), ProcessEvent{Type: EventProcessStart, InstanceID: 1})
	if len(*got) != 1 || (*got)[0] != int(EventProcessStart) {
		t.Fatalf("fire 的发起点不该被引擎整体挪走（监听器没接通道就得立即派发）：%v", *got)
	}
	if len(repo.pending) != 0 {
		t.Fatalf("引擎不得替监听器自作主张登记提交后腿：%d 条", len(repo.pending))
	}
}

func TestR7702ListenerChannelDefersUntilCommit(t *testing.T) {
	repo := &r77Repo{Repository: memory.New(), inTx: true}
	e, _ := r77Engine(t, repo)
	var ran int
	e.RunAfterCommit(context.Background(), func(context.Context) { ran++ })
	if ran != 0 || len(repo.pending) != 1 {
		t.Fatalf("接了通道的腿应登记到提交后（ran=%d pending=%d）", ran, len(repo.pending))
	}
	repo.flushAll()
	if ran != 1 {
		t.Fatalf("提交后腿应执行且只执行一次：%d", ran)
	}
}

func TestR7703ImmediateWithoutActiveTx(t *testing.T) {
	// 门面级没外包事务模板 ⇒ "落库后"与"提交后"等价 ⇒ 立即执行（行为与上一版一致）
	repo := &r77Repo{Repository: memory.New(), inTx: false}
	e, _ := r77Engine(t, repo)
	var ran int
	e.RunAfterCommit(context.Background(), func(context.Context) { ran++ })
	if ran != 1 {
		t.Fatalf("无活动事务时通道须立即执行：%d", ran)
	}
	if len(repo.pending) != 0 {
		t.Fatalf("无事务时不该留下登记：%d 条", len(repo.pending))
	}
}

func TestR7704NoExtensionsZeroSideEffects(t *testing.T) {
	repo := &r77Repo{Repository: memory.New(), inTx: true}
	e := &EngineImpl{repo: repo}
	e.FireEvent(context.Background(), ProcessEvent{Type: EventProcessStart, InstanceID: 1})
	if len(repo.pending) != 0 {
		t.Fatalf("未装配 Extensions 时不得登记提交后腿（§11.5 无监听器义务）：%d", len(repo.pending))
	}
}
