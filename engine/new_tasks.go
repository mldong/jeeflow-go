package engine

// spec04 §完成任务（:37/:77/:98/:111）：执行族四法返回**本次动作产出的任务行**，
// 基准＝java `JeeflowEngineImpl` 返回 `exec.getProcessTaskList()`（该列表由建单处每次追加），
// 空列表＝本动作没产出新任务（如定稿到 end）。start 族两法照旧返回实例（spec04 :9/:24）。
//
// 落点选择（audit8 R92，2026-10-15）：go 引擎没有 java 那个 Execution 载体，而全部建单都汇到
// 唯一一口 `saveNewTask`，且整条调用链本就带 ctx（事务传播即靠它透传 WithTx）。
// 因此这里用**每次调用作用域**的 typed ctx 值作收集器：公开方法入口注入、saveNewTask 追加、
// 公开方法出口读出——避免为透传一个 out 参数去改 12 个中间函数签名＋约 28 个调用点。
// 语义与 java 一致的地方：作用域＝一次动作（一次公开调用一份 sink），不是引擎级全局状态，
// 因此并发调用互不污染；未注入 sink 的调用（如内部/测试直调 saveNewTask）静默不收集，
// 与"该次调用不关心产出列表"同义。

import (
	"context"

	"github.com/mldong/jeeflow-go/model"
)

type newTasksSinkKey struct{}

// withNewTasksSink 给一次执行族调用挂上收集器，返回新 ctx 与 sink 句柄。
func withNewTasksSink(ctx context.Context) (context.Context, *[]*model.ProcessTask) {
	sink := new([]*model.ProcessTask)
	return context.WithValue(ctx, newTasksSinkKey{}, sink), sink
}

// appendNewTask 在唯一建单口把新行计入本次动作的产出（未注入 sink 时不收集）。
func appendNewTask(ctx context.Context, nt *model.ProcessTask) {
	if nt == nil {
		return
	}
	sink, ok := ctx.Value(newTasksSinkKey{}).(*[]*model.ProcessTask)
	if !ok || sink == nil {
		return
	}
	*sink = append(*sink, nt)
}

// newTasksOf 读出产出列表；始终返回非 nil 切片（对外形状＝空数组，不是 null）。
func newTasksOf(sink *[]*model.ProcessTask) []*model.ProcessTask {
	if sink == nil || *sink == nil {
		return []*model.ProcessTask{}
	}
	return *sink
}
