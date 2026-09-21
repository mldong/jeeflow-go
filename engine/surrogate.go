// 委托代理自动生效（issues/116，规范 06 §4.5 运行期语义 / 05 §SurrogateInterceptor）——
// 引擎内置、默认开启、可显式关闭。
//
// 此前各栈只有 `processSurrogate/*` 五个台账 action，"建任务那一刻应用生效委托"
// 无人实现（Go 全仓 grep surrogate 命中 interceptor/hook 0 处），用户配好"休假期间
// 张三替我批"后单子仍只发给张三本人。本文件把它变成引擎能力：
//
//   - 时机：任务参与者解析完成后、参与者落库前——并入**任务自身的参与者集合**
//     （nt.ActorIDs），随后由 SaveTask 与任务一起落库。
//     ⚠️ 不走"事后 AddTaskActor 补写"路：补写依赖 taskId 已分配，且与 saveTask 的
//     全量替换语义（jdbc replaceTaskActors）打架，是 Java 首版静默无效的病根。
//   - 动作：命中则追加被委托人，**授权人保留**（任一可办；委托不是转办，不摘原人）。
//   - 默认开启、可显式关闭（WithSurrogateAutoApply(false) / SetSurrogateAutoApply(false)）。
//   - 未配置扩展仓储（surrogateRepo == nil）时静默跳过，不打断建单。
package engine

import (
	"context"
	"log"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 装配 ───────────────────────────────────────────────────────────────────────

// Option 引擎构造选项（issues/116 引入；后续构造期配置统一走这里）。
// New 的四个必填参数不变，故现有 `engine.New(repo, userProv, idGen, exprEval)` 调用零改动。
type Option func(*EngineImpl)

// WithSurrogateRepository 注入委托扩展仓储（spi.ProcessExtRepository）。
// 不注入（或注入 nil）＝ 委托自动生效静默跳过，建单流程不受影响。
func WithSurrogateRepository(ext spi.ProcessExtRepository) Option {
	return func(e *EngineImpl) { e.surrogateRepo = ext }
}

// WithSurrogateAutoApply 委托自动生效开关（默认开启）；传 false 显式关闭，
// 行为回退到"仅台账 CRUD"（配了委托也不会并入参与者）。
func WithSurrogateAutoApply(on bool) Option {
	return func(e *EngineImpl) { e.surrogateOff = !on }
}

// SetSurrogateRepository 注入/替换委托扩展仓储（链式，对齐 SetRegistry/SetExtensions 风格）。
// 传 nil 即"未配置"——静默跳过。
func (e *EngineImpl) SetSurrogateRepository(ext spi.ProcessExtRepository) *EngineImpl {
	e.surrogateRepo = ext
	return e
}

// SetSurrogateAutoApply 运行期开关：false = 显式关闭委托自动生效（回退仅台账）。
func (e *EngineImpl) SetSurrogateAutoApply(on bool) *EngineImpl {
	e.surrogateOff = !on
	return e
}

// SurrogateAutoApply 当前是否启用委托自动生效（集成层自检用）。
//
// 关闭位以 surrogateOff 存储而非 surrogateEnabled bool——零值 EngineImpl{} 直构
// （内部测试/反射构造）也要落在"默认开启"上，反向标记才成立。
func (e *EngineImpl) SurrogateAutoApply() bool {
	return !e.surrogateOff
}

// SurrogateRepository 当前注入的委托扩展仓储（未注入返回 nil）。
func (e *EngineImpl) SurrogateRepository() spi.ProcessExtRepository {
	return e.surrogateRepo
}

// ─── 运行期：参与者落库前并入被委托人 ─────────────────────────────────────────

// applySurrogateToTask 对任务当前参与者逐个查生效委托，命中的被委托人并入
// nt.ActorIDs（原人保留、去重、不级联）。返回参与者是否发生变化（供调试/断言）。
//
// 关闭开关或未配置扩展仓储时直接返回，不报错、不打断建单。
// 单个参与者查询失败只记日志跳过（委托是增强能力，不得因它丢任务）。
func (e *EngineImpl) applySurrogateToTask(ctx context.Context, nt *model.ProcessTask, processName string) bool {
	if !e.SurrogateAutoApply() || e.surrogateRepo == nil || nt == nil || len(nt.ActorIDs) == 0 {
		return false
	}
	now := time.Now()
	// 快照解析出的原始参与者：只对这批人各查一次委托，被委托人不再级联查自己的委托
	// （A→B 且 B→C 不会把 C 也拉进来，与规范"每个 actor 查一次"一致，同时天然免疫环委托）
	base := append([]string(nil), nt.ActorIDs...)
	out := make([]string, 0, len(base)+4)
	out = append(out, base...)
	added := false
	for _, actor := range base {
		s, err := e.surrogateRepo.GetSurrogate(ctx, actor, processName, now)
		if err != nil {
			log.Printf("[jeeflow] 委托查询失败 actor=%s process=%s err=%v（跳过该参与者，不中断建单）",
				actor, processName, err)
			continue
		}
		if s == nil || s.Surrogate == "" || s.Surrogate == actor {
			continue
		}
		if contains(out, s.Surrogate) {
			continue
		}
		out = append(out, s.Surrogate)
		added = true
	}
	if added {
		nt.ActorIDs = out
	}
	return added
}

// saveNewTask 新任务落库唯一入口：先应用生效委托并入参与者，再 SaveTask。
// 调用点（createTask / createTaskWithActors / 顺序会签推进）都已通过
// inst.CreateTask(e.nextID(), …) 拿到非零 taskId，故并入的是将要真实落库的集合，
// 不存在"打在空 id 上静默无效"的时序坑。
func (e *EngineImpl) saveNewTask(ctx context.Context, nt *model.ProcessTask, processName string) error {
	e.applySurrogateToTask(ctx, nt, processName)
	return e.repo.SaveTask(ctx, nt)
}

// surrogateProcessName 委托查询用的流程名：取流程模型 name（对齐 Java
// execution.getProcessModel().getName()），缺失时回落定义行 name（按 defineId 缓存，
// 与 resolveInterceptors 同一姿势）。
func (e *EngineImpl) surrogateProcessName(flow *model.FlowModel, inst *model.ProcessInstance) string {
	if flow != nil && flow.Name != "" {
		return flow.Name
	}
	if inst == nil || inst.DefineID == 0 {
		return ""
	}
	if e.defineNameCache == nil {
		e.defineNameCache = map[int64]string{}
	}
	if n, ok := e.defineNameCache[inst.DefineID]; ok {
		return n
	}
	name := ""
	if def, err := e.repo.FindDefineByID(context.Background(), inst.DefineID); err == nil && def != nil {
		name = def.Name
	}
	e.defineNameCache[inst.DefineID] = name
	return name
}
