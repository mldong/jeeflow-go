package engine

import (
	"log"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

import "github.com/mldong/jeeflow-go/model"

// ─── Interceptor ───────────────────────────────────────────────────────────────

// FlowInterceptor 流程拦截器——对标 Java FlowInterceptor
type FlowInterceptor interface {
	// PreHandle 节点执行前调用，返回 false 则跳过该节点
	PreHandle(node *model.FlowNode, inst *model.ProcessInstance) (proceed bool)
	// PostHandle 节点执行后调用
	PostHandle(node *model.FlowNode, inst *model.ProcessInstance)
	// Order 拦截器排序，越小越先执行
	Order() int
}

// ─── Assignment ────────────────────────────────────────────────────────────────

// AssignmentHandler 动态参与者指派——对标 Java AssignmentHandler.assign
// handlerName: 节点配置的 assignmentHandler 类名（如 "com.xxx.MyHandler"）
// 返回参与者 ID 列表（nil 表示不处理）
type AssignmentHandler func(handlerName string, node *model.FlowNode, inst *model.ProcessInstance) []string

// ─── Decision ──────────────────────────────────────────────────────────────────

// DecisionHandler 自定义决策处理器——对标 Java DecisionHandler
// handlerName: 节点配置的 decisionHandler 类名
// 返回选中的分支边 ID（空字符串表示不处理）
type DecisionHandler func(handlerName string, node *model.FlowNode, inst *model.ProcessInstance, vars map[string]interface{}) string

// ─── Event ─────────────────────────────────────────────────────────────────────

// EventType 流程事件类型——码值取规范 11-events §11.3 的 **A 套整型**（java 血缘 1..4 连续扩展）。
//
// issues/132 §5 / spec §11.6：本栈原为 iota 起的 0..5 套（EventTaskComplete=4、EventCCCreate=5），
// 本轮**整表重排**到 A 套 1..9（EventCCCreate 5→4、EventTaskComplete 4→5）。这是破坏性变更：
// 已发布版本里数字码不可混用，集成层监听器一律**按规范名分派**（SpecName()），不得拿数字码当跨栈判据。
type EventType int

const (
	// EventProcessInstanceStart 1 实例发起成功（sourceId=instanceId；实例行 insert 之后）
	EventProcessInstanceStart EventType = 1
	// EventProcessInstanceEnd 2 实例进入终态——**流程自己走到终点**（办结 20 / 拒绝到终态 45
	// 共用这一支，靠载荷 state 分）（sourceId=instanceId；实例 state 落库为 20/45 这类
	// "走到终点"的状态之后）。
	// ⚠️ §11.3 码 2 行明写：**30(撤回)/40(终止) 不由本支表达**——各有专属码 8/9；
	// 同轮既发 8 又发 2 会让下游收到"流程已办结"的错通知（规范 08 场景 32 ⇒ 红）。
	EventProcessInstanceEnd EventType = 2
	// EventProcessTaskStart 3 新待办生成（含会签逐人、回退复活行、子流程任务；
	// 每个任务行落库之后逐任务 fire，载荷带 actors）
	EventProcessTaskStart EventType = 3
	// EventCCCreate 4 新增一条抄送记录（sourceId=instanceId；cc 行落库之后**逐抄送人** fire 一次。
	// 发起 f_ccActors／办理 tf_ccActors／手动 createCCInstance 三条路径同判——§11.2 原则 1）
	EventCCCreate EventType = 4
	// EventTaskComplete 5 任务被办掉（同意/跳转/会签办理；任务行 state 更新为已完成落库之后）
	EventTaskComplete EventType = 5
	// EventTaskReject 6 任务被退回/拒绝（含退发起人、软拒绝、跳转回退；与 5 **互斥**——
	// 同一动作走 reject 就不再 fire complete，靠载荷 submitType 分具体退法）
	EventTaskReject EventType = 6
	// EventTaskTransfer 7 转办发生（任务参与者被替换并落库之后）
	EventTaskTransfer EventType = 7
	// EventTaskWithdraw 8 撤回发生（撤回把实例 state 写 30 落库之后；
	// **每轮撤回只 fire 一次**，不逐任务。§11.3 码 8 行：**撤回只发 8，不补发 2**）
	EventTaskWithdraw EventType = 8
	// EventInstanceTerminated 9 实例被终止（state 写 40 落库之后）
	//
	// ⚠️ 本栈引擎目前没有任何写 state=40 的路径（无 terminate action，facade_test.go 的
	// issues/134 档也记着"本栈引擎不产出 40"）⇒ 码位与载荷形状先立好，**触发源缺席故无 fire 点**，
	// 待补终止 action 时在写 40 落库之后 fire（严禁拿 99 废弃 / 45 拒绝顶这一支）。
	EventInstanceTerminated EventType = 9

	// 10+ 预留：超时催办 / 超时自动通过 …（§11.3「本轮不发，仅占号防分叉」）。
	// 八栈都没有时钟扫描器，发了没有触发源——严禁在本表里私自发号。
)

// 兼容别名（issues/132 §5 整表重排的过渡层；值已随 A 套挪位）
//
// ⚠️ EventProcessFinish 与 EventProcessReject **同值 2**：实例级"办结/拒绝"两个旧事实位按 §11.6
// 收敛为"实例终态＝PROCESS_INSTANCE_END(2)，靠载荷 state 分"，任务退回另立 EventTaskReject(6)。
// 监听器 switch 不得同时列这两个别名（Go 编译期 duplicate case），集成壳 rebase 时合并成一支即可。
const (
	EventProcessStart  = EventProcessInstanceStart // 0 → 1
	EventProcessFinish = EventProcessInstanceEnd   // 1 → 2（与 Reject 合并为"实例终态"）
	EventProcessReject = EventProcessInstanceEnd   // 2 → 2（实例终态；任务退回见 EventTaskReject）
	EventTaskCreate    = EventProcessTaskStart     // 3 → 3
)

// specNameByEventType 规范名表（§11.3 权威名；集成层跨栈判据一律用名不用码）
var specNameByEventType = map[EventType]string{
	EventProcessInstanceStart: "PROCESS_INSTANCE_START",
	EventProcessInstanceEnd:   "PROCESS_INSTANCE_END",
	EventProcessTaskStart:     "PROCESS_TASK_START",
	EventCCCreate:             "CC_CREATE",
	EventTaskComplete:         "TASK_COMPLETE",
	EventTaskReject:           "TASK_REJECT",
	EventTaskTransfer:         "TASK_TRANSFER",
	EventTaskWithdraw:         "TASK_WITHDRAW",
	EventInstanceTerminated:   "INSTANCE_TERMINATED",
}

// SpecName 规范名（§11.3）；未登记码返回 "UNKNOWN(<code>)"，绝不猜名。
func (t EventType) SpecName() string {
	if n, ok := specNameByEventType[t]; ok {
		return n
	}
	return "UNKNOWN(" + strconv.Itoa(int(t)) + ")"
}

// EventTypeBySpecName 规范名 → 码值（集成层按名分派/门禁回读用）。
func EventTypeBySpecName(name string) (EventType, bool) {
	for t, n := range specNameByEventType {
		if n == strings.TrimSpace(name) {
			return t, true
		}
	}
	return 0, false
}

// ProcessEvent 流程事件。
//
// 承载形态各栈自定（§11.3 注：java 是 ProcessEvent{eventType, sourceId, ccActorId, data}），
// 本栈用强类型字段 + Payload() 单源派生规范载荷 map，契约只钉"必须能拿到 §11.3 那些键"。
// InstanceID 即"sourceId 指向"列：任务类码（3/5/6/7）另带 TaskID，其余（含 4）以 InstanceID 为 sourceId。
type ProcessEvent struct {
	Type       EventType
	InstanceID int64
	TaskID     int64
	NodeID     string
	Operator   string
	// CcActorID 抄送人 id（仅 CC_CREATE 事件非空，直传事件体，监听器免反查 cc 表——
	// 对齐 Java ProcessEvent.ccActorId / PHP ccActorId / issues/102 语义铁律 2）
	CcActorID string
	// State 实例**落库后**的状态整数（code 2 必备键 state；其余码 0＝未设）
	State int
	// Actors 该待办的参与者列表（code 3 必备键 actors，含委托并入后的最终集合）
	Actors []string
	// SubmitType 本次办理的提交类型（code 5/6 必备键 submitType；指针以区分"未设"与 0=APPLY）
	SubmitType *int
	// FromActor / ToActor 转办的两造（code 7 必备键）
	FromActor string
	ToActor   string
	// Reason 终止原因（code 9 必备键；本栈暂无触发源）
	Reason string
}

// Payload §11.3「直传载荷键」的 camelCase map 形态，由上面的强类型字段单源派生
// （只落非零/非空/已设的键，监听器既可用 evt.State 也可用 Payload()["state"]）。
func (p ProcessEvent) Payload() map[string]interface{} {
	out := map[string]interface{}{}
	if p.InstanceID != 0 {
		out["instanceId"] = p.InstanceID
	}
	if p.TaskID != 0 {
		out["taskId"] = p.TaskID
	}
	if p.NodeID != "" {
		out["nodeId"] = p.NodeID
	}
	if p.Operator != "" {
		out["operator"] = p.Operator
	}
	if p.CcActorID != "" {
		out["ccActorId"] = p.CcActorID
	}
	if p.State != 0 {
		out["state"] = p.State
	}
	if p.Actors != nil {
		out["actors"] = append([]string(nil), p.Actors...)
	}
	if p.SubmitType != nil {
		out["submitType"] = *p.SubmitType
	}
	if p.FromActor != "" {
		out["fromActor"] = p.FromActor
	}
	if p.ToActor != "" {
		out["toActor"] = p.ToActor
	}
	if p.Reason != "" {
		out["reason"] = p.Reason
	}
	return out
}

// ProcessEventListener 事件监听器。
// 订阅形状＝**监听器列表**（§11.2 原则 4 / §11.5）：一次 fire 送给全部已注册监听器，
// 注册顺序＝回调顺序，不得"后注册覆盖前注册"。
type ProcessEventListener func(event ProcessEvent)

// ─── Engine Extensions ─────────────────────────────────────────────────────────

// Extensions 引擎扩展配置
type Extensions struct {
	Interceptors     []FlowInterceptor
	// 定义级拦截器注册表（issue 34）：名字 → 实例；流程定义顶层 postInterceptors 按名解析
	InterceptorRegistry map[string]FlowInterceptor
	AssignmentHandler AssignmentHandler
	DecisionHandler   DecisionHandler
	Listeners        []ProcessEventListener
}

func (e *EngineImpl) SetExtensions(ext *Extensions) {
	e.ext = ext
	e.interceptorCache = map[int64][]FlowInterceptor{}
	e.defineNameCache = map[int64]string{}
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

// interceptorCache 定义级拦截器解析缓存（issue 34，按 defineId）
// issues/60：解析与校验分离——声明中存在未注册名时返回显式错误（不静默跳过），
// 且错误不写缓存，保证后续执行持续报错直至注册补齐。
func (e *EngineImpl) resolveInterceptors(inst *model.ProcessInstance) ([]FlowInterceptor, error) {
	if e.ext == nil {
		return nil, nil
	}
	if inst == nil || inst.DefineID == 0 {
		return e.ext.Interceptors, nil
	}
	if cached, ok := e.interceptorCache[inst.DefineID]; ok {
		return cached, nil
	}
	list := e.ext.Interceptors
	if def, err := e.repo.FindDefineByID(context.Background(), inst.DefineID); err == nil && def != nil {
		var meta struct {
			PostInterceptors string `json:"postInterceptors"`
		}
		if json.Unmarshal(def.Content, &meta) == nil && strings.TrimSpace(meta.PostInterceptors) != "" {
			list = nil
			for _, name := range strings.Split(meta.PostInterceptors, ",") {
				name = strings.TrimSpace(name)
				if name != "" {
					if ic, ok := e.ext.InterceptorRegistry[name]; !ok {
						return nil, fmt.Errorf("postInterceptors 声明的拦截器未注册: %s", name)
					} else {
						list = append(list, ic)
					}
				}
			}
		}
	}
	e.interceptorCache[inst.DefineID] = list
	return list, nil
}

// firePreInterceptors 执行前置拦截器
func (e *EngineImpl) firePreInterceptors(node *model.FlowNode, inst *model.ProcessInstance) (bool, error) {
	if e.ext == nil {
		return true, nil
	}
	list, err := e.resolveInterceptors(inst)
	if err != nil {
		return false, err
	}
	for _, ic := range list {
		if !ic.PreHandle(node, inst) {
			return false, nil
		}
	}
	return true, nil
}

// firePostInterceptors 执行后置拦截器
func (e *EngineImpl) firePostInterceptors(node *model.FlowNode, inst *model.ProcessInstance) error {
	if e.ext == nil {
		return nil
	}
	list, err := e.resolveInterceptors(inst)
	if err != nil {
		return err
	}
	for _, ic := range list {
		ic.PostHandle(node, inst)
	}
	return nil
}

// fireEvent 发布事件。
// 兜底语义（issues/104 P2 统一口径）：单监听器 panic 只记录不传播——
// 不得影响引擎主流程，也不得中断后续监听器（对齐 PHP per-listener catch）。
func (e *EngineImpl) fireEvent(evt ProcessEvent) {
	if e.ext == nil { return }
	for _, l := range e.ext.Listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[jeeflow] process event listener panic: type=%d(%s) instance=%d err=%v",
						int(evt.Type), evt.Type.SpecName(), evt.InstanceID, r)
				}
			}()
			l(evt)
		}()
	}
}

// FireEvent 公开事件发布入口（facade 层事件源调用，issues/102 CC_CREATE）：
// Go 栈的 cc 实例创建在 facade（CreateCcInstance），fire 通道在引擎侧私有，
// 故 facade 经此入口发布事件——纯增量：未装配 Extensions/监听器时零副作用。
func (e *EngineImpl) FireEvent(evt ProcessEvent) {
	e.fireEvent(evt)
}

// ─── 抄送腿（spec 11-events §11.7 / issues/127）───────────────────────────────

// HandleCcActors 抄送唯一漏斗：解析抄送人 → 建 wf_process_cc_instance 行 → **落库之后**
// 逐抄送人 fire CC_CREATE（code 4，ccActorId 直传事件体）。
//
// 三条路径共用本函数（§11.2 原则 1「同一事实只发一次，路径不进事件名」）：
//   - 发起 f_ccActors（facade startAndExecute）
//   - 办理 tf_ccActors（引擎 ExecuteProcessTask 一条路径，与任务更新同一个 ctx＝同一个事务）
//   - 手动 processInstance/createCCInstance（facade）
//
// ⚠️ §11.7 边界 2：覆盖面以 Java 基准为准，**办理腿只算 executeProcessTask 一条**。
// ExecuteAndJumpToEnd / ExecuteAndJumpTask / ExecuteAndJumpToFirstTaskNode 这类跳转·回退
// action 带的 tf_ccActors 本轮**不建 cc、不发 CC_CREATE**（java 基准里 handleCcActors 的
// 唯一调用点就在 executeProcessTask；php/csharp 同形）。要扩得先改 java 基准再逐栈传播并
// 另立案——单栈自行放宽＝跨栈分叉（go 第一轮 prepareExecuteTask 落点就是这种超集，已收窄）。
//
// 基准＝Java JeeflowEngineImpl.handleCcActors → notifyCcCreate（发起与办理走同一条腿）。
// ccActors 为空/未配置 ⇒ 零写入、零 fire、返回 nil（纯增量：不带抄送的办理行为不变）。
// 接收人过滤（trim / 非空 / 纯数字 / 去重）归集成层监听器，引擎只按 cc 行粒度 fire。
func (e *EngineImpl) HandleCcActors(ctx context.Context, instanceID int64, operator string, ccActors interface{}) error {
	actors := parseCcActors(ccActors)
	if len(actors) == 0 {
		return nil
	}
	if err := e.repo.CreateCcInstance(ctx, instanceID, operator, actors...); err != nil {
		return err
	}
	e.notifyCcCreate(instanceID, actors)
	return nil
}

// notifyCcCreate CC_CREATE 逐抄送人 fire（与 CreateCcInstance 逐行 INSERT 一一对应）。
// Operator 不带——事件体的"操作人"语义留给办理人/发起人，抄送人只走 CcActorID（对齐 Java）。
func (e *EngineImpl) notifyCcCreate(instanceID int64, actors []string) {
	for _, actor := range actors {
		e.fireEvent(ProcessEvent{Type: EventCCCreate, InstanceID: instanceID, CcActorID: actor})
	}
}

// parseCcActors 抄送人入参归一：逗号串 / []string / []interface{}（JSON body 数组形态）
// → []string；逐元素 trim，空元素剔除；nil / 空串 / 空数组 → nil。
func parseCcActors(v interface{}) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		var out []string
		for _, s := range strings.Split(t, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		var out []string
		for _, s := range t {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []interface{}:
		var out []string
		for _, a := range t {
			s := strings.TrimSpace(fmt.Sprintf("%v", a))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		if s := strings.TrimSpace(fmt.Sprintf("%v", v)); s != "" {
			return []string{s}
		}
		return nil
	}
}
