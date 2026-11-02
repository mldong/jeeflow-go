// 后加签（可重复）——配置解析、写入口校验、推进闸、轮次与账本（go 腿）。
//
// 基准＝jeeflow-core 的 `core/AddSignSupport.java` + `handler/impl/AddSignHandler.java`
// + `model/TaskModel.java`（三档推进闸）＋ `parser/impl/TaskParser.java`／`parser/ModelParser.java`
// （解析期两开关与两条硬校验），契约正文＝jeeflow-doc spec `04-engine-ops.md`「后加签（可重复）」
// 与 `06-facade.md` §2.7（协议键）＋文末失败文案表（四条逐字 msg）。
//
// 语义一句话：审批人办结自己那一条的同时可指定一名/多名后继办理人；被指定人在**同一节点**续办
// 且可继续加签；某次提交不再点人且本节点无未完成任务，才流出本节点。不新增节点类型、
// 不画自环边、不加 submitType 档。
//
// # go 与 java 的形状差（移植时逐条对过，别照抄类结构）
//
//   - java 有 `Execution` 载体，加签人挂在 `Execution#addSignOperators` 字段上只认当次请求；
//     go 没有 Execution，等价物＝`executeProcessTaskInner` 在**合并变量之前**从请求 args 取那份
//     `addSignOperatorsFrom(args)`，显式带进推进闸，绝不让处理器去读合并后的 vars（那里混着实例变量，
//     会把上一轮的 tf_addSignOperators 复活，凭空多建一条本节点待办——spec 04「流程变量约定」末注）。
//   - java 的解析期是 `ModelParser.parse`（定义 JSON → ProcessModel 那一跳，每次执行都会走）；
//     go 没有独立解析阶段（见 engine_impl.go:853 的现读注释），等价点＝每一处
//     `json.Unmarshal(def.Content, &flow)` 之后调 `validateAddSignFlow`（发起腿／执行公共序言／
//     loadFlowForTask 三处，覆盖 start 与四条执行族入口）。
//   - java 的三档闸写在 `TaskModel.exec`；go 的节点执行链是 `executeProcessTaskInner` 里那段
//     `ct == "SEQUENTIAL"` / `PARALLEL|RATIO` / 落穿流出边，加签档插在同一处（会签档之后、
//     流出边之前）。
//   - java 的 `AddSignHandler` 只管建单与裁决；报错类判据由 `AddSignSupport.validateSubmission`
//     在 `prepareExecution` 里、`instance.completeTask` **之前**拦下。go 同序：
//     `validateAddSignSubmission` 落在 `prepareExecuteTask` 的 `inst.CompleteTask` 之前——
//     **内存仓同样没有回滚**，先办结再判错就留下一条已完成却无人能改的任务行。
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

const (
	// PropAllowAddSign 节点属性：后加签开关（properties 平铺层，不接受 ext 嵌套），缺省 false。
	// 缺省＝关 ⇒ 不配置两键的既有流程行为逐字不变（spec 04 语义边界表最后一行）。
	PropAllowAddSign = "allowAddSign"
	// PropAddSignMaxRounds 节点属性：加签轮次上限，整数 ≥0，0＝不限，缺省 0。
	PropAddSignMaxRounds = "addSignMaxRounds"

	// KeyAddSignOperators 协议键：本次提交指定的**本节点**后继办理人，逗号串与数组两形同判据
	// （spec 06 §2.7）。与 KeyNextNodeOperator 互斥；节点未开 allowAddSign 却传该键 ⇒ 报错。
	KeyAddSignOperators = "tf_addSignOperators"
	// KeyAddSignHistory 协议键：追加式账本，每圈 append 一条
	// {round, fromActor, toActors, time, operator}，只追加不覆盖；落在**已完成任务行**变量上，
	// tf_ 前缀使 lineageVars（退回复活腿）天然剔掉它，不污染新待办。
	KeyAddSignHistory = "tf_addSignHistory"
	// VarAddSignRound 轮次键前缀（拼 _{节点名}，与 prefixKey 同形）。
	// 不复用会签簿记 loopCounter：后者是退回复活时被 lineageVars 剔掉的会签计数，
	// 加签链的轮次要跨"已办过的人"计数，两者语义不同（java FlowConst.ADD_SIGN_ROUND 同注）。
	VarAddSignRound = "addSignRound"

	// MsgAddSignMutex 四条逐字判据文案（跨栈统一，spec 06 文末失败文案表；P2 拍板＝内置三线逐字随 java）。
	MsgAddSignMutex = "tf_addSignOperators 与 tf_nextNodeOperator 互斥，不能同时提交"
	// MsgAddSignSwitchOff 节点未开开关却传键 ⇒ 报错，不静默忽略（静默忽略＝客户排障地狱）。
	MsgAddSignSwitchOff = "节点[%s]未开启 allowAddSign，不接受 tf_addSignOperators"
	// MsgAddSignSelf 加给自己＝死循环入口。
	MsgAddSignSelf = "不能把任务加签给自己"
	// MsgAddSignMaxRounds 超上限 ⇒ 报错，**不强制放行**（放行＝伪造"审批完成"）。
	MsgAddSignMaxRounds = "节点[%s]的加签轮次已达上限 %d，本次提交被拒绝"

	// addSignTimeLayout 全生态时间格式（spec §2.4），与 tf_transferHistory 同尺。
	addSignTimeLayout = "2006-01-02 15:04:05"
)

// ─── 两开关解析（非法值不得静默当缺省，纪律照 reapplyMode 先例） ──────────────

// parseAllowAddSign 缺省 false；非布尔档报配置错误。
// 字符串 "true"/"false"（不分大小写）与空白串按 java `String.valueOf(v).trim()` 同档收下，
// 其余一律响亮报错——静默落回 false 会让配置者以为开关生效。
func parseAllowAddSign(value interface{}, nodeID string) (bool, error) {
	if value == nil {
		return false, nil
	}
	if b, ok := value.(bool); ok {
		return b, nil
	}
	v := strings.TrimSpace(fmt.Sprintf("%v", value))
	switch strings.ToLower(v) {
	case "":
		return false, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("节点[%s]的 %s 配置非法: %s", nodeID, PropAllowAddSign, v)
}

// parseAddSignMaxRounds 缺省 0＝不限；非整数或负数报配置错误。
// 文案分两档与 java 逐字对齐：Number 支非整数＝「必须是整数」，字符串解析失败＝「必须是整数，0 表示不限」。
func parseAddSignMaxRounds(value interface{}, nodeID string) (int, error) {
	bad := func(v interface{}, tail string) error {
		return fmt.Errorf("节点[%s]的 %s 配置非法: %v%s", nodeID, PropAddSignMaxRounds, v, tail)
	}
	neg := func(n int) error { return bad(n, "（不得为负，0 表示不限）") }
	switch val := value.(type) {
	case nil:
		return 0, nil
	case int:
		if val < 0 {
			return 0, neg(val)
		}
		return val, nil
	case int64:
		if val < 0 {
			return 0, neg(int(val))
		}
		return int(val), nil
	case float64:
		if val != float64(int64(val)) {
			return 0, bad(val, "（必须是整数）")
		}
		if val < 0 {
			return 0, neg(int(val))
		}
		return int(val), nil
	case json.Number:
		n, err := val.Int64()
		if err != nil {
			return 0, bad(val.String(), "（必须是整数，0 表示不限）")
		}
		if n < 0 {
			return 0, neg(int(n))
		}
		return int(n), nil
	case bool:
		return 0, bad(val, "（必须是整数，0 表示不限）")
	default:
		v := strings.TrimSpace(fmt.Sprintf("%v", value))
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v) // 严格整数解析（java Integer.parseInt 同尺，"1.5" 落错误档）
		if err != nil {
			return 0, bad(v, "（必须是整数，0 表示不限）")
		}
		if n < 0 {
			return 0, neg(n)
		}
		return n, nil
	}
}

// ─── 解析期硬校验（规范 02 §4.3／spec 04 语义边界表末两行） ────────────────────

// validateAddSignFlow 对流程模型里每个任务节点做三件事：
//  1. 两开关逐个解析——非法值报错，不得静默当缺省（java TaskParser 里 parseAllow/parseMaxRounds
//     是**无条件**调的，所以"没开开关的节点配了脏值"同样要红，不能只在 allow==true 时校）；
//  2. 会签节点（performType=1）配 allowAddSign ⇒ 报错：会签池内加人与会签之后加人是两种语义，
//     merged 判据会打架；
//  3. 首任务节点（申请节点）配 allowAddSign ⇒ 报错：申请节点的办理是发起/重提语义，与加签路由打架。
//
// 为什么不在运行时降级：静默跑下去会让配置者以为开关生效，实际两档推进闸各裁各的
// （java ModelParser 同一条理由，原文注释在 ModelParser.java 那一段）。
//
// 为什么落在"每一处解析点"：go 没有 java 的独立解析阶段，定义 JSON → FlowModel 这一跳散在
// 发起腿／执行公共序言／loadFlowForTask 三处（见本文件头），三处都调即覆盖
// start 与四条执行族入口＝java ModelParser.parse 的覆盖面。
func (e *EngineImpl) validateAddSignFlow(flow *model.FlowModel) error {
	if flow == nil {
		return nil
	}
	for i := range flow.Nodes {
		node := &flow.Nodes[i]
		if node.Type != model.TypeTask {
			continue
		}
		allow, err := parseAllowAddSign(node.Properties[PropAllowAddSign], node.ID)
		if err != nil {
			return err
		}
		if _, err := parseAddSignMaxRounds(node.Properties[PropAddSignMaxRounds], node.ID); err != nil {
			return err
		}
		if !allow {
			continue
		}
		if IsCountersign(node.Properties["performType"]) {
			return fmt.Errorf("会签节点[%s]不允许配置 allowAddSign：会签池内加人与会签之后加人是两种语义，推进闸会打架", node.ID)
		}
		if e.isFirstTaskNode(flow, node) {
			return fmt.Errorf("首任务节点[%s]不允许配置 allowAddSign：申请节点的办理是发起/重提语义，与加签路由打架", node.ID)
		}
	}
	return nil
}

// ─── 写入口四判据（必须排在任何状态变更之前） ────────────────────────────────

// addSignOperatorsFrom 本次提交点名的加签人（逗号串与数组两形共用 §2.11 那枚归一单点
// valueToActors → spi.NormalizeActors：逐元素 trim、空串/纯空白/null 丢弃、丢完为空＝没传）。
// 没点人返回空集。
func addSignOperatorsFrom(args map[string]interface{}) []string {
	if args == nil {
		return nil
	}
	v, ok := args[KeyAddSignOperators]
	if !ok {
		return nil
	}
	return valueToActors(v, true)
}

// validateAddSignSubmission 写入口校验（互斥／开关未开／加给自己／超上限）。
//
// 判据顺序照 java AddSignSupport.validateSubmission：互斥先判（两键同传与开关开不开无关），
// 没点人直接放行其余判据（正常流转档也不要求节点开开关），然后开关、加自己、上限。
//
// 互斥这一条的理由不是语义矛盾（一个管本节点再来一轮、一个管流出后交给谁，逻辑可共存），
// 而是**参数生命周期**：tf_* 键只在当次请求的执行参数与已完成任务行变量里可读，加签使令牌
// 停留本节点、流出发生在后续请求，届时先传的 tf_nextNodeOperator 已不可读——同传时它是死参数。
// 静默择一＝用户选的"下一节点处理人"无声消失，报错是把它变响话（spec 06 §2.7）。
func validateAddSignSubmission(node *model.FlowNode, task *model.ProcessTask, args map[string]interface{}, operator string) error {
	if len(args) == 0 || task == nil {
		return nil
	}
	ops := addSignOperatorsFrom(args)
	nextPresent := false
	if v, ok := args[KeyNextNodeOperator]; ok && v != nil {
		nextPresent = strings.TrimSpace(fmt.Sprintf("%v", v)) != ""
	}
	if len(ops) > 0 && nextPresent {
		return errors.New(MsgAddSignMutex)
	}
	if len(ops) == 0 {
		// 没点人＝正常流转档，其余判据不涉及（也不要求节点开开关）
		return nil
	}
	var (
		nodeID = task.TaskName
		allow  bool
		err    error
	)
	if node != nil {
		nodeID = node.ID
		allow, err = parseAllowAddSign(node.Properties[PropAllowAddSign], node.ID)
		if err != nil {
			return err
		}
	}
	if node == nil || node.Type != model.TypeTask || !allow {
		return fmt.Errorf(MsgAddSignSwitchOff, nodeID)
	}
	me := strings.TrimSpace(operator)
	for _, actor := range ops {
		if me != "" && me == actor {
			return errors.New(MsgAddSignSelf)
		}
	}
	maxRounds, err := parseAddSignMaxRounds(node.Properties[PropAddSignMaxRounds], node.ID)
	if err != nil {
		return err
	}
	if maxRounds > 0 && addSignRoundOf(task, node.ID)+1 > maxRounds {
		return fmt.Errorf(MsgAddSignMaxRounds, node.ID, maxRounds)
	}
	return nil
}

// ─── 推进闸与建单 ─────────────────────────────────────────────────────────────

// execAddSignGate 后加签档的推进闸（java TaskModel.exec 的三档闸 + AddSignHandler.handle 合并体）。
// merged 判据（**顺序不可反**，spec 04）：
//
//	本次给了 addSignOperators → 在本节点建单，merged=false，不流转
//	没给但本节点还有未完成待办 → merged=false，不流转
//	两者皆否 → merged=true，由调用方放行既有出边
//
// 停止条件即本判据本身，**不另设第二判据源**：被加签人觉得够了、直接同意不点人，令牌即流出本节点。
// 多选时同一圈建多行，只有最后一个办结且没再点人才 merged=true。
//
// 报错类判据不在此（validateAddSignSubmission 已改动状态之前拦下）——进到这里的提交都已判定合法。
//
// 建单逐条照串行会签续建原语的形状（spec 04「建单规则」七条）：parent＝刚办结那条、
// taskType/form 继承节点、performType=NORMAL、轮次记任务变量、expire_time 同尺、
// 拦截器/事件/通知一律走既有腿（saveNewTask 之后 fire 码 3）、委托走 SurrogateInterceptor
// 同一条腿（saveNewTask 内 applySurrogateToTask，不摘原人）。
func (e *EngineImpl) execAddSignGate(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance,
	node *model.FlowNode, task *model.ProcessTask, operator string, ops []string) (bool, error) {
	nodeID := node.ID
	if len(ops) == 0 {
		doing, err := e.repo.FindDoingTasks(ctx, inst.ID, []string{nodeID})
		if err != nil {
			return false, err
		}
		// 走仓储读而不是聚合副本：与并行会签那一档"还有 doing 就停留"同一把尺子，
		// 且并发腿里另一支刚建的新行只有读回值能看到。
		return len(doing) == 0, nil
	}

	round := addSignRoundOf(task, nodeID) + 1
	now := time.Now()
	form := formKeyOf(node)
	expr := expireExprOf(node)
	isFirst := e.isFirstTaskNode(flow, node) // 加签节点不可能是首任务节点（解析期已拦），沿用现成判据算，不写死 false
	pn := e.surrogateProcessName(ctx, flow, inst)
	for _, actor := range ops {
		// 每个加签人一行（java 逐 actor createTask(singletonList(actor)) 同形）：
		// 兄弟待办各自办结，最后一位办结且不再点人才放行。
		nt := inst.CreateTaskWithActors(e.nextID(), nodeID, node.Text.Value, []string{actor},
			operator, form, now, task.ID, isFirst)
		putTaskVars(nt, map[string]interface{}{prefixKey(VarAddSignRound, nodeID): round})
		applyExpireTime(nt, expr, inst.Variables)
		// 分支归属与每一次普通建单同一条腿：并行范围内的加签任务必须继承父任务归属，
		// 否则汇合判据读不到这一支（缺省按父任务血缘＋模型静态范围推导）。
		e.attachBranchCtx(flow, inst, nt, nodeID, task.ID)
		if err := e.saveNewTask(ctx, nt, pn); err != nil {
			return false, err
		}
		e.fireEvent(ctx, ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID, TaskID: nt.ID,
			NodeID: nodeID, Operator: operator, Actors: nt.ActorIDs})
	}
	appendAddSignHistory(task, operator, ops, round, time.Now())
	// 账本写在已完成那条行上，而 prepareExecuteTask 已经把该行 update 过一次 ⇒ 必须再落一次，
	// 否则内存测试绿、真库里那一格永远没有 history（issues/142 §6.2 第 1 条同族病灶）。
	if err := e.repo.UpdateTask(ctx, task); err != nil {
		return false, err
	}
	syncTaskToAggregate(inst, task)
	return false, nil
}

// ─── 轮次与账本读写 ───────────────────────────────────────────────────────────

// addSignRoundOf 读轮次号：首圈（普通建单产生的那条）没有该键 ⇒ 0。
// 值经仓储 JSON 往返会是 float64，走 toIntOf 兼容 int/int64/float64 三档。
func addSignRoundOf(task *model.ProcessTask, nodeID string) int {
	if task == nil || task.Variables == nil {
		return 0
	}
	v, ok := task.Variables[prefixKey(VarAddSignRound, nodeID)]
	if !ok {
		return 0
	}
	if n := toIntOf(v); n > 0 {
		return n
	}
	return 0
}

// readAddSignHistory 读账本（测试与投影用）。
// 内存里是 []map[string]interface{}，经仓储 JSON 往返是 []interface{}——两形都收，
// 与 tf_transferHistory 那族同一条读取腿的形状。
func readAddSignHistory(task *model.ProcessTask) []map[string]interface{} {
	if task == nil || task.Variables == nil {
		return nil
	}
	switch v := task.Variables[KeyAddSignHistory].(type) {
	case []map[string]interface{}:
		return v
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// appendAddSignHistory 账本追加（只追加不覆盖），落已完成任务行变量。
// 五键固定 camelCase（round/fromActor/toActors/time/operator），time 一律全生态格式串，
// 形状照 tf_transferHistory（spec 06 §2.7）——账本跨栈同形才审得动。
func appendAddSignHistory(task *model.ProcessTask, operator string, toActors []string, round int, now time.Time) {
	if task == nil {
		return
	}
	if task.Variables == nil {
		task.Variables = map[string]interface{}{}
	}
	history := append(readAddSignHistory(task), map[string]interface{}{
		"round":     round,
		"fromActor": operator,
		"toActors":  append([]string{}, toActors...),
		"time":      now.Format(addSignTimeLayout),
		"operator":  operator,
	})
	task.Variables[KeyAddSignHistory] = history
}

// ─── 废弃未完成加签任务（拒绝／退回发起人） ───────────────────────────────────

// abandonAddSignOnNode 废弃本节点未完成的加签任务，不留孤儿待办（spec 04 语义边界表 2/3 行）。
// 只对开了 allowAddSign 的节点动手——会签节点的兄弟任务由既有会签腿管，不在此扩面。
// 持久化形状逐字照会签 merged 后那段废弃腿（engine_impl.go 现读）：逐条 UpdateTask ＋
// syncTaskToAggregate，否则内存里废弃了、真库那一行仍是 DOING。
func (e *EngineImpl) abandonAddSignOnNode(ctx context.Context, flow *model.FlowModel,
	inst *model.ProcessInstance, nodeID, operator string) error {
	if inst == nil || nodeID == "" || flow == nil {
		return nil
	}
	node := findNode(flow, nodeID)
	if node == nil || node.Type != model.TypeTask {
		return nil
	}
	allow, err := parseAllowAddSign(node.Properties[PropAllowAddSign], node.ID)
	if err != nil {
		return err
	}
	if !allow {
		return nil
	}
	remaining, err := e.repo.FindDoingTasks(ctx, inst.ID, []string{nodeID})
	if err != nil {
		return err
	}
	now := time.Now()
	for _, t := range remaining {
		t.Abandon(now)
		if err := e.repo.UpdateTask(ctx, t); err != nil {
			return err
		}
		syncTaskToAggregate(inst, t)
	}
	return nil
}
