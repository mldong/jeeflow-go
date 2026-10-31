package engine

// 「退回修改、重提直返」——分支归属与直返上下文的读写。
//
// 状态落点与 Java 参考实现一致：不新增表/列，全部寄存在任务变量 JSON 的保留键里
// （spec 04-engine-ops「退回修改、重提直返」）。本栈没有独立的模型解析阶段，节点 ID 即
// 任务行的 task_name，因此静态分支分析直接走 FlowModel 的 nodes/edges。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

const (
	// PropReapplyMode 节点属性：退回发起人后的重提方式
	PropReapplyMode = "reapplyMode"
	// VarBranchCtx 任务变量保留键：分支归属栈（外层→内层）
	VarBranchCtx = "_branchCtx"
	// VarReapplyCtx 任务变量保留键：本次退回绑定（只出现在申请待办上）
	VarReapplyCtx = "_reapplyContext"

	reapplyNormal         = "NORMAL"
	reapplyReturnToSource = "RETURN_TO_SOURCE"
)

type branchScope struct {
	Fork  string `json:"fork"`
	Join  string `json:"join"`
	Round int64  `json:"round"`
	Root  int64  `json:"root"`
}

// parseReapplyMode 缺省/空串⇒NORMAL；非法值必须报配置错误，不得静默当缺省。
func parseReapplyMode(node *model.FlowNode) (string, error) {
	raw, _ := stringFromProps(node.Properties, PropReapplyMode)
	v := strings.TrimSpace(raw)
	switch v {
	case "", reapplyNormal:
		return reapplyNormal, nil
	case reapplyReturnToSource:
		return reapplyReturnToSource, nil
	}
	return "", fmt.Errorf("节点[%s]的 reapplyMode 配置非法: %s", node.ID, v)
}

// ─── 模型静态分析 ────────────────────────────────────────────────────────────

// backwardTaskIDs 从节点沿入边回溯到指定分叉之间的任务节点集合（不含 fork 本身）。
// 可穿过嵌套的内层 fork，遇到 stopFork 即止；否则内层分支根会被漏掉。
func backwardTaskIDs(flow *model.FlowModel, nodeID, stopFork string) map[string]bool {
	out := map[string]bool{}
	stack := incomingSources(flow, nodeID)
	visited := map[string]bool{}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == "" || visited[cur] {
			continue
		}
		visited[cur] = true
		n := findNode(flow, cur)
		if n == nil {
			continue
		}
		if n.Type == model.TypeFork && cur == stopFork {
			continue
		}
		if n.Type == model.TypeTask || n.Type == model.TypeCustom {
			out[cur] = true
		}
		stack = append(stack, incomingSources(flow, cur)...)
	}
	return out
}

// findForkForJoin 汇合点对应的分叉节点。不能取"回溯遇到的第一个 fork"——嵌套并行时外层汇合点
// 沿内层分支回溯会先撞到内层 fork。判据：fork 的最近后支配汇合点等于该 join，多个候选取最外层。
func findForkForJoin(flow *model.FlowModel, joinID string) string {
	best := ""
	bestDepth := -1
	for _, n := range flow.Nodes {
		if n.Type != model.TypeFork {
			continue
		}
		if nearestPostDominatingJoin(flow, n.ID) != joinID {
			continue
		}
		if d := maxDistanceToJoin(flow, n.ID, joinID); d > bestDepth {
			bestDepth = d
			best = n.ID
		}
	}
	return best
}

func nearestPostDominatingJoin(flow *model.FlowModel, forkID string) string {
	best := ""
	bestMax := -1
	for _, n := range flow.Nodes {
		if n.Type != model.TypeJoin {
			continue
		}
		if !postDominates(flow, forkID, n.ID) {
			continue
		}
		d := maxDistanceToJoin(flow, forkID, n.ID)
		if d >= 0 && (bestMax < 0 || d < bestMax) {
			bestMax = d
			best = n.ID
		}
	}
	return best
}

// postDominates 屏蔽该汇合点后，fork 的任一分支仍能走到末端即返回 false。
func postDominates(flow *model.FlowModel, forkID, joinID string) bool {
	stack := outgoingTargets(flow, forkID)
	visited := map[string]bool{}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == "" || joinID == cur || visited[cur] {
			continue
		}
		visited[cur] = true
		outs := outgoingTargets(flow, cur)
		if len(outs) == 0 {
			return false
		}
		stack = append(stack, outs...)
	}
	return true
}

// maxDistanceToJoin 分支走到汇合点的最大跳数（外层程度）；不可达返回 -1。
func maxDistanceToJoin(flow *model.FlowModel, forkID, joinID string) int {
	frontier := outgoingTargets(flow, forkID)
	visited := map[string]bool{}
	for _, id := range frontier {
		visited[id] = true
	}
	d := 1
	found := -1
	for len(frontier) > 0 {
		var next []string
		for _, cur := range frontier {
			if cur == joinID {
				found = d
			}
			for _, t := range outgoingTargets(flow, cur) {
				if !visited[t] {
					visited[t] = true
					next = append(next, t)
				}
			}
		}
		frontier = next
		d++
	}
	return found
}

func outgoingTargets(flow *model.FlowModel, nodeID string) []string {
	var out []string
	for _, e := range flow.Edges {
		if e.SourceNodeID == nodeID {
			out = append(out, e.TargetNodeID)
		}
	}
	return out
}

func incomingSources(flow *model.FlowModel, nodeID string) []string {
	var out []string
	for _, e := range flow.Edges {
		if e.TargetNodeID == nodeID {
			out = append(out, e.SourceNodeID)
		}
	}
	return out
}

// staticScopes 节点所在的全部分支范围（外层→内层）。
func staticScopes(flow *model.FlowModel, nodeID string) []branchScope {
	type hit struct {
		scope branchScope
		size  int
	}
	var hits []hit
	for _, n := range flow.Nodes {
		if n.Type != model.TypeJoin {
			continue
		}
		fork := findForkForJoin(flow, n.ID)
		if fork == "" {
			continue
		}
		inside := backwardTaskIDs(flow, n.ID, fork)
		if !inside[nodeID] {
			continue
		}
		hits = append(hits, hit{scope: branchScope{Fork: fork, Join: n.ID}, size: len(inside)})
	}
	// 集合越大越外层
	for i := 0; i < len(hits); i++ {
		for j := i + 1; j < len(hits); j++ {
			if hits[j].size > hits[i].size {
				hits[i], hits[j] = hits[j], hits[i]
			}
		}
	}
	out := make([]branchScope, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.scope)
	}
	return out
}

// buildScopes 新任务的分支归属：逐档继承父任务同档的 round/root；父任务不在该档
// （本任务是该分叉的分支根）时 round=父任务ID、root=本任务ID。
func buildScopes(flow *model.FlowModel, nodeID string, parent *model.ProcessTask, selfID int64) []branchScope {
	statics := staticScopes(flow, nodeID)
	if len(statics) == 0 {
		return nil
	}
	var parentID int64
	var parentScopes []branchScope
	if parent != nil {
		parentID = parent.ID
		parentScopes = scopesOf(parent.Variables)
	}
	out := make([]branchScope, 0, len(statics))
	for _, s := range statics {
		inherited := false
		for _, p := range parentScopes {
			if p.Fork == s.Fork && p.Join == s.Join {
				out = append(out, branchScope{Fork: s.Fork, Join: s.Join, Round: p.Round, Root: p.Root})
				inherited = true
				break
			}
		}
		if !inherited {
			out = append(out, branchScope{Fork: s.Fork, Join: s.Join, Round: parentID, Root: selfID})
		}
	}
	return out
}

// ─── 任务变量读写 ────────────────────────────────────────────────────────────

func scopesOf(vars map[string]interface{}) []branchScope {
	if vars == nil {
		return nil
	}
	raw, ok := vars[VarBranchCtx]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []branchScope:
		return v
	case []map[string]interface{}:
		out := make([]branchScope, 0, len(v))
		for _, m := range v {
			out = append(out, branchScope{
				Fork:  asStr(m["fork"]),
				Join:  asStr(m["join"]),
				Round: asInt64(m["round"]),
				Root:  asInt64(m["root"]),
			})
		}
		return out
	case []interface{}:
		out := make([]branchScope, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, branchScope{
					Fork:  asStr(m["fork"]),
					Join:  asStr(m["join"]),
					Round: asInt64(m["round"]),
					Root:  asInt64(m["root"]),
				})
			}
		}
		return out
	}
	return nil
}

func putScopes(task *model.ProcessTask, scopes []branchScope) {
	if task == nil {
		return
	}
	if scopes == nil {
		scopes = []branchScope{}
	}
	putTaskVars(task, map[string]interface{}{VarBranchCtx: scopes})
}

func reapplyOf(vars map[string]interface{}) map[string]interface{} {
	if vars == nil {
		return nil
	}
	raw, ok := vars[VarReapplyCtx]
	if !ok || raw == nil {
		return nil
	}
	if m, ok := raw.(map[string]interface{}); ok && len(m) > 0 {
		return m
	}
	return nil
}

func branchOf(reapply map[string]interface{}) []branchScope {
	if reapply == nil {
		return nil
	}
	return scopesOf(map[string]interface{}{VarBranchCtx: reapply["branch"]})
}

func findScope(scopes []branchScope, joinID string) *branchScope {
	for i := range scopes {
		if scopes[i].Join == joinID {
			return &scopes[i]
		}
	}
	return nil
}

func newReapplyContext(sourceTaskID int64, sourceNode string, branch []branchScope) map[string]interface{} {
	if branch == nil {
		branch = []branchScope{}
	}
	return map[string]interface{}{
		"mode":         reapplyReturnToSource,
		"sourceTaskId": fmt.Sprintf("%d", sourceTaskID),
		"sourceNode":   sourceNode,
		"branch":       branch,
	}
}

func asStr(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func asInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case int32:
		return int64(x)
	case string:
		var n int64
		fmt.Sscanf(strings.TrimSpace(x), "%d", &n)
		return n
	}
	return 0
}

// stripReserved 客户端不得伪造/覆盖引擎控制键。
func stripReserved(args map[string]interface{}) map[string]interface{} {
	if args == nil {
		return map[string]interface{}{}
	}
	delete(args, VarBranchCtx)
	delete(args, VarReapplyCtx)
	return args
}

// ─── 汇合判据 ────────────────────────────────────────────────────────────────

// mergeReady 汇合是否可放行。有分支归属时按本轮各分支根判定（含申请人在改的等待关系）；
// 无分支归属的存量任务回落旧判据。
func (e *EngineImpl) mergeReady(ctx context.Context, flow *model.FlowModel, inst *model.ProcessInstance,
	joinID string, arrivingTaskID int64) bool {
	var arriving *model.ProcessTask
	for _, t := range inst.Tasks {
		if t.ID == arrivingTaskID {
			arriving = t
			break
		}
	}
	if arriving == nil {
		return e.legacyMergeReady(ctx, inst)
	}
	scope := findScope(scopesOf(arriving.Variables), joinID)
	if scope == nil {
		return e.legacyMergeReady(ctx, inst)
	}
	roots := map[int64]bool{}
	for _, t := range inst.Tasks {
		s := findScope(scopesOf(t.Variables), joinID)
		inRound := s != nil && s.Fork == scope.Fork && s.Round == scope.Round
		if inRound {
			if _, ok := roots[s.Root]; !ok {
				roots[s.Root] = false
			}
		}
		if t.TaskState != model.TaskStateDoing {
			continue
		}
		if inRound {
			roots[s.Root] = true
		}
		// 申请人在分叉之外修改：申请任务不叫分支节点名，必须按等待关系计入
		for _, b := range branchOf(reapplyOf(t.Variables)) {
			if b.Join == joinID && b.Fork == scope.Fork && b.Round == scope.Round {
				roots[b.Root] = true
			}
		}
	}
	if len(roots) == 0 {
		return e.legacyMergeReady(ctx, inst)
	}
	for _, blocked := range roots {
		if blocked {
			return false
		}
	}
	return true
}

func (e *EngineImpl) legacyMergeReady(ctx context.Context, inst *model.ProcessInstance) bool {
	doing, _ := e.repo.FindDoingTasks(ctx, inst.ID, nil)
	return len(doing) == 0
}

// ─── 建单 ────────────────────────────────────────────────────────────────────

// attachBranchCtx 给新任务写入分支归属（父任务按 parentTaskID 在聚合内查找）。
func (e *EngineImpl) attachBranchCtx(flow *model.FlowModel, inst *model.ProcessInstance,
	nt *model.ProcessTask, nodeID string, parentTaskID int64) {
	var parent *model.ProcessTask
	if parentTaskID > 0 {
		for _, t := range inst.Tasks {
			if t.ID == parentTaskID {
				parent = t
				break
			}
		}
	}
	putScopes(nt, buildScopes(flow, nodeID, parent, nt.ID))
}

// createTaskAtNode 在指定任务节点建一条普通待办（不执行该节点的出边）。
func (e *EngineImpl) createTaskAtNode(ctx context.Context, flow *model.FlowModel, node *model.FlowNode,
	inst *model.ProcessInstance, operator string, vars map[string]interface{}, parentTaskID int64,
	scopes []branchScope, reapply map[string]interface{}) (*model.ProcessTask, error) {
	actors := e.resolveActors(node, inst, operator, vars)
	now := time.Now()
	form := formKeyOf(node)
	expr := expireExprOf(node)
	isFirst := e.isFirstTaskNode(flow, node)
	var actor string
	if len(actors) > 0 {
		actor = actors[0]
	}
	nt := inst.CreateTask(e.nextID(), node.ID, node.Text.Value, actor, operator, form, now, parentTaskID, isFirst)
	if len(actors) > 1 {
		nt.ActorIDs = actors
	}
	applyExpireTime(nt, expr, inst.Variables)
	if scopes == nil {
		scopes = []branchScope{}
	}
	putScopes(nt, scopes)
	if reapply != nil {
		putTaskVars(nt, map[string]interface{}{VarReapplyCtx: reapply})
	}
	pn := e.surrogateProcessName(ctx, flow, inst)
	if err := e.saveNewTask(ctx, nt, pn); err != nil {
		return nil, err
	}
	e.fireEvent(ctx, ProcessEvent{Type: EventProcessTaskStart, InstanceID: inst.ID,
		TaskID: nt.ID, NodeID: node.ID, Operator: operator, Actors: nt.ActorIDs})
	return nt, nil
}

// ─── 引擎侧入口 ──────────────────────────────────────────────────────────────

func (e *EngineImpl) loadFlowForTask(ctx context.Context, task *model.ProcessTask) (*model.FlowModel, error) {
	inst, err := e.repo.FindInstanceByID(ctx, task.ProcessInstanceID)
	if err != nil || inst == nil {
		return nil, fmt.Errorf("流程实例不存在")
	}
	def, _ := e.repo.FindDefineByID(ctx, inst.DefineID)
	if def == nil {
		return nil, fmt.Errorf("流程定义不存在")
	}
	var flow model.FlowModel
	if err := json.Unmarshal(def.Content, &flow); err != nil {
		return nil, err
	}
	return &flow, nil
}

func firstTaskNode(flow *model.FlowModel) *model.FlowNode {
	start := findNodeByType(flow, model.TypeStart)
	if start == nil {
		return nil
	}
	for _, n := range followEdges(flow, start.ID) {
		if n.Type == model.TypeTask {
			return n
		}
	}
	return nil
}

// executeReapply submitType=5 且申请待办带直返上下文：校验来源后在来源节点直接建新待办，
// 不执行申请节点出边、不重算来源前条件、不执行来源节点出边。
func (e *EngineImpl) executeReapply(ctx context.Context, applyTask *model.ProcessTask,
	operator string, args map[string]interface{}) (*model.ProcessInstance, error) {
	st := toIntOf(args[KeySubmitType])
	if st != -1 && st != int(model.SubmitTypeReApply) {
		return nil, fmt.Errorf("带重提直返标记的申请待办只接受重新提交(submitType=5)")
	}
	inst, err := e.repo.FindInstanceByID(ctx, applyTask.ProcessInstanceID)
	if err != nil || inst == nil {
		return nil, fmt.Errorf("流程实例不存在")
	}
	flow, err := e.loadFlowForTask(ctx, applyTask)
	if err != nil {
		return nil, err
	}
	rc := reapplyOf(applyTask.Variables)
	sourceID := asInt64(rc["sourceTaskId"])
	var source *model.ProcessTask
	for _, t := range inst.Tasks {
		if t.ID == sourceID {
			source = t
			break
		}
	}
	if source == nil || source.TaskState != model.TaskStateDone ||
		source.ProcessInstanceID != applyTask.ProcessInstanceID {
		return nil, fmt.Errorf("重提直返来源任务无效或未完成")
	}
	sourceNode := findNode(flow, source.TaskName)
	if sourceNode == nil || sourceNode.Type != model.TypeTask {
		return nil, fmt.Errorf("重提直返来源节点配置已变更，拒绝执行")
	}
	if mode, merr := parseReapplyMode(sourceNode); merr != nil {
		return nil, merr
	} else if mode != reapplyReturnToSource {
		return nil, fmt.Errorf("重提直返来源节点配置已变更，拒绝执行")
	}
	if applyNode := findNode(flow, applyTask.TaskName); applyNode == nil || !e.isFirstTaskNode(flow, applyNode) {
		return nil, fmt.Errorf("重提直返必须从申请待办发起")
	}
	_, inst2, _, vars, err := e.prepareExecuteTask(ctx, applyTask.ID, operator, args, nil)
	if err != nil {
		return nil, err
	}
	// 遗留的下一节点指定人参数不得影响本次建单（历史记录不删）
	delete(vars, KeyNextNodeOperator)
	if _, err := e.createTaskAtNode(ctx, flow, sourceNode, inst2, operator, vars, applyTask.ID,
		branchOf(rc), nil); err != nil {
		return nil, err
	}
	out, _ := e.repo.FindInstanceByID(ctx, inst2.ID)
	return out, nil
}

// lockInstanceByTask 同实例互斥；任务不存在时退化为实例 0 的锁（无副作用）。
func (e *EngineImpl) lockInstanceByTask(taskID int64) func() {
	var key int64
	if t, _ := e.repo.FindTaskByID(context.Background(), taskID); t != nil {
		key = t.ProcessInstanceID
	}
	m, _ := e.instLocks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}
