// 门面第 **47** 个 action `processTask/removeTaskActor`（issues/115 残留 · Go 栈腿）。
//
// 契约逐字依据＝ jeeflow-doc spec 06-facade.md §processTask/removeTaskActor（八条语义＋守卫次序），
// 判据基准＝ jeeflow-java `RemoveTaskActorActionTest`（17 格逐格对照）。
//
// 它填的是 SPI 与门面之间那段空档：`spi.ProcessRepository.RemoveTaskActor` 从第一天起就是必选方法、
// 两仓都实现，但门面没有对应 action，摘人只能靠 `processTask/transfer`（摘 A **并**加 B）。
//
// 三个兄弟 action 的分工是本文件的判据主线：`surrogate`/`addCandidate` 只加、`transfer` 换人＋留痕、
// 本 action **只摘不加零留痕**（不写任务变量、不覆写任务 actor_id/update_user/update_time、
// **不 fire 事件**——issues/132 §11.3 定稿的事件集没有"摘人"码，码 7 的语义是"参与者被替换"）。
// 每条负向都同时断言"参与者一动不动"：摘人是删除操作，报错却删了一半比报错更糟。
//
// `TestR115WhitespacePaddedIdsAreRemovedAndDirtyRowsSurvive` 等三格对应门禁新格
// 「带空格入参可删 ∧ 空值不误删 `actor_id` 空串行 ∧ DELETE 拿的是行上的原值」：本栈内存仓写侧
// （`memory.Repository.AddTaskActor`）会归一，正常路径建不出空串/未 trim 行 ⇒ 用 `r115SpyRepo`
// （继承 `memory.Repository` 的 spy）把脏行从外部塞进来，并记录每次喂进 DELETE 的实参。
package facade_test

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 夹具 ──────────────────────────────────────────────────────────────────────

// r115SpyRepo 复刻"库里已经存在的历史脏行"与 `DELETE ... actor_id IN (?)` 的逐字语义：
// 脏行只能从外部塞进来（内存仓写侧归一后建不出来）。`FindTaskActors` 把真人那一半与脏行并起来
// 返回（与 JDBC 一条裸 SELECT 同形——脏行本来就会被读出来），`RemoveTaskActor` 记录每次喂进
// DELETE 的实参，并按 `IN` 的精确等值命中删除（**不 trim、不归一**，同 SQL 那句 WHERE）。
type r115SpyRepo struct {
	*memory.Repository
	mu          sync.Mutex
	dirty       map[int64][]string
	removeCalls [][]string
}

var _ spi.ProcessRepository = (*r115SpyRepo)(nil)

func newR115SpyRepo() *r115SpyRepo {
	return &r115SpyRepo{Repository: memory.New(), dirty: map[int64][]string{}}
}

func (s *r115SpyRepo) seedDirtyRow(taskID int64, actorID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[taskID] = append(s.dirty[taskID], actorID)
}

// dirtyRemaining 脏行清单的当前读数（取证用，返回副本）。
func (s *r115SpyRepo) dirtyRemaining(taskID int64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.dirty[taskID]...)
}

// findRealActors 只取"真人"那一半（脏行不进这个读数，否则"其余参与人原样保留"那种判据会被脏行干扰）。
func (s *r115SpyRepo) findRealActors(taskID int64) []string {
	out, _ := s.Repository.FindTaskActors(context.Background(), taskID)
	return out
}

func (s *r115SpyRepo) FindTaskActors(ctx context.Context, taskID int64) ([]string, error) {
	real, err := s.Repository.FindTaskActors(ctx, taskID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(append([]string{}, real...), s.dirty[taskID]...), nil
}

func (s *r115SpyRepo) RemoveTaskActor(ctx context.Context, taskID int64, actors []string) error {
	s.mu.Lock()
	s.removeCalls = append(s.removeCalls, append([]string{}, actors...))
	if rows := s.dirty[taskID]; len(rows) > 0 {
		drop := make(map[string]bool, len(actors))
		for _, a := range actors {
			drop[a] = true
		}
		var kept []string
		for _, r := range rows {
			if !drop[r] {
				kept = append(kept, r)
			}
		}
		s.dirty[taskID] = kept
	}
	s.mu.Unlock()
	return s.Repository.RemoveTaskActor(ctx, taskID, actors)
}

// lastRemoveCall 最近一次喂进 DELETE 的实参（判据打在"删除腿拿的是哪个值"上）。
func (s *r115SpyRepo) lastRemoveCall(t *testing.T) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.removeCalls) == 0 {
		t.Fatalf("夹具前置：应有至少一次 RemoveTaskActor 调用")
	}
	return append([]string{}, s.removeCalls[len(s.removeCalls)-1]...)
}

// r115Recorder 事件录制钩子（本栈既有口径：`engine.Extensions.Listeners`）。
type r115Recorder struct {
	mu     sync.Mutex
	events []engine.ProcessEvent
}

func (r *r115Recorder) add(evt engine.ProcessEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
}

func (r *r115Recorder) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

func (r *r115Recorder) snapshot() []engine.ProcessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]engine.ProcessEvent{}, r.events...)
}

// r115Env 门面 + spy 仓储 + 事件录制 + 一条停在 task1（参与者＝leader）的实例。
type r115Env struct {
	f      *facade.Facade
	repo   *r115SpyRepo
	events *r115Recorder
	instID int64
	taskID int64
}

// r115Setup 部署 01-simple（start → apply[applicant] → task1[leader] → end）→ startAndExecute
// ⇒ apply 自动办结、停在 task1。task1 的参与者就是 "leader" 一人（多参与者现场一律用兄弟
// action `addCandidate` 造，不直接塞仓储）。
func r115Setup(t *testing.T) *r115Env {
	t.Helper()
	repo := newR115SpyRepo()
	extRepo := memory.NewExt()
	rec := &r115Recorder{}
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, &testExprEval{})
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{func(evt engine.ProcessEvent) { rec.add(evt) }},
	})
	f := facade.New(eng, repo, extRepo)
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	defID := r0["data"].(map[string]interface{})["processDefineId"]
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defID, "operator": "zhangsan",
	})
	mustOk(t, r1)
	instID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskID := doingTaskID(t, repo.Repository, instID, "task1")
	if taskID == 0 {
		t.Fatalf("夹具前置：应有 task1 进行中任务")
	}
	rec.clear() // 建流/发起那一段的事件与本次判据无关，从干净读数开始算
	return &r115Env{f: f, repo: repo, events: rec, instID: instID, taskID: taskID}
}

// addActors 加签成人手（用兄弟 action 造多参与者现场）。
func (e *r115Env) addActors(t *testing.T, actors ...string) {
	t.Helper()
	r := e.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": e.taskID, "actorIds": actors,
	})
	mustOk(t, r)
}

// remove 走门面（三个入参都按 interface{} 传，便于塞 nil / 空串这些负向形状）。
func (e *r115Env) remove(taskID interface{}, actorIDs interface{}, operator interface{}) map[string]interface{} {
	return e.f.Flow("processTask/removeTaskActor", map[string]interface{}{
		"processTaskId": taskID, "actorIds": actorIDs, "operator": operator,
	})
}

func (e *r115Env) actors(t *testing.T) []string {
	t.Helper()
	out, err := e.repo.FindTaskActors(context.Background(), e.taskID)
	if err != nil {
		t.Fatalf("读参与者: %v", err)
	}
	return out
}

// finishTask1 办结 task1 ⇒ 该任务离开 DOING（"历史任务"那一档的夹具）。
func (e *r115Env) finishTask1(t *testing.T) {
	t.Helper()
	mustOk(t, e.f.Flow("processTask/execute", map[string]interface{}{
		"processTaskId": e.taskID, "operator": "leader", "submitType": 1,
	}))
	if tk, _ := e.repo.FindTaskByID(context.Background(), e.taskID); tk.TaskState == model.TaskStateDoing {
		t.Fatalf("夹具前置：task1 应已离开 DOING，实得 %v", tk.TaskState)
	}
}

func r115MustOk(t *testing.T, label string, r map[string]interface{}) {
	t.Helper()
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("%s 应成功（code=0 + msg=成功），实得 %v", label, r)
	}
	if msg, _ := r["msg"].(string); msg != "成功" {
		t.Fatalf("%s 成功信封 msg 应逐字为「成功」，实得 %v", label, r)
	}
}

// r115MustMsg 断言失败信封的 msg **逐字相等**（spec 同节：八栈文案一模一样，
// 本栈既有的 mustFailWithMsg 只做 Contains，照不住"多带后缀"这种分叉）。
func r115MustMsg(t *testing.T, label string, r map[string]interface{}, want string) {
	t.Helper()
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s 应失败且 code=99999999（禁止静默成功），实得 %v", label, r)
	}
	if msg, _ := r["msg"].(string); msg != want {
		t.Fatalf("%s msg = %q, want 逐字「%s」", label, msg, want)
	}
}

func r115CopyVars(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ─── 语义 1「只摘不加」＋ 正向核心 ──────────────────────────────────────────────

// 摘掉点名的人，其余参与人按顺序原样保留；data 出 null。
func TestR115RemovesOnlyTheNamedActorAndKeepsTheRest(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001", "9002")
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader", "9001", "9002"}) {
		t.Fatalf("夹具前置：参与者应 = [leader 9001 9002]，实得 %q", got)
	}

	r := e.remove(e.taskID, []string{"9001"}, "flow.admin")

	r115MustOk(t, "只摘点名的人", r)
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader", "9002"}) {
		t.Fatalf("只删点名的 9001，其余参与人原样保留（含顺序），实得 %q", got)
	}
	if r["data"] != nil {
		t.Fatalf("data 应出 null（spec 同节：前端消费面不读 data），实得 %v", r["data"])
	}
}

// 一次摘多人（集合语义，不是"一次只能摘一个人"）。
func TestR115RemovesSeveralActorsInOneCall(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001", "9002", "9003")

	r115MustOk(t, "一次摘多人", e.remove(e.taskID, []string{"9001", "9002"}, "flow.admin"))

	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader", "9003"}) {
		t.Fatalf("实得 %q want [leader 9003]", got)
	}
}

// 逗号串腿与数组腿同判据（§2.11 第 1 行「两形一把尺子」，摘人腿不得另抄一份）。
func TestR115CommaStringShapeRemovesTheSamePeople(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001", "9002")

	r115MustOk(t, "逗号串腿", e.remove(e.taskID, "9001, 9002 ", "flow.admin"))

	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("逗号串带空格照样命中，实得 %q want [leader]", got)
	}
}

// ─── 语义 3「归属判据同 transfer」 ─────────────────────────────────────────────

// 本人摘自己的那一票：无需特权。
func TestR115SelfRemovalNeedsNoPrivilege(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")

	r115MustOk(t, "本人摘自己", e.remove(e.taskID, []string{"9001"}, "9001"))

	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("实得 %q want [leader]", got)
	}
}

// 借道摘他人必须拦下，且参与者一动不动（transfer 能"摘 A 加 B"是因为 A＝操作人本人）。
func TestR115RemovingSomeoneElseWithoutPrivilegeIsRejected(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")
	before := append([]string{}, e.actors(t)...)

	r := e.remove(e.taskID, []string{"9001"}, "leader")

	r115MustMsg(t, "借道摘他人", r, "无权限摘除该任务参与人")
	if got := e.actors(t); !reflect.DeepEqual(got, before) {
		t.Fatalf("报错后一条都不许删：before=%q after=%q", before, got)
	}
}

// flow.auto 与 flow.admin 同档放行（沿用本栈既有 strings.EqualFold 特权判据）。
func TestR115AutoSystemOperatorIsAlsoPrivileged(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")

	r115MustOk(t, "flow.auto 代摘", e.remove(e.taskID, []string{"9001"}, "flow.auto"))

	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("实得 %q want [leader]", got)
	}
	// 哨兵大小写不敏感（沿用本栈既有 strings.EqualFold 写法）
	r115MustOk(t, "FLOW.ADMIN 同档放行",
		e.remove(e.taskID, []string{"nobody-here"}, "FLOW.ADMIN"))
}

// ─── 语义 5「不得摘空」：判据是集合差，不是入参条数 ─────────────────────────────

func TestR115NeverEmptiesTheTask(t *testing.T) {
	e := r115Setup(t)
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("夹具前置：task1 参与者应只剩 leader，实得 %q", got)
	}

	r := e.remove(e.taskID, []string{"leader"}, "leader")

	r115MustMsg(t, "摘空", r, "至少需保留一名参与人")
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("摘空会造出无人可办又无法重派的死单，人必须还在，实得 %q", got)
	}
}

// 绕过档：actorIds 里混进非参与者 id，"入参条数 < 参与人数"这种判据会放过去，
// 集合差判据必须照样拦下（spec 语义 5 的第二句）。
func TestR115MixedNonParticipantIdCannotBypassTheKeepOneFloor(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")

	r := e.remove(e.taskID, []string{"leader", "9001", "ghost"}, "leader")

	r115MustMsg(t, "混入非参与者 id 绕过下限", r, "至少需保留一名参与人")
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader", "9001"}) {
		t.Fatalf("实得 %q want [leader 9001]", got)
	}
}

// ─── 语义 4「只作用于进行中任务」 ───────────────────────────────────────────────

// 历史参与人行是审批链的取证依据（approvalRecord 读全状态任务行），非 DOING 一律拦下。
func TestR115FinishedTaskIsProtected(t *testing.T) {
	e := r115Setup(t)
	e.finishTask1(t)
	before := append([]string{}, e.actors(t)...)

	r := e.remove(e.taskID, []string{"leader"}, "flow.admin")

	r115MustMsg(t, "已办结任务摘人", r, "任务非进行中，不可摘除参与人")
	if got := e.actors(t); !reflect.DeepEqual(got, before) {
		t.Fatalf("已办结任务的参与人行不得被改写历史：before=%q after=%q", before, got)
	}
}

// ─── 语义 2「不留痕、不 fire 事件」 ─────────────────────────────────────────────

func TestR115LeavesNoTraceAndFiresNoEvent(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")
	// 任务行留痕列的"改前读数"：建单路径本来就会写 update_user/update_time（不是摘人写的），
	// 判据只能是"摘人这一步没动它"，**不能假定它本来是零值**。
	before := mustTaskRow(t, e.repo.Repository, e.taskID)
	varsBefore := r115CopyVars(before.Variables)
	updateUserBefore := before.UpdateUser
	updateTimeBefore := before.UpdateTime
	e.events.clear()

	r115MustOk(t, "摘人", e.remove(e.taskID, []string{"9001"}, "flow.admin"))

	if got := e.events.snapshot(); len(got) != 0 {
		names := make([]string, 0, len(got))
		for _, evt := range got {
			names = append(names, evt.Type.SpecName())
		}
		t.Fatalf("摘人不在 132 定稿事件集里，一律不 fire（码 7 的语义是「参与者被替换」）: %v", names)
	}
	after := mustTaskRow(t, e.repo.Repository, e.taskID)
	for _, key := range []string{"submitType", "tf_transferHistory", "tf_transferTo"} {
		if v, ok := after.Variables[key]; ok {
			t.Fatalf("不留痕：任务变量里不得出现 %s，实得 %v", key, v)
		}
	}
	if !reflect.DeepEqual(r115CopyVars(after.Variables), varsBefore) {
		t.Fatalf("不写任何任务变量：before=%v after=%v", varsBefore, after.Variables)
	}
	if after.UpdateUser != updateUserBefore {
		t.Fatalf("不覆写任务留痕列 update_user: before=%q after=%q", updateUserBefore, after.UpdateUser)
	}
	if !after.UpdateTime.Equal(updateTimeBefore) {
		t.Fatalf("不覆写任务留痕列 update_time: before=%v after=%v", updateTimeBefore, after.UpdateTime)
	}
	if after.ActorID != "" {
		t.Fatalf("不覆写任务 actor_id 列: %q", after.ActorID)
	}
}

// ─── 语义 7「幂等」：非参与者静默忽略，重放第二次仍成功 ─────────────────────────

func TestR115RemovingANonParticipantIsIdempotent(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001")

	r115MustOk(t, "首次摘人", e.remove(e.taskID, []string{"9001"}, "flow.admin"))
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("实得 %q want [leader]", got)
	}

	r115MustOk(t, "同一次摘人重放第二次应得成功信封（前端双点/集成层重放）",
		e.remove(e.taskID, []string{"9001"}, "flow.admin"))
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("重放不得再有副作用，实得 %q", got)
	}
	// 全程零事件（幂等腿也一样不 fire）
	if got := len(e.events.snapshot()); got != 0 {
		t.Fatalf("摘人一条事件都不该发，实得 %d 条", got)
	}
}

// ─── 必填档（逐字文案）与守卫次序 ───────────────────────────────────────────────

// 三档逐字文案：operator 缺省/纯空白 ⇒ `operator 必填`（严禁回落 user1）；主键缺失或 actorIds
// 丢完为空 ⇒ 与 surrogate 同一文案 `processTaskId/actorIds 缺失`；任务不存在 ⇒ `任务不存在`。
func TestR115MissingArmsReuseTheExistingErrorEnvelope(t *testing.T) {
	e := r115Setup(t)
	// 取证快照必须是副本：仓储返回的是内部行的拷贝，但下面的判据要跨五次调用比对比对。
	before := append([]string{}, e.actors(t)...)

	r115MustMsg(t, "缺省 operator ⇒ 必填档（严禁回落 user1）",
		e.remove(e.taskID, []string{"leader"}, nil), "operator 必填")
	r115MustMsg(t, "纯空白 operator 也不给过",
		e.remove(e.taskID, []string{"leader"}, "   "), "operator 必填")
	r115MustMsg(t, "主键空串 ⇒ 兄弟 action 同文案",
		e.remove("", []string{"9001"}, "flow.admin"), "processTaskId/actorIds 缺失")
	// 主键纯空白 / 0 / 负数 ⇒ 同一档（spec 语义 8：这四种形状都是"调用方没给对 id"，
	// 不得改口成「任务不存在」；taskIDArg 对它们本来就响亮报错，这里只统一 msg）
	r115MustMsg(t, "主键纯空白 ⇒ 缺参数档",
		e.remove("   ", []string{"9001"}, "flow.admin"), "processTaskId/actorIds 缺失")
	r115MustMsg(t, "主键 0 ⇒ 缺参数档（不拿 0 当 id 去查）",
		e.remove(int64(0), []string{"9001"}, "flow.admin"), "processTaskId/actorIds 缺失")
	r115MustMsg(t, "主键负数 ⇒ 缺参数档",
		e.remove(int64(-1), []string{"9001"}, "flow.admin"), "processTaskId/actorIds 缺失")
	// 唯一**不**折进缺参数档的一腿：超 2^53 的 float 是"精度已经丢了"另一种事实，必须原样透出
	// 精度护栏（java toLong / node toId / python _to_int 都留着这条，issues/38 E9·82）；
	// 伪装成"参数没传"，调用方就看不出自己传的是浮点雪花 id。
	// ⚠️ 取值要用**明显大于** 2^53 的量：9.007199254740993e15 在 float64 里就舍回 2^53 本身，
	// 护栏判据是 `> 1<<53`，用它会得到"没超精度"的假象（本栈 toInt64 实测同 java/node/python）。
	prec := e.remove(9.3e15, []string{"9001"}, "flow.admin")
	if msg, _ := prec["msg"].(string); !strings.Contains(msg, "超出 float64 精确范围") {
		t.Fatalf("超精度 float 应原样透出精度护栏，实得 %v", prec)
	}
	r115MustMsg(t, "actorIds 丢完为空 ⇒ 兄弟 action 同文案",
		e.remove(e.taskID, []interface{}{"", "  ", nil}, "flow.admin"), "processTaskId/actorIds 缺失")
	r115MustMsg(t, "任务不存在",
		e.remove(int64(424242), []string{"9001"}, "flow.admin"), "任务不存在")

	if got := e.actors(t); !reflect.DeepEqual(got, before) {
		t.Fatalf("五个报错档一条都不许删：before=%q after=%q", before, got)
	}
}

// 守卫次序（spec 同节末尾那段，逐栈一致，不接受本栈自行排序）：
// `operator 必填` 排在缺参数之前——否则"参数全缺"会先报主键缺失，把鉴权缺口藏进参数报错里；
// 权限档排在 DOING 档之前——否则外人可以靠"任务已完成"探到别人的任务状态。
func TestR115GuardOrderIsFixedAcrossStacks(t *testing.T) {
	e := r115Setup(t)

	r115MustMsg(t, "operator 必填排在主键缺失档之前",
		e.remove("", []string{"9001"}, nil), "operator 必填")

	e.finishTask1(t) // task1 已非 DOING，operator 又不是参与者
	r115MustMsg(t, "权限档先于非进行中档",
		e.remove(e.taskID, []string{"leader"}, "outsider"), "无权限摘除该任务参与人")
}

// ─── 门禁新格：带空格入参可删 ∧ 空值不误删 actor_id='' 脏行 ─────────────────────

// `" 9001 "` 必须命中库里的人（硬要求②「落库与比较一律取 trim 后的值」）；同时喂进 DELETE 的
// 实参永不能含空串/纯空白——历史 `actor_id` 空串行是 `DELETE ... actor_id IN (?)` 的受害者，
// 判据打在实参与脏行存活两处。
func TestR115WhitespacePaddedIdsAreRemovedAndDirtyRowsSurvive(t *testing.T) {
	e := r115Setup(t)
	e.addActors(t, "9001", "9002")
	e.repo.seedDirtyRow(e.taskID, "")    // 复刻历史脏行（内存仓写侧归一后建不出来）
	e.repo.seedDirtyRow(e.taskID, "   ") // 纯空白那一支也算脏行

	r115MustOk(t, "带空格入参 + 空值混喂", e.remove(e.taskID,
		[]interface{}{" 9001 ", "", nil, "   ", "9002"}, "flow.admin"))

	if got := e.repo.findRealActors(e.taskID); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("带空格的入参删得掉真人，其余参与人不动，实得 %q want [leader]", got)
	}
	if got := e.repo.dirtyRemaining(e.taskID); !reflect.DeepEqual(got, []string{"", "   "}) {
		t.Fatalf("空串/纯空白绝不能喂进 DELETE ⇒ 历史脏行必须原样还在，实得 %q", got)
	}
	e.repo.mu.Lock()
	calls := append([][]string{}, e.repo.removeCalls...)
	e.repo.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("只该有一次 DELETE 调用，实得 %d 次: %q", len(calls), calls)
	}
	for _, call := range calls {
		for _, actor := range call {
			if strings.TrimSpace(actor) == "" {
				t.Fatalf("喂给 DELETE 的实参不得含空串/纯空白: %q", call)
			}
		}
	}
}

// ─── 语义 6「匹配取归一值、DELETE 取行上的原值」＋语义 5「脏行不算一个人」 ───────

// 库里的行是修复前落下的未 trim 原值 `" 9101 "`，入参给 `"9101"`：判据必须把它当成同一个人
// **并真删掉**，且喂进 DELETE 的实参是**那一行的原值**。
// 反面形状＝拿归一值去删：判成同一人却一条没删，门面报成功而被摘的人待办还在（**假成功**）。
func TestR115UntrimmedHistoricalRowIsMatchedAndDeletedByRowValue(t *testing.T) {
	e := r115Setup(t)
	e.repo.seedDirtyRow(e.taskID, " 9101 ") // 历史未 trim 行（写侧归一后正常路径造不出来）

	r115MustOk(t, "归一匹配未 trim 历史行", e.remove(e.taskID, []string{"9101"}, "flow.admin"))

	if got := e.repo.findRealActors(e.taskID); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("真人那一半不许被动，实得 %q", got)
	}
	if got := e.repo.dirtyRemaining(e.taskID); len(got) != 0 {
		t.Fatalf("未 trim 的历史行应被归一匹配命中并删除，脏行清单实得 %q", got)
	}
	if last := e.repo.lastRemoveCall(t); !reflect.DeepEqual(last, []string{" 9101 "}) {
		t.Fatalf("DELETE 的实参是行上的原值，不是归一后的值（否则删不掉）: %q", last)
	}
}

// 「至少剩一人」的下限按**能办单的人数**算：库里只剩 `actor_id` 空串行时，摘走最后一个真人
// 必须报错——脏行谁也办不了，拿它撑住下限等于让"摘空"伪装成成功。
func TestR115DirtyRowsDoNotPropUpTheKeepOneFloor(t *testing.T) {
	e := r115Setup(t)
	e.repo.seedDirtyRow(e.taskID, "")
	e.repo.seedDirtyRow(e.taskID, "   ")

	r := e.remove(e.taskID, []string{"leader"}, "flow.admin")

	r115MustMsg(t, "脏行不算一个人", r, "至少需保留一名参与人")
	if got := e.repo.findRealActors(e.taskID); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("报错后真人那行还在，实得 %q", got)
	}
}

// ─── 回归：三个兄弟 action 的分工不被稀释 ───────────────────────────────────────

func TestR115SiblingActionsKeepTheirOwnSemantics(t *testing.T) {
	e := r115Setup(t)

	// surrogate 仍旧只加不摘
	r115MustOk(t, "surrogate 只加", e.f.Flow("processTask/surrogate", map[string]interface{}{
		"processTaskId": e.taskID, "actorIds": []string{"9101"},
	}))
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader", "9101"}) {
		t.Fatalf("surrogate 加签后参与人 = %q want [leader 9101]", got)
	}

	// 摘人不带加人
	r115MustOk(t, "removeTaskActor 只摘", e.remove(e.taskID, []string{"9101"}, "flow.admin"))
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"leader"}) {
		t.Fatalf("摘人后参与人 = %q want [leader]", got)
	}

	// transfer 换人语义不变：摘 A 加 B ＋ submitType=7 留痕照写（本 action 不碰那条腿）
	e.events.clear()
	r115MustOk(t, "transfer 换人", e.f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": e.taskID, "operator": "leader", "fromActor": "leader", "toActor": "boss",
	}))
	if got := e.actors(t); !reflect.DeepEqual(got, []string{"boss"}) {
		t.Fatalf("transfer 换人后参与人 = %q want [boss]", got)
	}
	tk := mustTaskRow(t, e.repo.Repository, e.taskID)
	if got := varNum(tk.Variables["submitType"]); got != int64(model.SubmitTypeTransfer) {
		t.Fatalf("transfer 仍写 submitType=7 留痕，实得 %d", got)
	}
	if got := e.events.snapshot(); len(got) == 0 {
		t.Fatalf("transfer 仍要 fire TASK_TRANSFER（只有摘人这一条腿不发事件）")
	}
}

// mustTaskRow 按 taskId 取任务行（夹具判据用；缺行直接 Fail 而不是拿 nil 继续跑）。
func mustTaskRow(t *testing.T, repo *memory.Repository, taskID int64) *model.ProcessTask {
	t.Helper()
	tk, err := repo.FindTaskByID(context.Background(), taskID)
	if err != nil || tk == nil {
		t.Fatalf("夹具前置：任务 %d 应存在（err=%v）", taskID, err)
	}
	return tk
}
