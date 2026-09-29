// 归属值写侧归一（issues/142 B 批 · Go 栈，**漏斗层** · 门面三条腿）。
//
// spec 06-facade.md §2.11 覆盖的写点里门面占三条：
//   - `processTask/addCandidate`、`processTask/surrogate`（本栈同体，都落 taskAddActor）；
//   - `processTask/transfer` 的 fromActor/toActor（归一后再参与删除与插入）；
//   - `processInstance/updateCCStatus` 的 operator（入参归一后再比）。
//     外加 §2.11「主键类参数另判一档」：processTaskId 缺失/空串/0 必须响亮报错。
//
// 普查实读（issues/142 §2 B 表 go 那一行）改前的三把尺子：
//
//	toStringSlice2 —— []string 不 trim 空串照收；[]interface{} 丢 nil 但 "" 照收、不 trim；
//	                 只有逗号串那一支才 TrimSpace＋丢空。
//	两仓 AddTaskActor —— 只判重不判空（写侧兜底见 memory/actor_norm_142_test.go 与
//	                 repository/jdbc/actor_norm_142_sqlite_test.go）。
//
// ⚠️ 反向哨兵按 §2.11④ 进每一格："0" 不得被丢掉。
package facade_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
)

// a142Env 门面 + 仓储 + 一条推到 task1 的实例。
type a142Env struct {
	f      *facade.Facade
	repo   *memory.Repository
	instID int64
	taskID int64
	before []string // task1 此刻的参与者（基线，用于算差集）
}

// a142Setup 部署 01-simple → 发起 → 取 task1 那条 DOING 行。
func a142Setup(t *testing.T, tag string) *a142Env {
	t.Helper()
	f, repo, _ := setupFacade()
	r0 := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	mustOk(t, r0)
	defID := mustDefineID(t, repo, "simple")
	r1 := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defID, "operator": "zhangsan",
	})
	mustOk(t, r1)
	instID := mustI64(r1["data"].(map[string]interface{})["processInstanceId"])
	taskID := doingTaskID(t, repo, instID, "task1")
	if taskID == 0 {
		t.Fatalf("夹具前置（%s）：应有 task1 进行中任务", tag)
	}
	before, err := repo.FindTaskActors(context.Background(), taskID)
	if err != nil {
		t.Fatalf("读基线参与者: %v", err)
	}
	return &a142Env{f: f, repo: repo, instID: instID, taskID: taskID, before: before}
}

// added 返回归一后**真正新增**的那批参与者（去掉基线里已有的）。
func (e *a142Env) added(t *testing.T) []string {
	t.Helper()
	now, err := e.repo.FindTaskActors(context.Background(), e.taskID)
	if err != nil {
		t.Fatalf("读参与者: %v", err)
	}
	var out []string
	for _, a := range now {
		if !containsStr2(e.before, a) {
			out = append(out, a)
		}
	}
	return out
}

func mustAdded142(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
		t.Fatalf("%s：实得 %q want %q", label, got, want)
	}
}

// TestIssue142FacadeAddCandidateArrayFormNormalized 数组形：逐元素 trim、空串/纯空白/nil 丢弃、
// 同次调用折叠。addCandidate 与 surrogate 在本栈同体，两条 action 名各跑一次。
//
// 改前红：[]interface{} 那一支不 trim、"" 照收 ⇒ 实得 [" b1 " "" "   " "<nil>" "b2" "b2"]。
func TestIssue142FacadeAddCandidateArrayFormNormalized(t *testing.T) {
	for _, action := range []string{"processTask/addCandidate", "processTask/surrogate"} {
		e := a142Setup(t, action)
		r := e.f.Flow(action, map[string]interface{}{
			"processTaskId": e.taskID,
			"actorIds":      []interface{}{" b1 ", "", "   ", nil, "b2", "b2"},
		})
		mustOk(t, r)
		mustAdded142(t, action+"：数组形应与串形同判据", e.added(t), []string{"b1", "b2"})
	}
}

// TestIssue142FacadeAddCandidateTwoFormsSameAnswer §2.11「逗号串与数组两形同判据」的正面对拍：
// 同一批人分别用逗号串与数组给 addCandidate，落到参与者表上必须是同一份值。
//
// 改前红：数组形那侧多出一条 ""（且不 trim），两形答案不同 ⇒ 本格直接红在比对上。
func TestIssue142FacadeAddCandidateTwoFormsSameAnswer(t *testing.T) {
	fromString := a142Setup(t, "串形")
	mustOk(t, fromString.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": fromString.taskID, "actorIds": " c1 , , c2 , c1",
	}))
	fromArray := a142Setup(t, "数组形")
	mustOk(t, fromArray.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": fromArray.taskID, "actorIds": []interface{}{" c1 ", "", " c2 ", "c1"},
	}))
	if !reflect.DeepEqual(fromString.added(t), fromArray.added(t)) {
		t.Fatalf("§2.11「两形同判据」分叉：串形 %q vs 数组形 %q",
			fromString.added(t), fromArray.added(t))
	}
	mustAdded142(t, "两形都该是 trim＋丢空＋折叠后的这一份", fromString.added(t), []string{"c1", "c2"})
}

// TestIssue142FacadeAddCandidateKeepsSentinelIds 反向哨兵（门面腿）："0"/"00"/"null"/"a"
// 都是正常归属值且互不相同（严禁 "0"=="00" 那种松散折叠），只有纯空白与 nil 被丢。
//
// 改前红：" " 与 nil 各落一行（前者未 trim 判不出空白，后者串化成 "<nil>"）。
func TestIssue142FacadeAddCandidateKeepsSentinelIds(t *testing.T) {
	e := a142Setup(t, "哨兵")
	mustOk(t, e.f.Flow("processTask/addCandidate", map[string]interface{}{
		"processTaskId": e.taskID, "actorIds": []interface{}{"0", "00", " ", nil, "null", "a"},
	}))
	mustAdded142(t, "§2.11④ 哨兵：\"0\" 不得被丢掉", e.added(t), []string{"0", "00", "null", "a"})
}

// TestIssue142FacadeAddCandidateAllBlankKeepsMissingParamEnvelope 丢完为空 ⇒ 沿用既有
// 「actorIds 缺失」信封（§2.11 硬要求③：不新造错误码/文案），且零副作用。
//
// 改前"半红"：[]string{"", "   "} 两个元素都活着 ⇒ code=0 报成功并落两条空归属值。
func TestIssue142FacadeAddCandidateAllBlankKeepsMissingParamEnvelope(t *testing.T) {
	e := a142Setup(t, "全空白")
	for _, raw := range []interface{}{[]string{"", "   "}, []interface{}{"  ", nil, ""}} {
		r := e.f.Flow("processTask/addCandidate", map[string]interface{}{
			"processTaskId": e.taskID, "actorIds": raw,
		})
		if code, _ := r["code"].(int); code == 0 {
			t.Fatalf("§2.11：归一后为空与\"空 actorIds\"同档，不得报成功，实得 %v", r)
		}
		if msg, _ := r["msg"].(string); !strings.Contains(msg, "actorIds 缺失") {
			t.Fatalf("§2.11③：须沿用既有「actorIds 缺失」文案，实得 msg=%q", msg)
		}
	}
	mustAdded142(t, "报错档必须零副作用", e.added(t), nil)
}

// TestIssue142FacadeTaskPrimaryKeyBlankOrZeroRejected §2.11「主键类参数另判一档」：
// processTaskId 缺失/空串/0 一律响亮报错（沿用既有「processTaskId 缺失或非法」信封），
// 不得拿 空串/0 当 id 往下落库。这条与"归属值为空 ⇒ 丢弃"是两件事。
//
// 改前红：**只有 `0` 这一档红**——toInt64(0) 不报错，改前直接把 process_task_id=0 的
// 参与者行钉进表里（""/缺键那两档改前已响亮报错，本格一并钉住不得回退）。
func TestIssue142FacadeTaskPrimaryKeyBlankOrZeroRejected(t *testing.T) {
	e := a142Setup(t, "主键")
	probe := []struct {
		label string
		id    interface{}
	}{
		{"缺键", nil},
		{"空串", ""},
		{"纯空白", "   "},
		{"数字 0", 0},
		{"字符串 \"0\"", "0"},
	}
	for _, c := range probe {
		args := map[string]interface{}{"actorIds": []string{"9101"}}
		if c.id != nil {
			args["processTaskId"] = c.id
		}
		r := e.f.Flow("processTask/addCandidate", args)
		if code, _ := r["code"].(int); code == 0 {
			t.Fatalf("§2.11 主键档：%s 不得被静默接受，实得 %v", c.label, r)
		}
		if msg, _ := r["msg"].(string); !strings.Contains(msg, "processTaskId 缺失或非法") {
			t.Fatalf("§2.11 主键档：%s 须沿用既有「processTaskId 缺失或非法」信封，实得 msg=%q", c.label, msg)
		}
	}
	// 零副作用：没有任何一条 actor 行落进库（改前红在 0 那一档：process_task_id=0 多一行）
	if got := a142ActorsOf(t, e.repo, 0); len(got) != 0 {
		t.Fatalf("§2.11：主键 0 不得拿去落库，实得 process_task_id=0 上有 %q", got)
	}
	mustAdded142(t, "主键报错档不得动既有参与者", e.added(t), nil)
}

// a142ActorsOf 读某任务 id 上的参与者行（主键 0 那一档的取证）。
func a142ActorsOf(t *testing.T, repo *memory.Repository, taskID int64) []string {
	t.Helper()
	got, err := repo.FindTaskActors(context.Background(), taskID)
	if err != nil {
		t.Fatalf("取证 actor 行: %v", err)
	}
	return got
}

// TestIssue142FacadeTransferNormalizesBeforeDeleteAndInsert transfer 的 fromActor/toActor
// 归一后再参与删除与插入（§2.11 写点表第 2 行＋硬要求②「比较取 trim 后的值」）：
//   - 库里那一行是**修复前落下的未 trim 原值** " 2001 "，入参 "2001" 必须判成同一个人并真删掉它；
//   - 入参 toActor " 3001 " 落库的是 trim 后的 "3001"。
//
// 改前红：`containsStr(participants, "2001")` 拿归一值比未 trim 的行 ⇒ 报
// 「原办理人不是该任务参与人」（code≠0），转办根本走不下去。
func TestIssue142FacadeTransferNormalizesBeforeDeleteAndInsert(t *testing.T) {
	e := a142Setup(t, "transfer")
	ctx := context.Background()
	tk, err := e.repo.FindTaskByID(ctx, e.taskID)
	if err != nil || tk == nil {
		t.Fatalf("读任务行: %v", err)
	}
	tk.ActorIDs = []string{"leader", " 2001 "} // 摆历史脏行（内存仓 UpdateTask 用它覆盖参与者表）
	if err := e.repo.UpdateTask(ctx, tk); err != nil {
		t.Fatalf("摆脏行: %v", err)
	}
	mustOk(t, e.f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": e.taskID, "fromActor": " 2001 ", "toActor": " 3001 ",
		"operator": "2001",
	}))
	got, _ := e.repo.FindTaskActors(ctx, e.taskID)
	want := []string{"leader", "3001"} // " 2001 " 被摘掉（用行上的原值去删），"3001" 落的是 trim 后的值
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11：转办后参与者应 %q，实得 %q", want, got)
	}
	// 留痕里的两个人也是归一后的形状（账本不再生出 " 2001 " 这种花名）
	fresh, _ := e.repo.FindTaskByID(ctx, e.taskID)
	if fresh.Variables["tf_transferTo"] != "3001" {
		t.Fatalf("§2.11：tf_transferTo 应落归一后的值，实得 %#v", fresh.Variables["tf_transferTo"])
	}
	rec, _ := fresh.Variables["tf_transferHistory"].([]interface{})
	if len(rec) != 1 {
		t.Fatalf("留痕账本应一跳，实得 %#v", fresh.Variables["tf_transferHistory"])
	}
	row, _ := rec[0].(map[string]interface{})
	if row["fromActor"] != "2001" || row["toActor"] != "3001" {
		t.Fatalf("§2.11：账本应记归一后的 fromActor/toActor，实得 %v", row)
	}
}

// TestIssue142FacadeTransferToActorAlreadyParticipantAfterTrim 归一后同一个人就是同一个人：
// 行上是 " 4001 "，入参 toActor "4001" ⇒ 应报既有「目标人已是该任务参与人」，不落第二行。
//
// 改前红："4001" 与 " 4001 " 判成两个人 ⇒ 报成功并追加一条，同一人两行（判重被架空）。
func TestIssue142FacadeTransferToActorAlreadyParticipantAfterTrim(t *testing.T) {
	e := a142Setup(t, "transfer-dup")
	ctx := context.Background()
	tk, _ := e.repo.FindTaskByID(ctx, e.taskID)
	tk.ActorIDs = []string{" 4001 ", "leader"}
	if err := e.repo.UpdateTask(ctx, tk); err != nil {
		t.Fatalf("摆脏行: %v", err)
	}
	r := e.f.Flow("processTask/transfer", map[string]interface{}{
		"processTaskId": e.taskID, "fromActor": "leader", "toActor": "4001", "operator": "leader",
	})
	if code, _ := r["code"].(int); code == 0 {
		t.Fatalf("§2.11：trim 后同人应命中「目标人已是该任务参与人」，实得 %v", r)
	}
	if msg, _ := r["msg"].(string); !strings.Contains(msg, "目标人已是该任务参与人") {
		t.Fatalf("应沿用既有文案，实得 msg=%q", msg)
	}
	if got, _ := e.repo.FindTaskActors(ctx, e.taskID); len(got) != 2 {
		t.Fatalf("报错档必须零副作用（不得追加第二行），实得 %q", got)
	}
}

// TestIssue142FacadeUpdateCCStatusOperatorNormalized §2.11 写点表第 4 行：
// updateCCStatus 的 operator **入参归一后再比**——带两端空格仍打中 trim 后落库的那一行。
//
// 改前红：operatorArg 不 trim（issues/129 只把空串回落 user1），内存仓按原值比 ⇒
// 一行都没置已读，而门面回 code=0（静默 no-op）。
func TestIssue142FacadeUpdateCCStatusOperatorNormalized(t *testing.T) {
	e := a142Setup(t, "cc-status")
	mustOk(t, e.f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": e.instID, "operator": "zhangsan", "actorIds": []string{"4101"},
	}))
	mustOk(t, e.f.Flow("processInstance/updateCCStatus", map[string]interface{}{
		"processInstanceId": e.instID, "operator": " 4101 ",
	}))
	rows := e.repo.CcRowsForTest(e.instID)
	if len(rows) != 1 {
		t.Fatalf("夹具：cc 应一行，实得 %d", len(rows))
	}
	if rows[0].State != 1 {
		t.Fatalf("§2.11：operator 归一后再比 ⇒ 那行应 state=1，实得 %d", rows[0].State)
	}
}

// TestIssue142FacadeStartNextNodeOperatorArrayNormalized f_nextNodeOperator（发起预指派）
// 的数组形与串形同判据：trim＋丢空＋nil 不串化。
//
// 改前红：数组形那一支不 trim、"" 照收、nil 变 "<nil>" ⇒ task1 参与者多出三条垃圾归属值。
func TestIssue142FacadeStartNextNodeOperatorArrayNormalized(t *testing.T) {
	f, repo, _ := setupFacade()
	mustOk(t, f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))}))
	defID := mustDefineID(t, repo, "simple")

	start := func(v interface{}) []string {
		r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
			"processDefineId": defID, "operator": "zhangsan", "f_nextNodeOperator": v,
		})
		mustOk(t, r)
		instID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])
		taskID := doingTaskID(t, repo, instID, "task1")
		if taskID == 0 {
			t.Fatalf("夹具：应推进到 task1")
		}
		actors, err := repo.FindTaskActors(context.Background(), taskID)
		if err != nil {
			t.Fatalf("读参与者: %v", err)
		}
		return actors
	}
	want := []string{"n1", "n2"}
	if got := start([]interface{}{" n1 ", "", "   ", nil, "n2", "n2"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11：f_nextNodeOperator 数组形应 %q，实得 %q", want, got)
	}
	if got := start(" n1 , , n2 , n1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("§2.11「两形同判据」分叉：串形应 %q，实得 %q", want, got)
	}
}
