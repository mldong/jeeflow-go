// issues/126 案 A · 任务行 expire_time 由**建单路径**按节点到期表达式真算（go 栈）。
//
// 形状照 Java 参考实现的 `ExpireTimeOnCreateTest`（jeeflow-java commit d9e9397）：
// ① 配 "2h" ⇒ **同一行** ExpireTime − CreateTime ≈ 2h（基准取本行 createTime，不拿"现在"当基准）；
// ② 表达式是个变量名 ⇒ 取该变量的值；③ 未配（属性缺键 / null / 空串三档）⇒ 该列必须空；
// ④ 解析不出 ⇒ 空，而不是 now。
// 在此之上把四处写点各自的格子补齐（普通建单 / 串行会签首位 / 串行会签推进位 / 并行会签全员 /
// 回退新建），并额外钉住两档易混事实：
//   - **变量源两档**：建单用实例变量、回退新建用随行拷贝那份变量；
//   - **表达式来源两档**：回退新建取的是"被回退掉的那个节点"（boot2 的 current），
//     不是复活行落地的节点（prev）——所以回退那几格的夹具给成**两个节点两份表达式**
//     （落地 1d / 当前 3h），只配一个节点时取错节点也照样绿，判不出来。
//
// 判据一律落在**内存仓读回的持久行**上（不是引擎返回的聚合对象），issues/113 教训：
// 只有读回值能证明"这一列真进了库"。
//
// ⚠️ 本文件对更早的既有用例只新增断言，没改它们的期望值；上面"表达式来源两档"那几格
//    属于 issues/126 本批自己的用例，09-28 二轮按基准把夹具改成双节点不同表达式。
package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

const expLayout = "2006-01-02 15:04:05"

// expSpec 一个任务节点：id + **完整**的 properties 片段（不用"基础片段+覆盖"的写法，
// 免得 JSON 里出现同名键靠"后者胜出"这种隐式行为）
type expSpec struct{ id, props string }

// expFlowWith 造 start → (specs 逐个 task 节点) → end 的线性流程图
func expFlowWith(name string, specs ...expSpec) string {
	var nodes, edges []string
	nodes = append(nodes, `{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}}`)
	prev := "start"
	for i, s := range specs {
		nodes = append(nodes, fmt.Sprintf(
			`{"id":"%s","type":"snaker:task","properties":{%s},"text":{"value":"节点%d"}}`, s.id, s.props, i+1))
		edges = append(edges, fmt.Sprintf(
			`{"id":"e%d","sourceNodeId":"%s","targetNodeId":"%s","properties":{}}`, i, prev, s.id))
		prev = s.id
	}
	nodes = append(nodes, `{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}`)
	edges = append(edges, fmt.Sprintf(
		`{"id":"eend","sourceNodeId":"%s","targetNodeId":"end","properties":{}}`, prev))
	return `{"name":"` + name + `","displayName":"到期时间建单","type":"approval","nodes":[` +
		strings.Join(nodes, ",") + `],"edges":[` + strings.Join(edges, ",") + `]}`
}

// expHarness 本文件自带的内存仓装配（与 surrogate_test.go 的 newHarness 同形，但换用户桩，
// 见 expUserProv）。扩展仓储传 nil——本案与委托无关。
func expHarness(t *testing.T, name, content string) (*engine.EngineImpl, *memory.Repository, int64) {
	t.Helper()
	repo := memory.New()
	def := &model.ProcessDefine{Name: name, DisplayName: "到期时间建单", Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return engine.New(repo, expUserProv{}, &sIDGen{}, nil), repo, def.ID
}

// expUserProv 用户桩：把两个人的 u_userId 直接做成**合法日期串**。
// 唯一用途是给"变量源两档"造出可判的对照——实例变量里 u_userId 恒是**发起人**（boss1）的，
// 而行变量里 u_userId 是**本次办结人**（zhangsan）的（prepareExecuteTask 的
// mergeExecIntoInstance 刻意不把操作人 u_* 写回实例，issues/97）。
// 于是"表达式＝u_userId"这一格能直接读出引擎用的是哪一份变量。
var expUserDates = map[string]string{
	"boss1":    "2028-01-01 01:01:01",
	"zhangsan": "2029-02-02 02:02:02",
}

type expUserProv struct{}

func (expUserProv) GetUser(userID string) (*model.UserInfo, error) {
	uid := userID
	if d, ok := expUserDates[userID]; ok {
		uid = d
	}
	return &model.UserInfo{UserID: uid, RealName: "用户" + userID}, nil
}

// repoReader 只依赖读接口，方便下面的 helper 收内存仓
type repoReader interface {
	FindDoingTasks(context.Context, int64, []string) ([]*model.ProcessTask, error)
}

// expMustDoing 读回某节点的 DOING 行（要求恰好 want 条，按 id 升序返回——内存仓遍历 map 无序）
func expMustDoing(t *testing.T, repo repoReader, instID int64, node string, want int) []*model.ProcessTask {
	t.Helper()
	rows, err := repo.FindDoingTasks(context.Background(), instID, []string{node})
	if err != nil {
		t.Fatalf("读回 %s 的 DOING 行: %v", node, err)
	}
	if len(rows) != want {
		t.Fatalf("%s 的 DOING 行数 = %d, want %d", node, len(rows), want)
	}
	for i := 1; i < len(rows); i++ { // 插入排序按 ID 升序，条数≤参与人数，够用
		for j := i; j > 0 && rows[j].ID < rows[j-1].ID; j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
	return rows
}

// expMustDelta 断言"同一行 expire − create ≈ 表达式偏移"（**不许只判非空**：
// 只判非空就会被占位 now() 蒙过去，那正是本病灶的形状）
func expMustDelta(t *testing.T, row *model.ProcessTask, want time.Duration) {
	t.Helper()
	if row.ExpireTime == nil {
		t.Fatalf("行 %d 配了到期表达式 %v，expire_time 却是空", row.ID, want)
	}
	delta := row.ExpireTime.Sub(row.CreateTime)
	// 带宽 [-5s, +60s]：容时钟/落库量纲，不容"根本没算"（java 同款注释：占位写法算出≈0）
	if delta < want-5*time.Second || delta > want+60*time.Second {
		t.Errorf("expire − create = %v，期望 %v±(5s,60s)；占位 now() 会算出≈0 ⇒ 新建即逾期", delta, want)
	}
}

// expMustNull 断言"这一列保持 NULL"（未配 / 解析不出两档共用）
func expMustNull(t *testing.T, row *model.ProcessTask, why string) {
	t.Helper()
	if row.ExpireTime != nil {
		t.Errorf("%s：expire_time 被赋成 %v，期望保持空（不造默认值、不写 now()）", why, *row.ExpireTime)
	}
}

func expAt(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.ParseInLocation(expLayout, s, time.Local)
	if err != nil {
		t.Fatalf("夹具时间 %q 解析失败: %v", s, err)
	}
	return v
}

func expStart(t *testing.T, eng *engine.EngineImpl, defID int64, args map[string]interface{}) *model.ProcessInstance {
	t.Helper()
	inst, err := eng.StartProcessInstanceByID(context.Background(), defID, "boss1", args)
	if err != nil {
		t.Fatalf("发起流程失败: %v", err)
	}
	return inst
}

// ─── ① 普通建单（engine.createTask 普通分支）────────────────────────────────────

func TestExpireTimeNormalCreateRelativeExpression(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_2h",
		expFlowWith("exp126_2h", expSpec{"approve", `"assignee":"zhangsan","expireTime":"2h"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126A"})
	expMustDelta(t, expMustDoing(t, repo, inst.ID, "approve", 1)[0], 2*time.Hour)
}

// 表达式是变量名 ⇒ 取实例变量里那个变量的值（FlowUtil.processTime 第一档；
// 建单三处的变量源＝实例变量，对齐 Java this.variables / boot2 execution.getArgs()）
func TestExpireTimeNormalCreateExpressionIsVariable(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_var",
		expFlowWith("exp126_var", expSpec{"approve", `"assignee":"zhangsan","expireTime":"dueAt"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"dueAt": "2026-12-31 10:00:00"})
	row := expMustDoing(t, repo, inst.ID, "approve", 1)[0]
	if row.ExpireTime == nil {
		t.Fatalf("表达式是变量名 dueAt 且实例变量有值，expire_time 却是空")
	}
	if !row.ExpireTime.Equal(expAt(t, "2026-12-31 10:00:00")) {
		t.Errorf("expire_time = %v，期望取变量值 %v", *row.ExpireTime, expAt(t, "2026-12-31 10:00:00"))
	}
}

// 未配的三档（属性缺键 / JSON null / 空串）⇒ 该列保持 NULL，不许造默认值。
// 三档并排跑，是因为"投成空串还是缺键"这种形状差异本身就藏过病灶（见 expireExprOf 注释）。
func TestExpireTimeNormalCreateUnconfiguredKeepsNull(t *testing.T) {
	cases := []struct{ seg, label string }{
		{`"assignee":"zhangsan"`, "属性缺键（未配）"},
		{`"assignee":"zhangsan","expireTime":null`, "配成 null"},
		{`"assignee":"zhangsan","expireTime":""`, "配成空串"},
	}
	for i, c := range cases {
		name := fmt.Sprintf("exp126_none%d", i)
		eng, repo, defID := expHarness(t, name, expFlowWith(name, expSpec{"approve", c.seg}))
		inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126N"})
		expMustNull(t, expMustDoing(t, repo, inst.ID, "approve", 1)[0], c.label)
	}
}

// 解析不出 ⇒ 空，而不是 now()（那等于静默造一个"建单即逾期"的值）
func TestExpireTimeNormalCreateUnparsableStaysNull(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_bad",
		expFlowWith("exp126_bad", expSpec{"approve", `"assignee":"zhangsan","expireTime":"not-a-time"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126B"})
	expMustNull(t, expMustDoing(t, repo, inst.ID, "approve", 1)[0], "表达式 not-a-time 解析不出")
}

// 绝对时刻档：表达式本身就是 "yyyy-MM-dd HH:mm:ss"
func TestExpireTimeNormalCreateAbsoluteExpression(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_abs",
		expFlowWith("exp126_abs", expSpec{"approve", `"assignee":"zhangsan","expireTime":"2027-03-04 05:06:07"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126AB"})
	row := expMustDoing(t, repo, inst.ID, "approve", 1)[0]
	if row.ExpireTime == nil || !row.ExpireTime.Equal(expAt(t, "2027-03-04 05:06:07")) {
		t.Errorf("expire_time = %v，期望绝对时刻 %v", row.ExpireTime, expAt(t, "2027-03-04 05:06:07"))
	}
}

// ─── ② 并行会签全员 / ③ 串行会签首位（engine.createTask 会签分支）───────────────

func TestExpireTimeParallelCountersignEveryMember(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_par", expFlowWith("exp126_par",
		expSpec{"cs", `"assignee":"zhangsan,wangwu","performType":"1","countersignType":"PARALLEL","expireTime":"2h"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126P"})
	rows := expMustDoing(t, repo, inst.ID, "cs", 2) // 并行＝全员一次建齐
	for _, r := range rows {
		expMustDelta(t, r, 2*time.Hour)
	}
}

func TestExpireTimeSequentialCountersignFirstMember(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_seq", expFlowWith("exp126_seq",
		expSpec{"cs", `"assignee":"zhangsan,wangwu","performType":"1","countersignType":"SEQUENTIAL","expireTime":"2h"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126S"})
	first := expMustDoing(t, repo, inst.ID, "cs", 1)[0]
	expMustDelta(t, first, 2*time.Hour)

	// 串行会签**推进到的那一位**走的是另一个写点（ExecuteProcessTask 里的推进分支），单独一格
	if _, err := eng.ExecuteProcessTask(context.Background(), first.ID, "zhangsan",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("首位成员办结失败: %v", err)
	}
	rows, err := repo.FindDoingTasks(context.Background(), inst.ID, []string{"cs"})
	if err != nil || len(rows) != 1 {
		t.Fatalf("推进后 DOING 行 = %d err=%v, want 1", len(rows), err)
	}
	expMustDelta(t, rows[0], 2*time.Hour)
}

// TestExpireTimeParallelCountersignUnconfiguredKeepsNull 会签节点**没配**到期表达式 ⇒ 行要建齐（两位
// 成员都在待办里），但这一列必须保持空：不造默认值、不写 now()。
// 与 TestExpireTimeParallelCountersignEveryMember 成对——只有正向档时，"接线整条没跑"和
// "跑了但算成空值"两种实现都能绿；配上这一档才分得开。
func TestExpireTimeParallelCountersignUnconfiguredKeepsNull(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_parnc", expFlowWith("exp126_parnc",
		expSpec{"cs", `"assignee":"zhangsan,wangwu","performType":"1","countersignType":"PARALLEL"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126PN"})
	rows := expMustDoing(t, repo, inst.ID, "cs", 2) // 行必须在，否则"空"是"没建行"混过去的
	for _, r := range rows {
		expMustNull(t, r, "并行会签：节点未配到期表达式")
	}
}

// TestExpireTimeSequentialUnconfiguredKeepsBothRowsNull 串行会签的**两个**写点（首成员建单 +
// 推进出的下一位）在未配表达式时都必须留空——推进那一支是独立接线点，只测首成员等于没测它。
func TestExpireTimeSequentialUnconfiguredKeepsBothRowsNull(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_seqnc", expFlowWith("exp126_seqnc",
		expSpec{"cs", `"assignee":"zhangsan,wangwu","performType":"1","countersignType":"SEQUENTIAL"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126SN"})
	first := expMustDoing(t, repo, inst.ID, "cs", 1)[0]
	expMustNull(t, first, "串行会签：首成员行（节点未配到期表达式）")

	if _, err := eng.ExecuteProcessTask(context.Background(), first.ID, "zhangsan",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("首位成员办结失败: %v", err)
	}
	rows, err := repo.FindDoingTasks(context.Background(), inst.ID, []string{"cs"})
	if err != nil || len(rows) != 1 {
		t.Fatalf("推进后 DOING 行 = %d err=%v, want 1", len(rows), err)
	}
	expMustNull(t, rows[0], "串行会签：推进出的第二成员行")
}

// 会签分支的 default（countersignType 既非 PARALLEL 也非 SEQUENTIAL，如 RATIO 系）也是逐人建单，
// 同一个写点形状——单独一格，否则这条分支的接线没有测试覆盖（变异对照实测发现的缺口）
func TestExpireTimeOtherCountersignTypeEveryMember(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_ratio", expFlowWith("exp126_ratio",
		expSpec{"cs", `"assignee":"zhangsan,wangwu","performType":"1","countersignType":"RATIO50","expireTime":"2h"`}))
	inst := expStart(t, eng, defID, map[string]interface{}{"BUSINESS_NO": "B126Q"})
	for _, r := range expMustDoing(t, repo, inst.ID, "cs", 2) {
		expMustDelta(t, r, 2*time.Hour)
	}
}

// ─── ④ 回退/跳转新建（engine.rollbackToParent）─────────────────────────────────

// expRollbackFlow 回退夹具：apply → approve → end，全由 zhangsan 办。
// 两个节点的 properties **各配各的**（applyProps＝复活行落地的节点＝prev，
// approveProps＝被回退掉的那个节点＝boot2 的 current）：写点④的到期表达式取的是**后者**，
// 前者在这几格里只当"取错节点"的对照——只给一个节点配表达式时取错节点也照样绿（没牙），
// 所以两档必须配成不同的值。
func expRollbackFlow(name, applyProps, approveProps string) string {
	return expFlowWith(name,
		expSpec{"apply", applyProps},
		expSpec{"approve", approveProps})
}

// expRollbackToApply 发起(boss1) → 办结 apply(zhangsan) → 从 still-DOING 的 approve 上
// ROLLBACK（target 空＝退回上一步，issues/121 P2 血缘版），返回**复活出来的那条 apply 行**
// 和**办结前的原始 apply 行**（原始行走写点①、按 apply 节点自己的表达式赋值，
// 正好当复活行该走写点④（approve 那份表达式）的对照）。
// 注意：回退的当前行必须还在 DOING（先把它办结再回退会得到 "task not doing"）。
func expRollbackToApply(t *testing.T, eng *engine.EngineImpl, repo repoReader, defID int64,
	startArgs, applyArgs map[string]interface{}) (revived, original *model.ProcessTask) {
	t.Helper()
	inst := expStart(t, eng, defID, startArgs)
	original = expMustDoing(t, repo, inst.ID, "apply", 1)[0]
	if _, err := eng.ExecuteProcessTask(context.Background(), original.ID, "zhangsan", applyArgs); err != nil {
		t.Fatalf("办结 apply 失败: %v", err)
	}
	approve := expMustDoing(t, repo, inst.ID, "approve", 1)[0]
	if _, err := eng.ExecuteAndJumpTask(context.Background(), approve.ID, "zhangsan",
		map[string]interface{}{"submitType": 3}, ""); err != nil {
		t.Fatalf("回退上一步失败: %v", err)
	}
	// 回退后 approve 行已办结，该节点的 DOING 只剩复活的那条 apply
	return expMustDoing(t, repo, inst.ID, "apply", 1)[0], original
}

// 写点④取的是**被回退掉的那个节点**（approve，配 "3h"），不是复活行落地的 apply（配 "1d"）。
// 同一条 apply 节点的两行给出两个不同到期值——原始行由写点①按 apply 自己的 1d 赋值，
// 复活行由写点④按 approve 的 3h 赋值：哪天表达式来源错接回 prev，复活行会算出 ≈1d，
// 这格当场红（变异对照实测的红样就是"实得 24h0m0s / 期望 3h"这种量级差）。
func TestExpireTimeRollbackCreateRelativeExpression(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_rb3h", expRollbackFlow("exp126_rb3h",
		`"assignee":"zhangsan","expireTime":"1d"`, `"assignee":"zhangsan","expireTime":"3h"`))
	revived, original := expRollbackToApply(t, eng, repo, defID,
		map[string]interface{}{"BUSINESS_NO": "B126R1"}, map[string]interface{}{})
	expMustDelta(t, original, 24*time.Hour) // 对照：落地节点的表达式只管常规建单那一行
	expMustDelta(t, revived, 3*time.Hour)   // 判点：复活行取被回退掉的那个节点
}

// 反向钉（取错节点最直接的判据）：**被回退掉的节点没配**、复活行落地的 apply 配了 "1d"
// ⇒ 复活行的 expire_time 必须留空（写点④只认 current 那份表达式）。
// 错接成 prev 就会算出 ≈1d（不是空），这格立刻红。
func TestExpireTimeRollbackCreateIgnoresLandingNodeExpression(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_rbcurnone", expRollbackFlow("exp126_rbcurnone",
		`"assignee":"zhangsan","expireTime":"1d"`, `"assignee":"zhangsan"`))
	revived, original := expRollbackToApply(t, eng, repo, defID,
		map[string]interface{}{"BUSINESS_NO": "B126R4"}, map[string]interface{}{})
	expMustDelta(t, original, 24*time.Hour) // 前置：1d 确实配在落地节点上（常规建单读得到它）
	expMustNull(t, revived, "被回退掉的节点（approve）没配表达式，落地节点（apply）配了 1d")
}

// 回退新建的变量源＝**随行拷贝那份变量**（boot2 的 hisVariable）：
// 表达式 "dueAt" 配在**被回退掉的 approve 节点**上，值在 apply 那条历史行的变量里 ⇒ 复活行取到它
// （落地节点同时配 "1d" 当"取错节点"的对照：错接 prev 会得到 ≈1d 而非那个绝对时刻）
func TestExpireTimeRollbackCreateUsesCarriedVariables(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_rbvar", expRollbackFlow("exp126_rbvar",
		`"assignee":"zhangsan","expireTime":"1d"`, `"assignee":"zhangsan","expireTime":"dueAt"`))
	revived, _ := expRollbackToApply(t, eng, repo, defID,
		map[string]interface{}{"BUSINESS_NO": "B126R2"},
		map[string]interface{}{"dueAt": "2026-12-31 10:00:00"})
	if revived.ExpireTime == nil {
		t.Fatalf("表达式 dueAt 在随行变量里，复活行的 expire_time 却是空")
	}
	if !revived.ExpireTime.Equal(expAt(t, "2026-12-31 10:00:00")) {
		t.Errorf("expire_time = %v，期望随行变量值 %v（≈1d 说明表达式来源错接成了落地节点 prev）",
			*revived.ExpireTime, expAt(t, "2026-12-31 10:00:00"))
	}
}

// 两档变量源的正面对撞（"搞混会让变量名这一档跨栈给出不同答案"的钉子）：
// 表达式 u_userId 配在**被回退掉的 approve 节点**上，且在**两份变量里都有、值不同**——
//   - 随行拷贝那份（apply 行）＝本次办结人 zhangsan 的 ⇒ 2029-02-02 02:02:02 ← 契约要这个
//   - 实例变量那份 ＝发起人的（prepareExecuteTask 的 mergeExecIntoInstance 不把操作人 u_*
//     写回实例，issues/97）⇒ 2028-01-01 01:01:01
//
// 哪天把回退新建的变量源错接成 inst.Variables，这一格立刻红
// （落地节点 apply 也配了 "1d"，错接成 prev 同样红——实得 ≈1d）。
func TestExpireTimeRollbackCreateReadsCarriedNotInstanceVariables(t *testing.T) {
	eng, repo, defID := expHarness(t, "exp126_rbsrc", expRollbackFlow("exp126_rbsrc",
		`"assignee":"zhangsan","expireTime":"1d"`, `"assignee":"zhangsan","expireTime":"u_userId"`))
	revived, _ := expRollbackToApply(t, eng, repo, defID,
		map[string]interface{}{"BUSINESS_NO": "B126R3"}, map[string]interface{}{})
	if revived.ExpireTime == nil {
		t.Fatalf("表达式 u_userId 两份变量里都有值，复活行却是空")
	}
	if !revived.ExpireTime.Equal(expAt(t, "2029-02-02 02:02:02")) {
		t.Errorf("expire_time = %v，期望随行那份（办结人 zhangsan）%v；取到 %v 就说明变量源错接成了实例变量",
			*revived.ExpireTime, expAt(t, "2029-02-02 02:02:02"), expAt(t, "2028-01-01 01:01:01"))
	}
}

// ─── 求值器档位（逐字对齐 Java FlowUtil.processTime 的三档 + 三个易错点）────────

func TestProcessTimeThreeTiers(t *testing.T) {
	base := time.Now()
	ms := base.Add(3 * time.Hour).UnixMilli()
	cases := []struct {
		name  string
		expr  string
		args  map[string]interface{}
		check func(*testing.T, *time.Time)
	}{
		{"未配-空串", "", nil, func(t *testing.T, v *time.Time) { expWantNil(t, "空串", v) }},
		{"未配-纯空白", "   ", nil, func(t *testing.T, v *time.Time) { expWantNil(t, "纯空白", v) }},
		{"变量档-日期字符串", "dueAt",
			map[string]interface{}{"dueAt": "2026-12-31 10:00:00"}, func(t *testing.T, v *time.Time) {
				expWantAt(t, "2026-12-31 10:00:00", v)
			}},
		{"变量档-毫秒时间戳", "dueMs", map[string]interface{}{"dueMs": ms},
			func(t *testing.T, v *time.Time) {
				if v == nil || !v.Equal(time.UnixMilli(ms)) {
					t.Errorf("毫秒档 = %v，期望 %v", v, time.UnixMilli(ms))
				}
			}},
		{"变量档-time.Time", "dueT", map[string]interface{}{"dueT": expFixed},
			func(t *testing.T, v *time.Time) {
				if v == nil || !v.Equal(expFixed) {
					t.Errorf("time.Time 档 = %v，期望 %v", v, expFixed)
				}
			}},
		// 易错点①：变量存在但类型不认识 ⇒ **落穿**（不是提前 return nil）
		{"变量档-类型不认识落穿", "2h", map[string]interface{}{"2h": true},
			func(t *testing.T, v *time.Time) {
				if v == nil {
					t.Errorf("落穿失败：bool 值该走相对档，不该返回空")
					return
				}
				d := v.Sub(base)
				if d < 2*time.Hour-time.Minute || d > 2*time.Hour+time.Minute {
					t.Errorf("落穿后相对档 = %v，期望≈base+2h", d)
				}
			}},
		{"变量档-非整值数值落穿", "dueF", map[string]interface{}{"dueF": 1.5},
			func(t *testing.T, v *time.Time) { expWantNil(t, "Double 档落穿后无相对/绝对档可走", v) }},
		// 易错点②：变量档**优先于**相对档（args 里真有个键叫 "2h" ⇒ 取变量值，不是 now+2h）
		{"变量档优先于相对档", "2h", map[string]interface{}{"2h": "2026-12-31 10:00:00"},
			func(t *testing.T, v *time.Time) { expWantAt(t, "2026-12-31 10:00:00", v) }},
		// 易错点③：字符串档解析失败＝终局 null，**不落穿**（"30m" 不是相对档的输入）
		{"变量档-字符串解析失败即空", "dueBad", map[string]interface{}{"dueBad": "tomorrow"},
			func(t *testing.T, v *time.Time) { expWantNil(t, "变量值为解析不出的字符串", v) }},
		{"变量档-值为 null 落穿", "dueNull", map[string]interface{}{"dueNull": nil},
			func(t *testing.T, v *time.Time) {
				expWantNil(t, "变量存在但值是 null ⇒ 落穿后绝对档也不出", v)
			}},
		{"相对档-秒", "90s", nil, func(t *testing.T, v *time.Time) { expWantFromNow(t, 90*time.Second, v) }},
		{"相对档-分", "45m", nil, func(t *testing.T, v *time.Time) { expWantFromNow(t, 45*time.Minute, v) }},
		{"相对档-时", "2h", nil, func(t *testing.T, v *time.Time) { expWantFromNow(t, 2*time.Hour, v) }},
		{"相对档-天(日历加)", "3d", nil, func(t *testing.T, v *time.Time) { expWantFromNow(t, 72*time.Hour, v) }},
		{"相对档-负偏移", "-1h", nil, func(t *testing.T, v *time.Time) { expWantFromNow(t, -time.Hour, v) }},
		{"相对档-前缀非整数不出值", "xh", nil, func(t *testing.T, v *time.Time) { expWantNil(t, "前缀非整数", v) }},
		{"绝对档", "2027-03-04 05:06:07", nil, func(t *testing.T, v *time.Time) { expWantAt(t, "2027-03-04 05:06:07", v) }},
		{"绝对档-带日期无时分秒解析不出", "2027-03-04", nil, func(t *testing.T, v *time.Time) { expWantNil(t, "缺时分秒", v) }},
		{"解析不出", "not-a-time", nil, func(t *testing.T, v *time.Time) { expWantNil(t, "垃圾串", v) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.check(t, engine.ProcessTime(c.expr, c.args))
		})
	}
}

// 天档必须是**日历加天**（AddDate），不是乘 86400s——这里用固定时刻核差值，
// 与 Java Calendar.add(DAY_OF_MONTH) 同量纲
func TestProcessTimeDayTierIsCalendarBased(t *testing.T) {
	v := engine.ProcessTime("5d", nil)
	if v == nil {
		t.Fatalf("5d 算出了空")
	}
	if got := v.AddDate(0, 0, -5); got.Year() != time.Now().Year() || got.Month() != time.Now().Month() ||
		got.Day() != time.Now().Day() {
		t.Errorf("5d 减回 5 个日历日应落回今天，实得 %v", got)
	}
}

var expFixed = time.Date(2030, 6, 7, 8, 9, 10, 0, time.Local)

func expWantNil(t *testing.T, why string, v *time.Time) {
	t.Helper()
	if v != nil {
		t.Errorf("%s：期望空，实得 %v（**任何档都不许兜底成 now()**）", why, *v)
	}
}

func expWantAt(t *testing.T, s string, v *time.Time) {
	t.Helper()
	want, err := time.ParseInLocation(expLayout, s, time.Local)
	if err != nil {
		t.Fatalf("夹具时间 %q: %v", s, err)
	}
	if v == nil || !v.Equal(want) {
		t.Errorf("实得 %v，期望 %v", v, want)
	}
}

func expWantFromNow(t *testing.T, off time.Duration, v *time.Time) {
	t.Helper()
	if v == nil {
		t.Fatalf("相对档算出了空")
	}
	d := time.Until(*v)
	if d < off-2*time.Second || d > off+2*time.Second {
		t.Errorf("相对档偏移 = %v，期望 ≈%v", d, off)
	}
}
