// 空抄送人不建 cc 行（issues/141 G10 · owner 2026-09-29 拍「空不创建行」· Go 栈三条入口）。
//
// 立法逐字依据＝spec 06-facade.md §2.10：三条入口（发起 f_ccActors／办理 tf_ccActors／门面
// 手动 createCCInstance）解析抄送人集合时，**空串、纯空白、数组里的空元素一律丢弃**；丢完为空
// ⇒ 不建任何 cc 行、也**不 fire 码 4**；逗号串与数组两种形态必须同判据。
//
// 病灶与 java 同源：java 旧形状是 `"".split(",")` 得到一个空元素 ⇒ 真落一条 actor_id 为空串的
// cc 行；
// 本仓的对应形状是**门面手动腿**——`toStringSlice2` 对 []string / []interface{} 两形既不 trim
// 也不丢空（只有逗号串那一支丢了），于是 `{"actorIds":[""," "]}` 直落两行空归属值＋fire 两支码 4。
// 空归属值是 issues/129 那族"空 operator 读全库"的病根，不能从抄送侧继续往里灌。
//
// 判据两层都挡（spec §2.10 实现要求①）：漏斗层（engine.parseCcActors ＋门面手动腿，均过
// [spi.NormalizeCcActors]）＋ 写侧层（两仓的 CreateCcInstance 各自再挡一次，见
// memory/cc_blank_actor_141_test.go 与 repository/jdbc/cc_blank_actor_141_sqlite_test.go）。
// 只修漏斗 ⇒ 绕过引擎/门面直连仓储的调用方照样灌进空值；只修写侧 ⇒ 手动腿的"全空白"档还会
// 报成功而不是与"空 actorIds"同档。两层各管各的格子，本文件钉漏斗，仓储档另开两文件。
//
// 落库与比较一律取 trim 后的值（实现要求②）：`" 123 "` 与 `"123"` 是同一个人，这一条与已落地
// 的 G2 写侧判重（[memory.Repository.CreateCcInstanceIfAbsent]，本仓 ed29661）咬合——不 trim
// 就让同一人落两行，把 G2 的判据打穿。
package facade_test

import (
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
)

// manualRaw141G10 手动腿原始返回（全空白档要看它是不是与"空 actorIds"同档，不能假定成功）。
func manualRaw141G10(t *testing.T, f *facade.Facade, instanceID int64, actorIDs interface{}) map[string]interface{} {
	t.Helper()
	return f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "zhangsan", "actorIds": actorIDs,
	})
}

// mustManualOk141G10 手动腿：断言成功（非空档才用）。
func mustManualOk141G10(t *testing.T, f *facade.Facade, instanceID int64, actorIDs interface{}) {
	t.Helper()
	mustOk(t, manualRaw141G10(t, f, instanceID, actorIDs))
}

func assertNoCcRow141G10(t *testing.T, repo *memory.Repository, instanceID int64, events *[]engine.ProcessEvent, label string) {
	t.Helper()
	if n := len(repo.CcRowsForTest(instanceID)); n != 0 {
		t.Fatalf("%s：G10 不得建 cc 行，实得 %d 行 actors=%q", label, n, ccActors141(repo, instanceID))
	}
	if len(*events) != 0 {
		t.Fatalf("%s：G10 丢完为空不得 fire 码 4（spec 11.2 原则 1「码=事实」），实得 %v", label, ccEvtActors(*events))
	}
}

// ═══ 正向对照 ═══

// TestIssue141G10NonBlankCcStillCreatesAndFires 归一不许把正常抄送走掉：两个有效的人
// 照旧逐人建行、逐人 fire 码 4（顺序与入参一致）。这一格按设计**改前也不红**（对照组）。
func TestIssue141G10NonBlankCcStillCreatesAndFires(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil

	manualRaw141G10(t, f, instanceID, []string{"7501", "7502"})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7501", "7502"}) {
		t.Fatalf("正向对照：非空抄送人应照旧逐人落行，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7501", "7502"}) {
		t.Fatalf("正向对照：照旧逐人 fire 码 4，实得 %q", got)
	}
}

// ═══ 手动腿（门面 processInstance/createCCInstance）───

// TestIssue141G10ManualLegAllBlankSameAsMissingActorIds 全空白集合 ⇒ 不建行、不 fire，
// 并且与本仓既有的"空 actorIds"档**同判**（实现要求④：沿用 `actorIds 缺失`，不新造错误码或文案）。
//
// 改前红：[]string{"", "   "} 在手动腿既不 trim 也不丢空 ⇒ code=0 成功＋两行 actor_id 为空/纯空白
// 的 cc 行＋两支码 4（本仓 G10 的主病灶，java 那边是 `"".split(",")` 的同一条）。
func TestIssue141G10ManualLegAllBlankSameAsMissingActorIds(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)

	allBlank := start141(t, f, defineID, nil)
	*events = nil
	resp := manualRaw141G10(t, f, allBlank, []string{"", "   "})

	if code, _ := resp["code"].(int); code == 0 {
		t.Fatalf("G10：全空白不得报成功（丢完为空＝没给抄送人），实得 %v", resp)
	}
	if msg, _ := resp["msg"].(string); !strings.Contains(msg, "actorIds 缺失") {
		t.Fatalf("G10：全空白须与既有\"空 actorIds\"档同判据（沿用原文案，不新造），实得 msg=%q", msg)
	}
	assertNoCcRow141G10(t, repo, allBlank, events, "手动腿全空白 []string{\"\", \"   \"}")

	// 同档铁证：空集合与全空白的返回逐字一致（同一支错误，不是两个新语义）
	empty := start141(t, f, defineID, nil)
	respEmpty := manualRaw141G10(t, f, empty, []interface{}{})
	if got, _ := resp["msg"].(string); got != respEmpty["msg"] {
		t.Fatalf("全空白与空集合必须同文案：全空白=%q 空集合=%q", got, respEmpty["msg"])
	}
	if got, want := resp["code"], respEmpty["code"]; got != want {
		t.Fatalf("全空白与空集合必须同 code：实得 %v vs %v", got, want)
	}

	// 纯空白字符串形（"   "）同样落这一档
	blankStr := start141(t, f, defineID, nil)
	*events = nil
	resp2 := manualRaw141G10(t, f, blankStr, "   ")
	if code, _ := resp2["code"].(int); code == 0 {
		t.Fatalf("G10：逗号串纯空白不得报成功，实得 %v", resp2)
	}
	assertNoCcRow141G10(t, repo, blankStr, events, "手动腿逗号串纯空白 \"   \"")
}

// TestIssue141G10ManualLegDropsBlankElementsKeepsValidOnes 混着给 ⇒ 只丢空元素，
// 有效的人照旧建行＋fire（且 fire 的入参里不得混进空值）。
//
// 改前红：[]string{"7601","","  ","7602"} 落 4 行（含 "" 与 "  "）＋ fire 4 支。
func TestIssue141G10ManualLegDropsBlankElementsKeepsValidOnes(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil

	mustManualOk141G10(t, f, instanceID, []string{"7601", "", "  ", "7602"})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7601", "7602"}) {
		t.Fatalf("G10：数组里的空元素丢弃、有效元素保留，实得 %q", got)
	}
	if n := len(repo.CcRowsForTest(instanceID)); n != 2 {
		t.Fatalf("G10：不得落出 actor_id 为空的行，应恰好 2 行，实得 %d", n)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7601", "7602"}) {
		t.Fatalf("G10：fire 的入参只含有效的人，实得 %q", got)
	}
}

// TestIssue141G10ManualLegNilElementIsNotAnActor 数组里的 nil 元素（JSON body `[null]` 的
// 真实形状）＝空元素，丢弃；**不得**被 fmt.Sprintf 成字面量 "<nil>" 当成一个正常抄送人。
//
// 改前红：手动腿 []interface{}{"7651", nil, ""} ⇒ 落 2 行 ["7651" "<nil>"]＋fire 2 支；
// 全 nil/全空的 []interface{}{"", nil} ⇒ 落 1 行 "<nil>" 且报成功。
func TestIssue141G10ManualLegNilElementIsNotAnActor(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil

	mustManualOk141G10(t, f, instanceID, []interface{}{"7651", nil, ""})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"7651"}) {
		t.Fatalf("G10：nil 元素应与空串同档丢弃，不得变成 \"<nil>\" 归属值，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7651"}) {
		t.Fatalf("G10：码 4 不得发给 \"<nil>\"，实得 %q", got)
	}

	allNil := start141(t, f, defineID141(t, f), nil)
	*events = nil
	resp := manualRaw141G10(t, f, allNil, []interface{}{"", nil})
	if code, _ := resp["code"].(int); code == 0 {
		t.Fatalf("G10：[]interface{}{\"\", nil} 丢完为空 ⇒ 与空 actorIds 同档，实得 %v", resp)
	}
	assertNoCcRow141G10(t, repo, allNil, events, "手动腿全空/nil 形")
}

// ═══ 发起腿 f_ccActors（逗号串 / 数组两种形态同判据）───

// TestIssue141G10StartLegEmptyAndBlankCreateNoRow 发起腿给空串/纯空白 ⇒ 不建行、不 fire。
// （本仓 strings.Split("", ",") 得到 [""] 而非 java 的 [""]→一个空元素，归一后同样丢完为空；
// 这一格是把"逗号串与数组两形同判据"钉在发起腿上，别只修一条腿。）
func TestIssue141G10StartLegEmptyAndBlankCreateNoRow(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	*events = nil

	for _, v := range []interface{}{"", "   ", []string{"", "   "}, []interface{}{"", nil}} {
		instanceID := start141(t, f, defineID, v)
		assertNoCcRow141G10(t, repo, instanceID, events, "发起腿全空白档")
	}
}

// TestIssue141G10StartLegCommaStringDropsEmptyElements 逗号串里的空元素（"7701,,7702"）丢弃，
// 两个人照旧建行＋fire；尾随逗号（"7701,"）同样不得建空行。
func TestIssue141G10StartLegCommaStringDropsEmptyElements(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	*events = nil

	inst := start141(t, f, defineID, "7701,,7702")
	if got := ccActors141(repo, inst); !eqStrs(got, []string{"7701", "7702"}) {
		t.Fatalf("G10：逗号串空元素丢弃，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7701", "7702"}) {
		t.Fatalf("G10：逐有效人 fire，实得 %q", got)
	}

	*events = nil
	tail := start141(t, f, defineID, "7751,")
	if got := ccActors141(repo, tail); !eqStrs(got, []string{"7751"}) {
		t.Fatalf("G10：尾随逗号不得建空行，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7751"}) {
		t.Fatalf("G10：尾随逗号只 fire 那一个有效的人，实得 %q", got)
	}
}

// TestIssue141G10StartLegCollectionDropsBlankElements 数组形态给空元素 ⇒ 与逗号串同一判据
// （逗号串修好了、数组腿漏修＝本条要抓的形状）。
func TestIssue141G10StartLegCollectionDropsBlankElements(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	*events = nil

	inst := start141(t, f, defineID, []interface{}{"7801", "", "  "})
	if got := ccActors141(repo, inst); !eqStrs(got, []string{"7801"}) {
		t.Fatalf("G10：数组形态与逗号串同判据，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"7801"}) {
		t.Fatalf("G10：数组形态只 fire 有效的人，实得 %q", got)
	}
}

// ═══ 办理腿 tf_ccActors ═══

// TestIssue141G10ExecuteLegBlankStringCreatesNoRow 办理腿给纯空白串 ⇒ 不建行、不 fire。
func TestIssue141G10ExecuteLegBlankStringCreatesNoRow(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	*events = nil

	for _, v := range []interface{}{"   ", "", []interface{}{""}} {
		instanceID := start141(t, f, defineID, nil)
		execute141(t, f, repo, instanceID, v)
		assertNoCcRow141G10(t, repo, instanceID, events, "办理腿全空白档")
	}
}

// TestIssue141G10ExecuteLegDropsBlankKeepsValidActors 办理腿混给空元素（尾随逗号／数组空元素）
// ⇒ 只丢空的，有效的人照旧建行＋fire。
func TestIssue141G10ExecuteLegDropsBlankKeepsValidActors(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)

	instanceID := start141(t, f, defineID, nil)
	*events = nil
	execute141(t, f, repo, instanceID, "8201,")
	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"8201"}) {
		t.Fatalf("G10：办理腿尾随逗号不得建空行，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"8201"}) {
		t.Fatalf("G10：办理腿只 fire 有效的人，实得 %q", got)
	}

	second := start141(t, f, defineID, nil)
	*events = nil
	execute141(t, f, repo, second, []string{"8251", "", "  ", "8252"})
	if got := ccActors141(repo, second); !eqStrs(got, []string{"8251", "8252"}) {
		t.Fatalf("G10：办理腿数组空元素丢弃，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"8251", "8252"}) {
		t.Fatalf("G10：办理腿数组形态 fire 入参不含空值，实得 %q", got)
	}
}

// ═══ trim 后同值＝同一个人（实现要求②，与 G2 判重咬合）───

// TestIssue141G10CcActorValuesAreTrimmed 落库值取 trim 后的串：" 8301 " 与 "8301" 是同一个人。
// 手动腿数组形改前**不 trim** ⇒ 落出 actor_id=" 8301 " 的行（本格红）。
func TestIssue141G10CcActorValuesAreTrimmed(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)
	*events = nil

	manual := start141(t, f, defineID, nil)
	mustManualOk141G10(t, f, manual, []string{" 8301 ", "8302"})
	if got := ccActors141(repo, manual); !eqStrs(got, []string{"8301", "8302"}) {
		t.Fatalf("G10：手动腿入库值应是 trim 后的串，实得 %q", got)
	}
	if got := ccEvtActors(*events); !eqStrs(got, []string{"8301", "8302"}) {
		t.Fatalf("G10：fire 的入参也得是 trim 后的串，实得 %q", got)
	}

	// 逗号串与数组两形的 trim 同判据（发起腿带前后空格）
	*events = nil
	started := start141(t, f, defineID, " 8351 , 8352 ")
	if got := ccActors141(repo, started); !eqStrs(got, []string{"8351", "8352"}) {
		t.Fatalf("G10：逗号串两端的空格同样剥掉，实得 %q", got)
	}
}

// TestIssue141G10PaddedValueHitsTheDedupRule 先抄 "8401" 再抄 " 8401 " ⇒ 写侧判重必须命中：
// 仍是 1 行、0 新 fire。不 trim 就会让同一人落两行，把 G2（本仓 ed29661）的判据打穿。
func TestIssue141G10PaddedValueHitsTheDedupRule(t *testing.T) {
	f, repo, events := setupCc141(t)
	defineID := defineID141(t, f)

	instanceID := start141(t, f, defineID, nil)
	mustManualOk141G10(t, f, instanceID, "8401")

	*events = nil
	tick141()
	mustManualOk141G10(t, f, instanceID, []string{" 8401 "})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"8401"}) {
		t.Fatalf("G10：带空格的同一人不得再建第二行（trim 与 G2 判重同一条尺子），实得 %q", got)
	}
	if n := len(repo.CcRowsForTest(instanceID)); n != 1 {
		t.Fatalf("G10：判重命中 ⇒ 仍是 1 行，实得 %d", n)
	}
	if len(*events) != 0 {
		t.Fatalf("G10：判重命中没有\"创建\"这个事实 ⇒ 不得 fire 码 4，实得 %v", ccEvtActors(*events))
	}
}

// ═══ 反向哨兵（实现要求④）───

// TestIssue141G10NormalActorIdsAreNotMistakenForBlank 判据只吃空值，不吃"看起来像空"的正常 id：
// "0"、"false"、"null" 字面串都是有效抄送人，必须照旧建行＋fire。这一格按设计**改前也不红**。
func TestIssue141G10NormalActorIdsAreNotMistakenForBlank(t *testing.T) {
	f, repo, events := setupCc141(t)
	instanceID := start141(t, f, defineID141(t, f), nil)
	*events = nil

	mustManualOk141G10(t, f, instanceID, []string{"0", "user-1"})

	if got := ccActors141(repo, instanceID); !eqStrs(got, []string{"0", "user-1"}) {
		t.Fatalf("G10 只丢空串/纯空白：\"0\" 这类正常 id 不得被吃掉，实得 %q", got)
	}
	if len(*events) != 2 {
		t.Fatalf("反向哨兵：照旧逐人 fire，实得 %d 支", len(*events))
	}

	// "null"/"false" 字面串同档：归一判据只看"是不是空白"，不看值长什么样
	second := start141(t, f, defineID141(t, f), nil)
	*events = nil
	mustManualOk141G10(t, f, second, []string{"null", "false", "0"})
	if got := ccActors141(repo, second); !eqStrs(got, []string{"null", "false", "0"}) {
		t.Fatalf("反向哨兵：字面量串不得被当空值丢掉，实得 %q", got)
	}
}
