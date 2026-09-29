// issues/139 门面出口 msg（go 腿）——现读三处把底层异常拼进了对外 msg：
//
//	deploy / redeploy / designRedeploy 原写法 `fmt.Errorf("流程定义 JSON 解析失败: %w", err)`，
//	而门面顶层出口是 `errorResult(err.Error())`（facade.go:192）⇒ 失败信封的 msg 字段
//	带上 "unexpected end of JSON input" / "invalid character '<' …" 这类**底层异常原文**，
//	逐条 JSON 出错文案还各不相同（前端按 msg 分支即失效，且泄漏实现细节）。
//
// 裁定口径：msg 必须是**逐字固定文案**「读取流程定义 JSON 失败」，与 Java 参考实现同源
// （jeeflow-core ModelParser.java:47 `new RuntimeException("读取流程定义 JSON 失败", e)`
// ＋ JeeflowFacade.java:177 `error(e.getMessage())`——getMessage() 就是这句、cause 不进 msg）。
//
// 本文件从**门面出口**取证，非法 JSON 三种形状各测一遍：① 截断 ② 类型不符 ③ 根本不是 JSON。
// 断言用**逐字相等**而非"包含"（issues/134 同口径：宽松匹配会让"固定文案 + 尾巴"蒙过去）。
// 三档 msg 必须**完全一致** ⇒ 这本身就是"异常没漏进 msg"的铁证（漏了就会随 JSON 错误而异）。
package facade_test

import (
	"strings"
	"testing"
)

// want139 逐字固定文案（对齐 Java ModelParser.java:47 的 getMessage()）
const want139 = "读取流程定义 JSON 失败"

// badJSON139 三种非法 JSON：底层 err.Error() 天然互不相同（截断 / 类型 / 语法）。
var badJSON139 = []struct {
	label   string
	content string
}{
	{"截断", `{"name":"broken",`},                   // unexpected end of JSON input
	{"类型不符", `{"name":123,"displayName":"类型不符"}`}, // cannot unmarshal number into Go struct field …
	{"根本不是 JSON", `<xml><flow name="x"/></xml>`},  // invalid character '<' …
}

// mustFixedMsg139 断言失败信封：code=99999999 ＋ msg 逐字等值固定文案 ＋ 不含底层异常痕迹。
func mustFixedMsg139(t *testing.T, label string, r map[string]interface{}) {
	t.Helper()
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s 应失败且 code=99999999, got %v", label, r)
	}
	msg, ok := r["msg"].(string)
	if !ok {
		t.Fatalf("%s msg 应为 string，实得 %T (%v)", label, r["msg"], r["msg"])
	}
	if msg != want139 {
		t.Fatalf("%s msg 应逐字等值 %q，实得 %q ⇒ 要么异常漏进 msg，要么文案漂移", label, want139, msg)
	}
	// 兜底网：Go encoding/json 异常文案的标志性片段，一个都不许出现在 msg 里
	for _, leak := range []string{"invalid character", "unexpected end", "cannot unmarshal", "go struct field"} {
		if strings.Contains(strings.ToLower(msg), leak) {
			t.Fatalf("%s msg 含底层异常痕迹 %q: %q", label, leak, msg)
		}
	}
}

// TestIssue139DeployMsgIsFixedText processDefine/deploy：坏 JSON ⇒ 固定文案，三种病灶同一条 msg。
func TestIssue139DeployMsgIsFixedText(t *testing.T) {
	f, _, _ := setupFacade()
	for _, c := range badJSON139 {
		r := f.Flow("processDefine/deploy", map[string]interface{}{"content": c.content, "operator": "u139"})
		mustFixedMsg139(t, "deploy/"+c.label, r)
	}
}

// TestIssue139RedeployMsgIsFixedText processDefine/redeploy：同一口径（门面另一条腿）。
func TestIssue139RedeployMsgIsFixedText(t *testing.T) {
	f, _, _ := setupFacade()
	for _, c := range badJSON139 {
		r := f.Flow("processDefine/redeploy", map[string]interface{}{
			"processDefineId": int64(91390001), "content": c.content, "operator": "u139",
		})
		mustFixedMsg139(t, "redeploy/"+c.label, r)
	}
}

// TestIssue139DesignRedeployMsgIsFixedText processDesign/redeploy：内容快照取自设计历史，
// 快照本身是坏 JSON ⇒ 同一条固定文案；同一条快照再走 processDesign/deploy（内部复用 deploy）
// 也必须是同一条 —— 两条腿共用一个出口口径。
func TestIssue139DesignRedeployMsgIsFixedText(t *testing.T) {
	f, _, _ := setupFacade()
	for _, c := range badJSON139 {
		// designSave 不解析 content（facade.go designSave 只走 contentBytes）⇒ 坏 JSON 也能存进快照
		r := f.Flow("processDesign/save", map[string]interface{}{
			"name": "d139", "displayName": "139 取证设计", "content": c.content, "operator": "u139",
		})
		if code, _ := r["code"].(int); code != 0 {
			t.Fatalf("save(%s) 夹具失效：坏快照应能存进去: %v", c.label, r)
		}
		designID := mustI64(r["data"].(map[string]interface{})["id"])

		r = f.Flow("processDesign/redeploy", map[string]interface{}{"id": designID, "operator": "u139"})
		mustFixedMsg139(t, "designRedeploy/"+c.label, r)

		r = f.Flow("processDesign/deploy", map[string]interface{}{"id": designID, "operator": "u139"})
		mustFixedMsg139(t, "designDeploy/"+c.label, r)
	}
}

// TestIssue139GoodJsonStillDeploys 回归护栏：修 msg 不能把合法 JSON 一起拦掉。
func TestIssue139GoodJsonStillDeploys(t *testing.T) {
	f, _, _ := setupFacade()
	content := string(flowContent(t, "01-simple.json"))
	if r := f.Flow("processDefine/deploy", map[string]interface{}{"content": content, "operator": "u139"}); r["code"].(int) != 0 {
		t.Fatalf("合法 JSON deploy 应成功: %v", r)
	}
	if r := f.Flow("processDefine/redeploy", map[string]interface{}{
		"processDefineId": int64(1), "content": content, "operator": "u139",
	}); r["code"].(int) != 0 {
		t.Fatalf("合法 JSON redeploy 应成功: %v", r)
	}
}

// TestIssue139AdjacentMsgsUnchanged 只收 139 这一面：同族相邻固定文案不得被带跑。
func TestIssue139AdjacentMsgsUnchanged(t *testing.T) {
	f, _, _ := setupFacade()
	// 合法 JSON 但缺 name ⇒ 仍是「流程定义缺少 name」，不是 139 那条
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": `{"displayName":"无名"}`, "operator": "u139"})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("缺 name 应失败: %v", r)
	}
	if got, _ := r["msg"].(string); got != "流程定义缺少 name" {
		t.Fatalf("缺 name 文案应保持不变，实得 %q", got)
	}
	// content 整条缺失 ⇒ 「content 缺失」
	r = f.Flow("processDefine/deploy", map[string]interface{}{"operator": "u139"})
	if got, _ := r["msg"].(string); got != "content 缺失" {
		t.Fatalf("content 缺失文案应保持不变，实得 %q", got)
	}
}
