// issues/139 的另一半：门面内部**返回的 error 对象**仍带着原始异常（错误链），
// 只有对外信封的 msg 字段被收敛成逐字固定文案。
//
// 为什么这一格要单独测（facade_msg_139_test.go 只看了出口 msg）：
// 修复姿势是"facadeFixedError{msg 固定, cause 原始}"。若哪天有人图省事直接
// `errors.New(msgReadFlowDefineJSONFailed)` 把 cause 丢了，出口 msg 照样绿，
// 但集成方 / 日志再也拿不到"到底是哪一行哪个字段解析不过"——排查能力静默归零。
// 本文件在 facade 包内测包内函数，把"cause 必须可 errors.As 取到"钉住。
package facade

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestIssue139DeployKeepsJsonSyntaxErrorInCause deploy：msg 固定 ＋ cause 是原始 *json.SyntaxError。
func TestIssue139DeployKeepsJsonSyntaxErrorInCause(t *testing.T) {
	f := &Facade{} // 解析在碰仓储之前就失败，无需装配
	_, err := f.deploy(map[string]interface{}{"content": `{"name":"broken",`})
	if err == nil {
		t.Fatal("坏 JSON 的 deploy 应返回 error")
	}
	// ① Error() 逐字固定 ⇒ 出口 msg（errorResult(err.Error())）拿到的就是这句
	if err.Error() != msgReadFlowDefineJSONFailed {
		t.Fatalf("Error() 应逐字等值 %q，实得 %q", msgReadFlowDefineJSONFailed, err.Error())
	}
	// ② 原始异常仍在链上：Unwrap 非空，且能按具体类型 As 出来
	if errors.Unwrap(err) == nil {
		t.Fatalf("cause 丢了：错误链只剩固定文案，集成方/日志无法定位真实解析错误 %v", err)
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("cause 应为 *json.SyntaxError，实得 %T: %v", err, err)
	}
	// ③ 出口映射（顶层 errorResult(err.Error())）确实用的就是这句
	if got := errorResult(err.Error())["msg"]; got != msgReadFlowDefineJSONFailed {
		t.Fatalf("失败信封 msg 应为固定文案，实得 %v", got)
	}
}

// TestIssue139RedeployKeepsJsonTypeErrorInCause redeploy：类型不符走 *json.UnmarshalTypeError，
// 出口 msg 与语法错档**完全同一条**（跨栈逐字文案），差异只体现在 cause 上。
func TestIssue139RedeployKeepsJsonTypeErrorInCause(t *testing.T) {
	f := &Facade{}
	err := f.redeploy(map[string]interface{}{
		"processDefineId": int64(1), "content": `{"name":123}`,
	})
	if err == nil {
		t.Fatal("坏 JSON 的 redeploy 应返回 error")
	}
	if err.Error() != msgReadFlowDefineJSONFailed {
		t.Fatalf("Error() 应逐字等值 %q，实得 %q", msgReadFlowDefineJSONFailed, err.Error())
	}
	var ute *json.UnmarshalTypeError
	if !errors.As(err, &ute) {
		t.Fatalf("cause 应为 *json.UnmarshalTypeError，实得 %T: %v", err, err)
	}
}
