package engine

import (
	"regexp"
	"strings"
	"testing"
)

// issues/232 R64 拍 A：autoGenTitle 钉秒 `yyyy-MM-dd HH:mm:ss`（Go 半边）。
// 变异＝格式串回退 "2006-01-02 15:04"（分钟档），两枚判据复红。
// 回归腿：expire_time 解析格见 expire_time_instance_137a_test.go（本判据不动该腿）。
func TestAutoGenTitleSecondsFormat232(t *testing.T) {
	vars := map[string]interface{}{KeyRealName: "张三"}
	e := &EngineImpl{}
	e.addAutoGenTitle("请假申请", vars)
	title, _ := vars[KeyAutoGenTitle].(string)

	secondsRe := regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	minuteRe := regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}$`)
	prefix := "张三的请假申请-"

	if !secondsRe.MatchString(title) {
		t.Fatalf("标题应以秒级时间戳结尾（钉秒 yyyy-MM-dd HH:mm:ss）: %q", title)
	}
	if minuteRe.MatchString(title) {
		t.Fatalf("标题不得停在分钟粒度（改前形状）: %q", title)
	}
	if !strings.HasPrefix(title, prefix) {
		t.Fatalf("标题前缀形状 {realName}的{displayName}- 应保持: %q", title)
	}
}
