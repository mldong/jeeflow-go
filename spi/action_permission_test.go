package spi

import "testing"

// action → 权限码映射默认件的跨栈对齐测试（issues/248；语义参照实现＝java
// DefaultActionPermissionProvider，其测试是权威）。断言的是"与 java 逐条一致"，
// 不是"与我这份实现一致"——改这张表必须同时改 java 那份，否则本卡必红。

func sameSet(t *testing.T, got, want []string, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: 码数不符 got=%v want=%v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: 第 %d 枚不符 got=%v want=%v", label, i, got, want)
		}
	}
}

func TestDefaultPermissionCodesThreeWays(t *testing.T) {
	// ① 默认派生：wf:{action 的 / 换 :}
	sameSet(t, PermissionCodes("processDesign/save"), []string{"wf:processDesign:save"}, "默认派生")
	// ② OR 族（boot3 @SaCheckPermission(mode=OR) 语义：任选其一）
	sameSet(t, PermissionCodes("processDefine/detail"),
		[]string{"wf:processDefine:detail", "wf:processDesign:listByType"}, "OR 两码")
	sameSet(t, PermissionCodes("processTask/candidatePage"),
		[]string{"wf:processTask:execute", "wf:processTask:candidatePage"}, "OR 复用 execute")
	// ③ 放行族：nil／空 = 登录即可
	if got := PermissionCodes("processInstance/detail"); got != nil {
		t.Fatalf("processInstance/detail 应放行，实得 %v", got)
	}
	for _, stats := range []string{"processInstance/stats/overview", "processInstance/stats/trend",
		"processInstance/stats/group"} {
		if got := PermissionCodes(stats); got != nil {
			t.Fatalf("统计 action %s 应放行（issues/103），实得 %v", stats, got)
		}
	}
}

// TestNewActionsRideExecuteCode 钉 issues/246／247 的 B 案粒度：两枚新 action 复用"办理"这枚码，
// 不派生成 wf:processTask:recall／wf:processTask:revokeDynamicCountersign，也不新建菜单按钮种子。
func TestNewActionsRideExecuteCode(t *testing.T) {
	for _, action := range []string{"processTask/recall", "processTask/revokeDynamicCountersign"} {
		sameSet(t, PermissionCodes(action), []string{"wf:processTask:execute"}, action+" 须复用 execute")
	}
	// 反向哨兵：没有配进两张表的 action 必须仍走默认派生，
	// 否则"整族都被塞进 execute"这种错法会被本测试放过。
	sameSet(t, PermissionCodes("processTask/transfer"), []string{"wf:processTask:transfer"}, "哨兵 transfer")
	sameSet(t, PermissionCodes("processTask/removeTaskActor"),
		[]string{"wf:processTask:removeTaskActor"}, "哨兵 removeTaskActor")
}

// TestTableShapeMatchesJava 把两张表的条目数钉住（与 java OR_RULES 8 条／NO_PERM_ACTIONS 11 条对齐）。
// 这里刻意写死数字：它不是"会漂的读数"，而是跨栈契约的形状锚——加一条规则要八栈同批改。
// issues/253 动态协办 processTask/aidant 复用 execute 码进 OR 表 ⇒ 7→8（本轮同批 +1）。
func TestTableShapeMatchesJava(t *testing.T) {
	if len(actionPermOrRules) != 8 {
		t.Fatalf("OR 规则条数应为 8（对齐 java DefaultActionPermissionProvider），现读 %d", len(actionPermOrRules))
	}
	if len(actionPermNoPerm) != 11 {
		t.Fatalf("放行清单条数应为 11（含统计三条），现读 %d", len(actionPermNoPerm))
	}
}

// TestOverrideAndFallback 覆盖注册生效、传 nil 回落内置默认件（java ServiceContext 同语义）。
func TestOverrideAndFallback(t *testing.T) {
	defer SetActionPermissionProvider(nil) // 用例之间不互相污染
	SetActionPermissionProvider(customPerm{})
	if got := PermissionCodes("anything/here"); len(got) != 1 || got[0] != "custom:1" {
		t.Fatalf("覆盖注册未生效，实得 %v", got)
	}
	SetActionPermissionProvider(nil)
	sameSet(t, PermissionCodes("processDesign/save"), []string{"wf:processDesign:save"}, "回落后默认件")
}

type customPerm struct{}

func (customPerm) PermissionCodes(string) []string { return []string{"custom:1"} }

// TestProviderMetaExposed SPI 清单里要能看见内置默认件（与 java／c#／rust 的元数据约定同形）。
func TestProviderMetaExposed(t *testing.T) {
	typ, class := ActionPermissionProviderMeta()
	if typ != "ActionPermissionProvider" || class != "com.mldong.jeeflow.spi.DefaultActionPermissionProvider" {
		t.Fatalf("元数据条目不符：%s / %s", typ, class)
	}
}
