package spi

// ActionPermissionProvider —— action → 权限码映射提供者（issues/29 的跨语言续腿，hub issues/248）。
//
// 边界与 java `com.mldong.jeeflow.spi.IActionPermissionProvider` 逐字同口径：
// 引擎**不依赖任何鉴权框架**（sa-token / JWT / 中间件都是宿主框架的事），
// 本 SPI 只提供"action → 权限码"这一份**元数据**；校验执行仍由集成方框架层完成。
//
// 为什么要在引擎里放这张表（而不是各集成壳自备）：现读十壳各抄一份
// （go 三壳 wf_permission.go／python 三壳 wf_permission.py／nestjs wf-permission.ts／
// laravel WfPermission.php／salvo wf_permission.rs／csharp WfEndpoints.cs），
// 新 action 或粒度调整要改十处＋引擎，且"只用引擎、自写集成层"的客户跨栈读出两套码。
// 语义参照实现是 java，本文件与其 OR／放行两张表**逐条一致**。
type ActionPermissionProvider interface {
	// PermissionCodes 返回 action 的权限码集合（**OR 语义**——任一持有即有权）。
	// 返回 nil／空切片 = 放行（登录即可）。
	PermissionCodes(action string) []string
}

// actionPermDefault OR 规则表（对齐 java DefaultActionPermissionProvider.OR_RULES）
var actionPermOrRules = map[string][]string{
	"processDefine/detail":             {"wf:processDefine:detail", "wf:processDesign:listByType"},
	"processDefine/startAndExecute":    {"wf:processDefine:startAndExecute", "wf:processDesign:listByType"},
	"processDefine/getLastByName":      {"wf:processDefine:detail", "wf:processDesign:listByType", "wf:processDefine:getLastByName"},
	"processTask/candidatePage":        {"wf:processTask:execute", "wf:processTask:candidatePage"},
	"processTask/jumpAbleTaskNameList": {"wf:processTask:execute"},
	// issues/246／247 两枚新 action 定 B 案（owner 2026-11-04）：**复用"办理"这枚码**，不另建菜单按钮种子。
	// 判据是既有惯例——transfer／removeTaskActor／candidatePage／latest 都没有 sys_menu 按钮行，
	// candidatePage／jumpAbleTaskNameList 就是走本表复用 execute；而撤回／撤会签的主体
	// 按引擎判据本来就必然是该行参与者（＝有办理权的人），拆独立码实际给不出去。
	"processTask/recall":                   {"wf:processTask:execute"},
	"processTask/revokeDynamicCountersign": {"wf:processTask:execute"},
	// issues/253 动态协办：照 246/247 B 案惯例复用"办理"这枚码，不建按钮种子行——
	// 派生协办的主体按引擎判据本来就必然是主办行名册内的人（＝有办理权的人）。
	"processTask/aidant": {"wf:processTask:execute"},
}

// actionPermNoPerm 放行清单（对齐 java NO_PERM_ACTIONS；统计三条是 issues/103 的收口成果）
var actionPermNoPerm = map[string]bool{
	"processInstance/detail":              true,
	"processInstance/highLight":           true,
	"processInstance/approvalRecord":      true,
	"processInstance/getAssigneeTextData": true,
	"processInstance/bizData":             true,
	"processTask/detail":                  true,
	"processTask/addCandidate":            true,
	"processTask/latest":                  true,
	"processInstance/stats/overview":      true,
	"processInstance/stats/trend":         true,
	"processInstance/stats/group":         true,
}

// DefaultActionPermissionProvider 引擎内置默认实现（java
// DefaultActionPermissionProvider 的逐条同款）：放行 → OR → 默认 wf:{action 的 / 换 :}。
type DefaultActionPermissionProvider struct{}

// PermissionCodes 见 ActionPermissionProvider。
func (DefaultActionPermissionProvider) PermissionCodes(action string) []string {
	if actionPermNoPerm[action] {
		return nil
	}
	if or, ok := actionPermOrRules[action]; ok {
		return or
	}
	return []string{"wf:" + replaceAllSlash(action)}
}

// replaceAllSlash 把 action 里所有 '/' 换成 ':'（默认权限码规则）。
func replaceAllSlash(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c == '/' {
			out[i] = ':'
		}
	}
	return string(out)
}

// ─── 可选覆盖位（java ServiceContext.find(IActionPermissionProvider) 的同位）──────────
//
// 与 java 一致的兜底语义：**没注册就等于用内置默认件**，集成方不配也有统一权限语义；
// 特殊集成方（另一套权限码体系）覆盖注册后按自家规则走。

var actionPermissionProvider ActionPermissionProvider = DefaultActionPermissionProvider{}

// SetActionPermissionProvider 覆盖注册（传 nil 回落到内置默认件）。
func SetActionPermissionProvider(p ActionPermissionProvider) {
	if p == nil {
		actionPermissionProvider = DefaultActionPermissionProvider{}
		return
	}
	actionPermissionProvider = p
}

// ActionPermissionProviderOrDefault 取当前生效的提供者（恒非 nil，故调用方不必判空）。
func ActionPermissionProviderOrDefault() ActionPermissionProvider {
	return actionPermissionProvider
}

// PermissionCodes 便捷出口：按当前生效提供者取 action 的权限码集合。
func PermissionCodes(action string) []string {
	return ActionPermissionProviderOrDefault().PermissionCodes(action)
}

// ActionPermissionProviderMeta 引擎元数据清单条目（与 java HandlerRegistry／c# Metadata.cs
// ／rust metadata.rs 同一约定：把内置默认件也暴露给前端设计器的 SPI 字典源）。
func ActionPermissionProviderMeta() (string, string) {
	return "ActionPermissionProvider", "com.mldong.jeeflow.spi.DefaultActionPermissionProvider"
}
