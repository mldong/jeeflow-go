package spi

import "strings"

// NormalizeCcActors 抄送人集合归一（issues/141 G10「空抄送人不建 cc 行」·
// spec 06-facade.md §2.10，基准＝Java StringUtils.normalizeCcActors）。
//
// 本函数是**这一条判据在本仓的唯一落点**，三条入口与两仓写侧共用它，不要再抄第二份：
//   - 漏斗层：引擎 HandleCcActors 的入参解析（逗号串／[]string／[]interface{}）、
//     门面手动腿 createCCInstance 的 actorIds 解析；
//   - 写侧层：内存仓与 JDBC 仓的 CreateCcInstance／CreateCcInstanceIfAbsent 各自再挡一次
//     （绕过引擎/门面直连仓储的调用方同样建不出空行，见 ProcessRepository.CreateCcInstance）。
//
// 判据（逐元素）：
//  1. trim 后再落库/比较——" 123 " 与 "123" 是同一个人；不 trim 会和 issues/141 G2 的
//     写侧判重错开，让同一人落两行，把 G2 的判据打穿；
//  2. 空串、纯空白丢弃；
//  3. 同一次调用内的重复折叠，顺序保持。
//
// 丢完为空 ⇒ 调用方**不得建任何 cc 行、也不得 fire CC_CREATE（码 4）**；手动腿这一档与
// 本仓既有的"空 actorIds"同判（沿用 `actorIds 缺失`，不新造错误码或文案）。
//
// ⚠️ 反向哨兵：判据只吃空值，不吃"看起来像空"的正常 id——"0" 是有效抄送人，不得被丢掉。
func NormalizeCcActors(raw ...string) []string {
	var out []string
	for _, actorID := range raw {
		trimmed := strings.TrimSpace(actorID)
		if trimmed == "" {
			continue
		}
		if !containsCcActor(out, trimmed) {
			out = append(out, trimmed)
		}
	}
	return out
}

func containsCcActor(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
