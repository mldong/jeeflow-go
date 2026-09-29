package spi

import "strings"

// NormalizeActors **归属值（actor id）集合归一的唯一判据落点**。
//
// 立法出处两处，同一条尺子，只是表不同：
//   - issues/141 G10 · spec 06-facade.md §2.10「空抄送人不建 cc 行」→ `wf_process_cc_instance.actor_id`；
//   - issues/142 B 批 · spec 06-facade.md §2.11「归属值写侧归一」→ `wf_process_task_actor.actor_id`
//     （owner 2026-09-30 拍「八栈一起收：两形同判据＋写侧兜底＋trim＋哨兵」）。
//
// 本函数是**这一条判据在本仓的唯一落点**，两条表的所有入口与两仓写侧共用它，**不要再抄第二份**
// （spec §2.11 结尾点名：两份判据迟早分叉，php 那轮就实测到"归一函数内部严格比较、仓储写侧
// 却用松散 in_array ⇒ '0' == '00' 把第二个人静默丢掉"）。现在的共用面：
//   - 抄送侧漏斗层：引擎 parseCcActors（逗号串／[]string／[]interface{}）、门面手动腿 createCCInstance；
//   - 抄送侧写侧层：内存仓与 JDBC 仓的 CreateCcInstance／CreateCcInstanceIfAbsent；
//   - 任务侧漏斗层：门面 toStringSlice2（addCandidate／surrogate 的 actorIds）、
//     引擎 valueToActors（f_nextNodeOperator／tf_nextNodeOperator／assignee 命中变量）；
//   - 任务侧写侧层：内存仓与 JDBC 仓的 AddTaskActor；
//   - 单人入参（门面 transfer 的 fromActor／toActor、updateCCStatus 的 operator）也过这一枚，
//     取归一结果的那一个值。
//
// 判据（逐元素）：
//  1. trim 后再落库/比较——" 123 " 与 "123" 是同一个人；不 trim 会和 issues/141 G2 的
//     写侧判重错开，让同一人落两行，把 G2 的判据打穿；
//  2. 空串、纯空白丢弃（调用方在丢完为空时按"参数缺失"档报错／不建行／不 fire 码 4）；
//     `nil` 元素由**调用方**在本函数之前就丢掉——它们是形态拆解那一层的事，
//     严禁先 fmt.Sprint 成 "<nil>" 再交给本函数（那已经不是空值了，判据吃不掉）；
//  3. 同一次调用内的重复折叠，顺序保持。
//
// ⚠️ 反向哨兵（spec §2.10④／§2.11④）：判据只吃空值，不吃"看起来像空"的正常 id——
// "0" 是有效参与者，**不得**被丢掉；"0" 与 "00" 是**两个不同的人**（比较一律用字符串等值，
// 严禁借语言自带的假值判据或松散比较）。
//
// ⚠️ 主键类参数（processTaskId 等）**不套用本判据**：归属值可有可无，主键没有就是调用方写错了，
// 必须响亮报错，不得拿 空串/0 当 id 往下落库（spec 06 §2.11「主键类参数另判一档」）。
func NormalizeActors(raw ...string) []string {
	var out []string
	for _, actorID := range raw {
		trimmed := strings.TrimSpace(actorID)
		if trimmed == "" {
			continue
		}
		if !containsActor(out, trimmed) {
			out = append(out, trimmed)
		}
	}
	return out
}

// NormalizeCcActors 抄送人集合归一（issues/141 G10 · spec 06-facade.md §2.10）。
//
// **本函数只是 [NormalizeActors] 的旧名转发**，不是第二份判据：
// §2.11 要求任务侧复用 §2.10 已落地的那一枚单点，导出名换成通用的 NormalizeActors
// 属破坏性 API 变化 ⇒ 旧名保留一代，新代码请直接用 [NormalizeActors]。
//
// 判据、四层共用面、"0" 反向哨兵与主键另判一档，全部见 [NormalizeActors]。
//
// Deprecated: 请用 [NormalizeActors]（同一枚判据，通用名）。
func NormalizeCcActors(raw ...string) []string { return NormalizeActors(raw...) }

func containsActor(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
