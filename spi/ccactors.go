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
// ⚠️ **删除腿例外**：两仓 RemoveTaskActor 不直接调本函数，走 [ActorDeleteForms]
// （它内部仍逐元素调本函数取 trim 形与判空，只额外把原值形也放进集合）。删除腿只留 trim 形
// 就是 spec 语义 6 点名的那种假成功——门面交出脏行原值 " 9101 "，被削成 9101 后真库删不掉那一行。
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

// ActorDeleteForms 归属值**删除腿**展开（issues/137 §3-6 · spec 06-facade.md
// §processTask/removeTaskActor 语义 6 ＋ §2.11 写点表末行，owner 2026-10-02 拍「两形并集」）：
// 把待删的归属值列表展开成 `DELETE ... WHERE actor_id IN (...)` 真正要绑的值——
// **空值一律丢弃，非空值同时保留「原值」与「trim 值」两形**（按字面去重、保序）。
//
// 为什么必须两形、只取一头各有一种假成功（1.8.36 之前八栈正好分成这两派，没有一处两全）：
//   - 只取 **trim 形**（php/csharp/rust/moon 四栈八处的旧形状）⇒ 门面按语义 6 交出的历史脏行
//     原值 " 9101 " 被削成 9101，真库（MySQL NO PAD 排序规则）下那一行删不掉，门面却报成功
//     ——**被摘的人待办还在**；
//   - 只取 **原值形**（go/node/python/java 四栈九处的旧形状，本仓改前也是这一派：两仓
//     RemoveTaskActor 裸传）⇒ 第三方绕过门面直连仓储传 " 8601 " 时删不掉写侧归一后落库的
//     规范行 8601（issues/142 §9.2 那一路）；且空值照喂 DELETE，会把历史 actor_id 空串 脏行
//     批量误删（那是替脏数据做掉唯一痕迹，issues/129 那族"空归属值读全库"的删除位对偶）。
//
// 两形并集同时满足两侧：脏行按原值形命中、规范行按 trim 形命中。按 §2.11 归一口径
// " 9101 " 与 9101 本就是**同一个人**，两行都删掉才是"摘掉这个人"的正确结果，不构成误删。
// 并集**包含 trim 形**，所以 issues/142 B 批"删除位 trim"的既有判据无需反向改。
//
// 判据本体只有一枚：trim 与判空**复用 [NormalizeActors]**（逐元素调它取归一形），本函数只加
// "原值也进集合"这一层，**不抄第二份 trim/判空代码**（spec §2.11 尾注：两份判据迟早分叉）。
// 去重按**字面**做（复用 [containsActor]），不按"trim 后相同"折叠原值形——
// " 9101 "（一个空格）与 "  9101  "（两个空格）是**两种不同的原值形**，都要保留，
// 库里可能正是其中任一种脏法，少带一种就删不掉那一行。
//
// ⚠️ 反向哨兵（§2.10④／§2.11④）：判空只吃"trim 后为空"，不吃"看起来像空"的正常 id——
// "0" 是合法 id 必须留下，且 "0" 与 "00" 是**两个不同的人**（严禁借 Go 的假值/len==0 之类判据）。
//
// ⚠️ `nil` 元素在 Go 侧到不了本函数（入参是 []string）：形态拆解那一层（门面 toStringSlice2／
// 引擎 valueToActors）必须先把 []interface{} 里的 nil 丢掉，**严禁**先 fmt.Sprint 成 "<nil>"
// 再传进来——那已经不是空值了，本判据吃不掉，还会误删一个真名叫 "<nil>" 的人。
//
// 调用方（两仓删除侧）的义务是三件事：① 本函数已经把空值丢干净；② 拿返回的并集去
// `DELETE ... IN`；③ **并集为空 ⇒ 早退，一条 DELETE 都不发**（不得退化成"清空该任务全部参与者"，
// 那是语义 5「至少需保留一名参与人」的仓储侧对偶）。内存仓与 SQL 仓必须同一条判据、同一个答案
// （issues/117 场景 27 那把尺子）。
//
// 返回 nil/空切片表示"没有可删的值"（入参为 nil、空列表，或元素全是空值）。
func ActorDeleteForms(raw ...string) []string {
	var out []string
	for _, actorID := range raw {
		// 判空与 trim 都问 NormalizeActors 那一枚（归一结果为空 ⇒ 该元素是空值）：
		// ① 空值一律丢弃，不喂 DELETE。
		normalized := NormalizeActors(actorID)
		if len(normalized) == 0 {
			continue
		}
		trimmed := normalized[0]
		if !containsActor(out, actorID) {
			out = append(out, actorID) // ② 原值形：保住未 trim 的历史脏行
		}
		if !containsActor(out, trimmed) {
			out = append(out, trimmed) // ② trim 形：保住写侧归一后的规范行
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
