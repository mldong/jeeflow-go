// 参与者**删除腿**归属值判据的单点测试（issues/137 §3-6 · Go 栈 · owner 2026-10-02 拍「两形并集」）。
//
// 立法逐字依据＝spec 06-facade.md **§processTask/removeTaskActor 语义 6** ＋ **§2.11** 写点表末行
// （删除腿那一行）。判据基准＝ jeeflow-java `TaskActorDeleteFormsTest`（java 基准腿 commit c48a6ba）。
//
// [spi.ActorDeleteForms] 是这条判据在本仓的唯一落点：两仓删除侧（`memory/repository.go`、
// `repository/jdbc/jdbc.go` 的 `RemoveTaskActor`）共用它，**不抄第二份 trim/判空**——
// trim 与判空的判据本体仍是 [spi.NormalizeActors] 那一枚，本函数只加"原值也进集合"这一层。
//
// 为什么是"两形并集"而不是只取一头（1.8.36 之前八栈正好分成相反的两派，各自都有一种假成功）：
//   - 只取 **trim 形**（php/csharp/rust/moon 四栈八处的旧形状）⇒ 门面按语义 6 交出的历史脏行原值
//     " 9101 " 被削成 9101，真库 NO PAD 排序规则下那一行删不掉而门面报成功——被摘的人待办还在；
//   - 只取 **原值形**（go/node/python/java 四栈九处的旧形状，**本仓改前也是这一派**：两仓裸传）
//     ⇒ 第三方绕过门面直连仓储传 " 8601 " 时删不掉写侧归一后的规范行 8601（issues/142 §9.2 那一路），
//     且空值照喂 DELETE，把历史 actor_id 空串 脏行批量误删（替脏数据做掉唯一痕迹）。
//
// 两仓删除腿的格子在 memory/task_actor_delete_forms_137_test.go 与
// repository/jdbc/task_actor_delete_forms_137_sqlite_test.go（同一份数据两仓必须同答案，
// issues/117 场景 27 那把尺子）。
package spi_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/spi"
)

// adf137Must 逐字比对（顺序也算判据：保序便于各栈 IN 列表与日志逐字对照）。
// want 为 nil 时按"空切片"判（本仓既有惯例：NormalizeActors 无命中返回 nil）。
func adf137Must(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(want) == 0 {
		if len(got) != 0 {
			t.Fatalf("%s：应为空，实得 %q", label, got)
		}
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s：实得 %q want %q", label, got, want)
	}
}

// TestIssue137ActorDeleteFormsDropsBlankEntirely 义务①：空串／纯空白／制表／换行一律丢弃，
// 一个都不进删除集合（否则历史 actor_id 空串 脏行会被 `IN` 里绑的那个空串批量误删）。
//
// 改前红样对照＝变异 C（仓储腿裸传）：本单点不存在，空值直喂 DELETE。
func TestIssue137ActorDeleteFormsDropsBlankEntirely(t *testing.T) {
	adf137Must(t, "语义 6 义务①：空值一律丢弃",
		spi.ActorDeleteForms("", "   ", "\t", "\n", "\r\n", " \t\r\n "), nil)
	adf137Must(t, "混给只丢空值，有效值两形照出",
		spi.ActorDeleteForms("", " 9101 ", "   "), []string{" 9101 ", "9101"})
}

// TestIssue137ActorDeleteFormsNilAndEmptyInputIsEmpty nil 入参／空列表 ⇒ 空切片。
// 调用方据此**早退，一条 DELETE 都不发**（义务③）——空切片不得退化成"清空该任务全部参与者"。
func TestIssue137ActorDeleteFormsNilAndEmptyInputIsEmpty(t *testing.T) {
	var nilSlice []string
	adf137Must(t, "nil 入参 ⇒ 空切片（早退，不发 DELETE）", spi.ActorDeleteForms(nilSlice...), nil)
	adf137Must(t, "空列表 ⇒ 空切片", spi.ActorDeleteForms([]string{}...), nil)
	adf137Must(t, "零个实参 ⇒ 空切片", spi.ActorDeleteForms(), nil)
}

// TestIssue137ActorDeleteFormsPaddedValueYieldsBothForms 义务②：带空格的值**同时**产出
// 「原值」与「trim 值」两形，且原值在前（保序）。
//
// 这一格是"假成功修复"的判据本体：只产出 trim 形（变异 A）⇒ 门面交出的脏行原值 " 9101 "
// 不在集合里，真库那一行删不掉而门面报成功。
func TestIssue137ActorDeleteFormsPaddedValueYieldsBothForms(t *testing.T) {
	adf137Must(t, "语义 6 义务②：原值形删脏行、trim 形删规范行",
		spi.ActorDeleteForms(" 9101 "), []string{" 9101 ", "9101"})
	adf137Must(t, "前导空格那一支同样两形（真库判据不随排序规则漂）",
		spi.ActorDeleteForms(" 8601"), []string{" 8601", "8601"})
}

// TestIssue137ActorDeleteFormsAlreadyTrimmedCollapsesToSingleForm 已 trim 过的值两形相同
// ⇒ 只一份，不得让 IN 列表白白翻倍（也不得让同一个人被删两次的日志噪声出现）。
//
// 这一格是 issues/142 §9.2「删除位 trim」既有判据在单点上的形状：入参 " 8601 " 必须产出
// 规范形 8601，才删得掉写侧归一后落库的规范行。只产出原值形（变异 B）⇒ 本格仍绿
// （"8601" 的原值形就是它自己），红的是仓储腿那格 normalizedRow…ByTrimmedForm。
func TestIssue137ActorDeleteFormsAlreadyTrimmedCollapsesToSingleForm(t *testing.T) {
	adf137Must(t, "已 trim 值只一份", spi.ActorDeleteForms("9101"), []string{"9101"})
	adf137Must(t, "中间空白是值的一部分（只 trim 两端）", spi.ActorDeleteForms("a b"), []string{"a b"})
}

// TestIssue137ActorDeleteFormsDedupsAcrossElementsByLiteral 跨元素去重：同一原值形重复给 ⇒ 折叠成一份。
//
// ⚠️ 去重按**字面**做，不按"trim 后相同"折叠原值形：`" 9101 "`（一个空格）与 `"  9101  "`（两个空格）
// 是**两种不同的原值形**，都得进集合——库里可能正是其中任一种脏法，少带一种就删不掉那一行。
func TestIssue137ActorDeleteFormsDedupsAcrossElementsByLiteral(t *testing.T) {
	adf137Must(t, "同一原值形重复给 ⇒ 折叠成一份",
		spi.ActorDeleteForms(" 9101 ", "9101", "9101", " 9101 "), []string{" 9101 ", "9101"})
	adf137Must(t, "不同原值形（一个空格／两个空格）各自保留，trim 形仍只一份",
		spi.ActorDeleteForms(" 9101 ", "  9101  "), []string{" 9101 ", "9101", "  9101  "})
}

// TestIssue137ActorDeleteFormsKeepsZeroLikeIdsDistinct 反向哨兵（§2.11 硬要求④）：
// 判据只吃"trim 后为空"，**不吃**"看起来像空"的正常 id。"0" 必须留下，且 "0" 与 "00" 是两个不同的人
// （严禁借 Go 的假值判据／len==0 之类把 "0" 吃掉）。
func TestIssue137ActorDeleteFormsKeepsZeroLikeIdsDistinct(t *testing.T) {
	got := spi.ActorDeleteForms("0", "00", " 0 ")
	adf137Must(t, "'0'/'00' 都是合法 id 且互不相同，' 0 ' 的原值形也要在",
		got, []string{"0", "00", " 0 "})
	for _, want := range []string{"0", "00", " 0 "} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("哨兵 %q 不得被丢掉，实得 %q", want, got)
		}
	}
}

// TestIssue137ActorDeleteFormsPreservesOrderAcrossActors 保序：多个人按入参顺序展开
// （原值形紧跟其 trim 形），便于各栈 IN 列表与日志逐字对照。
func TestIssue137ActorDeleteFormsPreservesOrderAcrossActors(t *testing.T) {
	adf137Must(t, "语义 6 义务②：保序展开",
		spi.ActorDeleteForms("a", " b ", "c"), []string{"a", " b ", "b", "c"})
	adf137Must(t, "空值夹在中间不打乱其余元素的顺序",
		spi.ActorDeleteForms(" b ", "", "a", "   ", " b "), []string{" b ", "b", "a"})
}

// TestIssue137ActorDeleteFormsNeverManufacturesNullLiteral 空值不得被串化成 "null"/"<nil>"/"None"
// 再去匹配——那会删掉一个真名叫 "null" 的人（spec §2.11 第 1 行点名的五栈病形）。
// Go 侧入参是 []string（nil 元素到不了本函数），本格钉的是"本单点绝不无中生有造出这类字面串"。
func TestIssue137ActorDeleteFormsNeverManufacturesNullLiteral(t *testing.T) {
	for _, forms := range [][]string{
		spi.ActorDeleteForms("", "   ", "\t"),
		spi.ActorDeleteForms(nil...),
	} {
		for _, f := range forms {
			if f == "null" || f == "<nil>" || f == "None" || strings.TrimSpace(f) == "" {
				t.Fatalf("空值入参不得产出任何字面串或空值，实得 %q", forms)
			}
		}
		if len(forms) != 0 {
			t.Fatalf("全空入参应为空切片，实得 %q", forms)
		}
	}
	// "null"/"<nil>" 作为**真人名**时是合法归属值，照出两形（此处已 trim ⇒ 各一份）。
	adf137Must(t, "字面串 null/<nil> 是合法归属值",
		spi.ActorDeleteForms("null", "<nil>"), []string{"null", "<nil>"})
}

// TestIssue137ActorDeleteFormsReusesNormalizeActorsJudge 判据本体只有一枚：trim 与判空
// 与 [spi.NormalizeActors] 同答案（本函数只多"原值形"这一层）。两份判据迟早分叉
// （spec §2.11 尾注点名 php 那轮实测到的"归一函数严格比较、仓储写侧松散 in_array"）。
func TestIssue137ActorDeleteFormsReusesNormalizeActorsJudge(t *testing.T) {
	cases := [][]string{
		{" 9101 "},
		{"9101", " 9101 ", ""},
		{"0", "00", " 0 ", "  "},
		{"a b", " a b "},
		{"", "   ", "\t", "\r\n"},
		nil,
	}
	for _, raw := range cases {
		normalized := spi.NormalizeActors(raw...)
		forms := spi.ActorDeleteForms(raw...)
		// ① 归一后的每一个 trim 形都必须在并集里（并集包含 trim 形 ⇒ 142 §9.2 既有判据不破）
		for _, n := range normalized {
			found := false
			for _, f := range forms {
				if f == n {
					found = true
				}
			}
			if !found {
				t.Fatalf("入参 %q：trim 形 %q 不在并集 %q 里", raw, n, forms)
			}
		}
		// ② 并集里不得出现任何"trim 后为空"的值（义务①）
		for _, f := range forms {
			if strings.TrimSpace(f) == "" {
				t.Fatalf("入参 %q：并集含空值 %q", raw, f)
			}
		}
		// ③ 并集里每个值要么是入参原值、要么是入参的 trim 形（不许无中生有）
		for _, f := range forms {
			ok := false
			for _, r := range raw {
				if f == r || f == strings.TrimSpace(r) {
					ok = true
				}
			}
			if !ok {
				t.Fatalf("入参 %q：并集里的 %q 既非原值也非 trim 形", raw, f)
			}
		}
	}
}
