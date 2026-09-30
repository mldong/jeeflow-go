// 归属值写侧归一判据的**单点**测试（issues/142 B 批 · spec 06-facade.md §2.11，
// 判据本体逐字承 issues/141 G10 · §2.10，owner 2026-09-30 拍「八栈一起收：
// 两形同判据＋写侧兜底＋trim＋哨兵」）。
//
// NormalizeActors 是这条判据在本仓的唯一落点（cc 侧旧名 NormalizeCcActors 只是它的转发），
// 所以除了各消费方的格子，判据本体也直接钉一遍：丢什么、留什么、顺序与折叠，
// 以及"旧名＝同一枚"（两份判据迟早分叉，spec §2.11 结尾点名 php 那轮实测到的
// "归一函数内部严格比较、仓储写侧却用松散 in_array ⇒ '0' == '00' 静默丢掉第二个人"）。
package spi_test

import (
	"reflect"
	"testing"

	"github.com/mldong/jeeflow-go/spi"
)

// TestIssue142NormalizeActorsDropsBlanksTrimsAndFolds 判据本体三件事：trim、丢空、折叠。
//
// 改前不会红（G10 已把这枚尺子做对了），本格的作用是钉"任务侧复用的就是这一枚"——
// 消费方（门面 toStringSlice2／引擎 valueToActors／两仓 AddTaskActor）的格子红不红，
// 都以这里的判据为基准。
func TestIssue142NormalizeActorsDropsBlanksTrimsAndFolds(t *testing.T) {
	cases := []struct {
		name string
		raw  []string
		want []string
	}{
		{"nil 入参", nil, nil},
		{"空串", []string{""}, nil},
		{"纯空白（空格/制表/换行）", []string{"   ", "\t", "\n", " \r\n "}, nil},
		{"混给只丢空", []string{"7601", "", "  ", "7602"}, []string{"7601", "7602"}},
		{"两端空格剥掉", []string{" 123 "}, []string{"123"}},
		{"同人两种写法折叠成一行", []string{" 123 ", "123"}, []string{"123"}},
		{"同次调用重复折叠且顺序保持", []string{"b", "a", "b", " a "}, []string{"b", "a"}},
		{"中间空白是值的一部分（只 trim 两端）", []string{"a b"}, []string{"a b"}},
	}
	for _, c := range cases {
		if got := spi.NormalizeActors(c.raw...); !reflect.DeepEqual(got, c.want) && !(len(got) == 0 && len(c.want) == 0) {
			t.Fatalf("%s：实得 %q want %q", c.name, got, c.want)
		}
	}
}

// TestIssue142NormalizeActorsSentinelsArePeople 反向哨兵（spec §2.10④／§2.11④）：
// 判据只吃空值，"0" 这类"看起来像空"的正常 id **不得**被丢掉；
// "0" 与 "00" 是**两个不同的人**（比较用字符串等值，严禁借语言自带的假值判据/松散比较）。
func TestIssue142NormalizeActorsSentinelsArePeople(t *testing.T) {
	got := spi.NormalizeActors("0", "00", "null", "false", "a", " ")
	want := []string{"0", "00", "null", "false", "a"} // 只有纯空白那一个被丢
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("哨兵：\"0\"/\"00\"/\"null\"/\"false\"/\"a\" 都是正常归属值，实得 %q want %q", got, want)
	}
	// "0" 与 "00" 各自单独给都保留 ⇒ 判据没把数字 0 当假值
	for _, one := range []string{"0", "00"} {
		if got := spi.NormalizeActors(one); len(got) != 1 || got[0] != one {
			t.Fatalf("哨兵：%q 不得被吃掉，实得 %q", one, got)
		}
	}
}

// TestIssue142NormalizeCcActorsIsThinForwarder 旧名必须与新入口**同判据**（薄封装转发，
// 不是第二份实现）。改前不会红（G10 那份判据与本体逐字同形），钉的是"以后别把其中一支改掉"。
func TestIssue142NormalizeCcActorsIsThinForwarder(t *testing.T) {
	probe := [][]string{
		nil,
		{""},
		{"  ", "0", "00"},
		{" 123 ", "123", "", "a"},
		{"b", "a", "b"},
	}
	for _, raw := range probe {
		if got, old := spi.NormalizeActors(raw...), spi.NormalizeCcActors(raw...); !reflect.DeepEqual(got, old) {
			t.Fatalf("旧名 NormalizeCcActors(%q)=%q 与 NormalizeActors(%q)=%q 分叉 ⇒ 出现第二份判据",
				raw, old, raw, got)
		}
	}
}
