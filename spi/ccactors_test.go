// 抄送人集合归一判据的单点测试（issues/141 G10 · spec 06-facade.md §2.10）。
//
// NormalizeCcActors 是这一条判据在本仓的**唯一落点**（引擎漏斗、门面手动腿、内存仓与 JDBC 仓
// 写侧四层共用），所以除了各消费方的格子，判据本体也直接钉一遍：丢什么、留什么、顺序与折叠。
package spi_test

import (
	"testing"

	"github.com/mldong/jeeflow-go/spi"
)

func eqG10(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIssue141G10NormalizeCcActorsDropsBlanksAndTrims(t *testing.T) {
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
		{"顺序保持", []string{"b", "a"}, []string{"b", "a"}},
		{"反向哨兵 0 不是空值", []string{"0"}, []string{"0"}},
		{"反向哨兵 null/false 字面串不是空值", []string{"null", "false", "0"}, []string{"null", "false", "0"}},
		{"中间空白是值的一部分（只 trim 两端）", []string{"a b"}, []string{"a b"}},
	}
	for _, c := range cases {
		got := spi.NormalizeCcActors(c.raw...)
		if !eqG10(got, c.want) {
			t.Fatalf("%s：实得 %q want %q", c.name, got, c.want)
		}
	}
}
