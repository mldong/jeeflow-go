// 空抄送人不建 cc 行（issues/141 G10 · Go 栈，SQL 仓一路 ＋ 手动腿 SQL 侧）。
//
// spec 06-facade.md §2.10（owner 2026-09-29 拍「空不创建行」）：三条入口解析抄送人集合时
// 空串/纯空白/数组里的空元素一律丢弃，丢完为空 ⇒ 不建任何 cc 行、也不 fire 码 4；判据必须
// **两层都挡**——漏斗层之外，JDBC 仓的 `CreateCcInstance` 自己也要丢（绕过引擎/门面直连
// 仓储的调用方同样建不出 actor_id 为空串的行，空归属值是 issues/129 那族"空 operator 读全库"
// 的病根）。
//
// 本文件是 memory/cc_blank_actor_141_test.go 的 SQL 镜像：**同一份数据两仓必须同答案**
// （issues/117 场景 27）；两仓的判据都收在同一个 [spi.NormalizeCcActors] 上，不各抄一份。
// trim 一档在本侧尤其要紧——actor_id 是 VARCHAR，MySQL 与 sqlite 都会保留两端空格，
// " 8901 " 与 "8901" 不 trim 就是两行，把 G2 的写侧判重打穿。
//
// 跑法（sqlite :memory:，不连 160 真库）：
//
//	go test ./repository/jdbc/ -run 'TestIssue141G10'
package jdbc_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// ccActorsRaw141G10 直查某实例全部 cc 行的 actor_id（按建行顺序）。取证走库而不是返回值，
// 与同目录 G1/G2 的规矩一致（"只看返回值不作数"）。
func ccActorsRaw141G10(t *testing.T, db *sql.DB, instanceID int64) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT actor_id FROM wf_process_cc_instance WHERE process_instance_id = ? ORDER BY id`, instanceID)
	if err != nil {
		t.Fatalf("取证 cc 行: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var actorID string
		if err := rows.Scan(&actorID); err != nil {
			t.Fatalf("scan cc 行: %v", err)
		}
		out = append(out, actorID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate cc 行: %v", err)
	}
	return out
}

// manualRaw141G10 手动腿原始返回（全空白档要看它是不是与"空 actorIds"同档，不能假定成功）。
func manualRaw141G10(t *testing.T, f *facade.Facade, instanceID int64, actorIDs interface{}) map[string]interface{} {
	t.Helper()
	return f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "zhangsan", "actorIds": actorIDs,
	})
}

func mustManualOk141G10(t *testing.T, f *facade.Facade, instanceID int64, actorIDs interface{}) {
	t.Helper()
	resp := manualRaw141G10(t, f, instanceID, actorIDs)
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("手动抄送应成功, 实得 %v", resp)
	}
}

func mustEq141G10(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s：实得 %q（%d 个）want %q", label, got, len(got), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s：实得 %q want %q", label, got, want)
		}
	}
}

// ═══ 手动腿（门面 → SQL 仓）───

// TestIssue141G10JdbcManualLegAllBlankCreatesNoRow 手动腿给全空白 ⇒ 库里一行都不许有、
// 码 4 一支都不发，并且与"空 actorIds"档同判（沿用既有文案，不新造错误语义）。
//
// 改前红：[]string{"", "   "} 经门面直落 SQL 仓 ⇒ 两行 actor_id 为空串／纯空白 ＋ 两支码 4、code=0。
func TestIssue141G10JdbcManualLegAllBlankCreatesNoRow(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	instanceID := newInstance141(t, db, 91419101, "CC141-G10-ALL-BLANK")

	resp := manualRaw141G10(t, f, instanceID, []string{"", "   "})

	if code, _ := resp["code"].(int); code == 0 {
		t.Fatalf("G10：全空白与空集合同档（spec 06 §2.10），不得报成功，实得 %v", resp)
	}
	if msg, _ := resp["msg"].(string); !strings.Contains(msg, "actorIds 缺失") {
		t.Fatalf("G10：全空白须沿用既有\"空 actorIds\"文案，实得 msg=%q", msg)
	}
	if n := ccRowCount141(t, db, instanceID); n != 0 {
		t.Fatalf("G10：cc 表必须零行，实得 %d", n)
	}
	mustEq141G10(t, "G10：actor 集合必须为空", ccActorsRaw141G10(t, db, instanceID), nil)
	if len(*events) != 0 {
		t.Fatalf("G10：全空白不得 fire 码 4，实得 %v", ccEvtActors(*events))
	}
}

// TestIssue141G10JdbcBlankElementsDroppedValidOnesRemain 混着给：只丢空元素，
// 有效的人照旧落行＋fire（逗号串与数组两形同判据的 SQL 侧）。
func TestIssue141G10JdbcBlankElementsDroppedValidOnesRemain(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	instanceID := newInstance141(t, db, 91419102, "CC141-G10-MIXED")

	mustManualOk141G10(t, f, instanceID, []string{"8701", "", "  ", "8702"})

	mustEq141G10(t, "G10：空元素丢弃、有效元素保留", ccActorsRaw141G10(t, db, instanceID), []string{"8701", "8702"})
	if n := ccRowCount141(t, db, instanceID); n != 2 {
		t.Fatalf("G10：库里只有两行，实得 %d", n)
	}
	if got := ccEvtActors(*events); len(got) != 2 || got[0] != "8701" || got[1] != "8702" {
		t.Fatalf("G10：fire 的入参只含有效的人，实得 %v", got)
	}
}

// ═══ 写侧兜底：绕过引擎/门面直连仓储也建不出空行 ═══

// TestIssue141G10JdbcRepoWriteDropsBlankActors 空串／纯空白／未 trim 值都按归一后的形状处理。
// 只修漏斗（HandleCcActors）不修写侧，第三方仓储直投就还能灌进空值——本条钉第二层。
//
// 改前红：`for _, actorID := range actorIDs` 无判据 ⇒ "" 与 "   " 各 INSERT 一行。
func TestIssue141G10JdbcRepoWriteDropsBlankActors(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91419103, "CC141-G10-REPO")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "", "   ", "8801"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	mustEq141G10(t, "G10：SQL 仓写侧空串/纯空白都不建行", ccActorsRaw141G10(t, db, instanceID), []string{"8801"})
	if n := ccRowCount141(t, db, instanceID); n != 1 {
		t.Fatalf("G10：只落那一行，实得 %d", n)
	}
}

// TestIssue141G10JdbcCcActorValueTrimmedAndHitsDedupRule 落库值取 trim 后的串：
// " 8901 " 与 "8901" 是同一个人（与 G2 判重咬合）。
//
// 改前红：先落 " 8901 "（未 trim）⇒ 第二次 "8901" 判不出重复，库里两行、还多 fire 一支码 4。
func TestIssue141G10JdbcCcActorValueTrimmedAndHitsDedupRule(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	f, _, events := newCc141Facade(t, db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91419104, "CC141-G10-TRIM")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", " 8901 "); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	mustEq141G10(t, "G10：入库值应是 trim 后的串", ccActorsRaw141G10(t, db, instanceID), []string{"8901"})

	*events = nil
	mustManualOk141G10(t, f, instanceID, "8901")

	if n := ccRowCount141(t, db, instanceID); n != 1 {
		t.Fatalf("G10：带空格与不带空格判为同一人 ⇒ 不新增行，实得 %d 行", n)
	}
	if len(*events) != 0 {
		t.Fatalf("G10：判重命中 ⇒ 不 fire 码 4，实得 %v", ccEvtActors(*events))
	}
}

// TestIssue141G10JdbcIfAbsentSubsetExcludesBlanks CreateCcInstanceIfAbsent 返回的子集也不得
// 含空值/未 trim 值——子集直接拿去逐人 fire 码 4。
func TestIssue141G10JdbcIfAbsentSubsetExcludesBlanks(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91419105, "CC141-G10-SUBSET")

	created, err := repo.CreateCcInstanceIfAbsent(ctx, instanceID, "zhangsan", "", "8951", "  ", " 8952 ")
	if err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	mustEq141G10(t, "G10：实际新建子集只含有效且 trim 后的人", created, []string{"8951", "8952"})
	mustEq141G10(t, "G10：子集与库里真行一致", ccActorsRaw141G10(t, db, instanceID), []string{"8951", "8952"})
}

// TestIssue141G10JdbcNormalActorIdsAreNotMistakenForBlank 反向哨兵：判据只吃空值，
// 不吃 "0" 这类"看起来像空"的正常 id。这一格按设计**改前也不红**（哨兵）。
func TestIssue141G10JdbcNormalActorIdsAreNotMistakenForBlank(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	instanceID := newInstance141(t, db, 91419106, "CC141-G10-SENTINEL")

	mustManualOk141G10(t, f, instanceID, []string{"0", "user-1"})

	mustEq141G10(t, "G10 只丢空串/纯空白：\"0\" 不得被吃掉",
		ccActorsRaw141G10(t, db, instanceID), []string{"0", "user-1"})
	if len(*events) != 2 {
		t.Fatalf("反向哨兵：照旧逐人 fire，实得 %d 支", len(*events))
	}
}
