// 抄送两格的 SQL 仓一路（issues/141 G1 归属条件必填 ＋ G2 写侧判重＝幂等空操作 · Go 栈，T0）。
//
// 与 memory/cc_page_ownership_141_test.go（内存仓 G1）、facade/cc_write_dedup_141_test.go
// （内存仓＋引擎/门面三条腿 G2）跑同一条判据：**同一份数据，SQL 仓与内存仓必须给同一个答案**
// （spec 06 §2.5，issues/117 场景 27 那把尺子扩到 ccList）。
//
// G1 的旧形状在这一侧是"空集合 IN 条件被整个丢掉不加"（LEFT JOIN wf_process_cc_instance
// 的条件一旦被吞，取数范围就退化成全部实例）；非归属列的空值放行不变。
// G2 的四档逐字照 spec 06 §4：同一 (实例, 人) 已有 cc 行时**跳过**——①不新增行 ②不重置未读
// 状态（state）③不更新原行时间（create_time/update_time 与原行 id 逐字不变）④不 fire
// CC_CREATE（码 4）。查询侧不引入 DISTINCT、历史重复行不清理（owner 拍为接受既成事实），
// 所以这里只钉写侧。断言直接查 wf_process_cc_instance 的真实行——只看返回值不作数。
//
// 跑法（sqlite :memory:，不连 160 真库）：
//
//	go test ./repository/jdbc/ -run 'TestIssue141JdbcCc'
package jdbc_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 夹具与取证辅助 ────────────────────────────────────────────────────────────

// cc141DefineID 夹具流程定义 id（私有段，不与 T1 真库用例的 900001 相撞）
const cc141DefineID = int64(91415001)

// seedCc141Define 放一条流程定义（PageCcInstances 的 LEFT JOIN pd 才有名字可带出）。
func seedCc141Define(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO wf_process_define (id,name,display_name,type,state,version) VALUES (?,?,?,?,1,0)`,
		cc141DefineID, "cc141", "抄送141测试流程", "approval")
	if err != nil {
		t.Fatalf("seed define: %v", err)
	}
}

// newInstance141 直插一条实例行（cc 的归属对象），返回实例 id。
func newInstance141(t *testing.T, db *sql.DB, id int64, businessNo string) int64 {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO wf_process_instance (id,process_define_id,state,business_no,operator,variable)
		 VALUES (?,?,10,?,?,'{}')`, id, cc141DefineID, businessNo, "zhangsan")
	if err != nil {
		t.Fatalf("seed instance %d: %v", id, err)
	}
	return id
}

// ccEvidence 一行 cc 的取证形状（时间取驱动返回值的字符串形态，两仓对比只看"变没变"）。
type ccEvidence struct {
	ID         int64
	State      int
	CreateTime string
	UpdateTime string
}

// ccRows141 查某 (实例, 人) 的真实 cc 行（①②③三档都断在这里）。
func ccRows141(t *testing.T, db *sql.DB, instanceID int64, actorID string) []ccEvidence {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, state, create_time, update_time FROM wf_process_cc_instance
		 WHERE process_instance_id = ? AND actor_id = ? ORDER BY id`, instanceID, actorID)
	if err != nil {
		t.Fatalf("取证 cc 行: %v", err)
	}
	defer rows.Close()
	var out []ccEvidence
	for rows.Next() {
		var (
			id         int64
			state      int
			createTime interface{}
			updateTime interface{}
		)
		if err := rows.Scan(&id, &state, &createTime, &updateTime); err != nil {
			t.Fatalf("scan cc 行: %v", err)
		}
		out = append(out, ccEvidence{
			ID: id, State: state,
			CreateTime: fmt.Sprint(createTime), UpdateTime: fmt.Sprint(updateTime),
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf(" iterate cc 行: %v", err)
	}
	return out
}

// ccRowCount141 某实例的全部 cc 行数（不带 actor 条件）。
func ccRowCount141(t *testing.T, db *sql.DB, instanceID int64) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?`, instanceID).Scan(&n); err != nil {
		t.Fatalf("count cc 行: %v", err)
	}
	return n
}

// stampCcTimes141 把某 (实例, 人) 的 cc 行时间刷成两个**哨值**——③档的取证不依赖时钟精度
// （MySQL TIMESTAMP 只到秒，纯靠 sleep 会让"没刷新"变成测不出来的空断言）：判重的错实现
// 只要碰了原行时间或改走"删旧插新"，哨值就保不住。
func stampCcTimes141(t *testing.T, db *sql.DB, instanceID int64, actorID string) (string, string) {
	t.Helper()
	create := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	update := time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC)
	_, err := db.ExecContext(context.Background(),
		`UPDATE wf_process_cc_instance SET create_time=?, update_time=? WHERE process_instance_id=? AND actor_id=?`,
		create, update, instanceID, actorID)
	if err != nil {
		t.Fatalf("刷哨值时间: %v", err)
	}
	got := ccRows141(t, db, instanceID, actorID)
	if len(got) != 1 {
		t.Fatalf("夹具失效：哨值刷新后应恰好 1 行，实得 %d", len(got))
	}
	return got[0].CreateTime, got[0].UpdateTime
}

func qc141(conds ...spi.Condition) spi.PageQuery {
	out := spi.PageQuery{PageNum: 1, PageSize: 100}
	if len(conds) > 0 {
		out.Conditions = conds
	}
	return out
}

// newCc141Facade 把 SQL 仓接到引擎＋门面上（④/子集两档断的是"码 4 到底发没发、发给谁"）。
// 返回的 slice 只收 CC_CREATE，"重复抄送没有新事件"就断在这里。
func newCc141Facade(t *testing.T, db *sql.DB) (*facade.Facade, *jdbc.Repository, *[]engine.ProcessEvent) {
	t.Helper()
	repo := jdbc.New(db)
	idGen := &tsIDGen{base: time.Now().UnixMilli()*4000 + testStackSeq*1000}
	eng := engine.New(repo, &noopUserProvider{}, idGen, nil)
	events := &[]engine.ProcessEvent{}
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{func(evt engine.ProcessEvent) {
			if evt.Type == engine.EventCCCreate {
				*events = append(*events, evt)
			}
		}},
	})
	return facade.New(eng, repo, nil), repo, events
}

// manualCc141 门面手动腿（与引擎 f_/tf_ 两腿共用同一条判重判据，spec §11.7）。
func manualCc141(t *testing.T, f *facade.Facade, instanceID int64, actorIDs ...string) {
	t.Helper()
	resp := f.Flow("processInstance/createCCInstance", map[string]interface{}{
		"processInstanceId": instanceID, "operator": "zhangsan", "actorIds": actorIDs,
	})
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("手动抄送应成功, 实得 %v", resp)
	}
}

func ccEvtActors(events []engine.ProcessEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.CcActorID)
	}
	return out
}

func eqStrings(a, b []string) bool {
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

// ═══ G1：归属条件必填，缺条件/空值 ⇒ 空页（SQL 仓） ═══

// TestIssue141JdbcCcPageWithOwnershipReturnsOnlyMine 正向对照：带有效归属时只出"我的"。
func TestIssue141JdbcCcPageWithOwnershipReturnsOnlyMine(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	mine := newInstance141(t, db, 91415101, "CC141-MINE")
	theirs := newInstance141(t, db, 91415102, "CC141-THEIRS")
	if err := repo.CreateCcInstance(ctx, mine, "zhangsan", "user1"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if err := repo.CreateCcInstance(ctx, theirs, "zhangsan", "user2"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	rows, total, err := repo.PageCcInstances(ctx, qc141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user1"}), "user1")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("带条件应命中我的那 1 条, got total=%d rows=%d", total, len(rows))
	}
	if rows[0].ID != mine || rows[0].ID == theirs {
		t.Fatalf("命中的应是我的实例 %d，实得 %d（别人的串进来了）", mine, rows[0].ID)
	}
}

// TestIssue141JdbcCcPageWithoutOwnershipIsEmptyPage 缺陷档：归属整条没给 ⇒ **空页**。
// 库里真有两个实例两行 cc（夹具守卫），这一格同时钉住"不是 0==0 自等"。
func TestIssue141JdbcCcPageWithoutOwnershipIsEmptyPage(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	mine := newInstance141(t, db, 91415111, "CC141-MINE")
	newInstance141(t, db, 91415112, "CC141-THEIRS")
	if err := repo.CreateCcInstance(ctx, mine, "zhangsan", "user1"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	if rows, total, _ := repo.PageCcInstances(ctx, qc141(), "user1"); len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效：PageCcInstances(user1) 应有 1 行, got rows=%d total=%d", len(rows), total)
	}

	for _, blank := range []struct{ name, val string }{{"空串", ""}, {"全空白", "   "}, {"制表符", "\t"}} {
		rows, total, err := repo.PageCcInstances(ctx, qc141(), blank.val)
		if err != nil {
			t.Fatalf("PageCcInstances(%s) 不应报错: %v", blank.name, err)
		}
		if total != 0 || len(rows) != 0 {
			t.Fatalf("缺归属条件必须返回空页，实得 %s ⇒ total=%d rows=%d（退化成「这条不加」返回全部实例）",
				blank.name, total, len(rows))
		}
		if rows == nil {
			t.Fatalf("空页的 rows 必须是空集而不是 nil（契约 rows=[]，与内存仓同形状）")
		}
	}
}

// TestIssue141JdbcCcPageBlankOwnershipConditionIsEmptyPage 空值三形＋空 IN ⇒ 空页。
// **本档的空集合支是 SQL 仓改前的红**：buildWhere 对 `IN ?` 的空列表整个不加条件，
// 取数范围退化成"这个人的一页 + 无该条件"，而内存仓（同样按空值处理）给空页 ⇒ 两仓两个答案。
func TestIssue141JdbcCcPageBlankOwnershipConditionIsEmptyPage(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	mine := newInstance141(t, db, 91415121, "CC141-MINE")
	if err := repo.CreateCcInstance(ctx, mine, "zhangsan", "user1"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if rows, total, _ := repo.PageCcInstances(ctx, qc141(), "user1"); len(rows) != 1 || total != 1 {
		t.Fatalf("前置失效：PageCcInstances(user1) 应有 1 行, got rows=%d total=%d", len(rows), total)
	}

	for _, v := range []interface{}{"", "   ", nil} {
		rows, total, err := repo.PageCcInstances(ctx, qc141(spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: v}), "user1")
		if err != nil {
			t.Fatalf("cc.actor_id EQ %v 查询失败: %v", v, err)
		}
		if len(rows) != 0 || total != 0 {
			t.Fatalf("空串/全空白/nil 归属条件 ⇒ 空页，实得 EQ %v ⇒ rows=%d total=%d", v, len(rows), total)
		}
	}
	for _, op := range []string{"EQ", "IN"} {
		for _, empty := range []interface{}{[]interface{}{}, []string{}} {
			rows, total, err := repo.PageCcInstances(ctx, qc141(spi.Condition{Column: "cc.actor_id", Operator: op, Value: empty}), "user1")
			if err != nil {
				t.Fatalf("cc.actor_id %s 空集合查询失败: %v", op, err)
			}
			if len(rows) != 0 || total != 0 {
				t.Fatalf("空集合归属条件（%s %T）⇒ 空页，实得 rows=%d total=%d ⇒ 条件被整个丢掉不加",
					op, empty, len(rows), total)
			}
		}
	}
}

// TestIssue141JdbcCcPageBlankNonOwnershipConditionStillIgnored 改动面哨兵：只收归属谓词，
// 非归属列的空值放行不变（m_LIKE_* 传空串仍按"没填"忽略 ⇒ 可选过滤照旧生效）。
func TestIssue141JdbcCcPageBlankNonOwnershipConditionStillIgnored(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	mine := newInstance141(t, db, 91415131, "CC141-MINE")
	if err := repo.CreateCcInstance(ctx, mine, "zhangsan", "user1"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}

	rows, total, err := repo.PageCcInstances(ctx, qc141(
		spi.Condition{Column: "cc.actor_id", Operator: "EQ", Value: "user1"},
		spi.Condition{Column: "t.business_no", Operator: "LIKE", Value: ""},
	), "user1")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 || total != 1 {
		t.Fatalf("空值非归属条件应被忽略、归属条件照常生效 ⇒ 1 行, got rows=%d total=%d ⇒ 通用放行被误改了",
			len(rows), total)
	}
	if rows[0].ID != mine {
		t.Fatalf("命中实例应为 %d, 实得 %d", mine, rows[0].ID)
	}
}

// ═══ G2：写侧判重＝幂等空操作（SQL 仓） ═══

// TestIssue141JdbcFirstCcStillCreatesRowAndFiresPerActor 正向对照：全新的一次抄送
// 照旧逐人建行＋逐人 fire 码 4，新行是未读（state=0）。判重不许把这个吃掉。
func TestIssue141JdbcFirstCcStillCreatesRowAndFiresPerActor(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, repo, events := newCc141Facade(t, db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91415141, "CC141-FIRE")

	manualCc141(t, f, instanceID, "8101", "8102")

	if n := ccRowCount141(t, db, instanceID); n != 2 {
		t.Fatalf("全新抄送应逐人建行，实得 %d 行", n)
	}
	if got := ccEvtActors(*events); !eqStrings(got, []string{"8101", "8102"}) {
		t.Fatalf("全新抄送应逐人 fire 码 4 且顺序与入参一致，实得 %v", got)
	}
	for _, e := range *events {
		if e.InstanceID != instanceID {
			t.Fatalf("码 4 的 sourceId 应为实例 %d，实得 %d", instanceID, e.InstanceID)
		}
	}
	if row := ccRows141(t, db, instanceID, "8101"); len(row) != 1 || row[0].State != 0 {
		t.Fatalf("新行应是未读（state=0），实得 %+v", row)
	}
	// 仓储读侧同步反映真实行集
	ids, err := repo.FindCcActorIDs(ctx, instanceID)
	if err != nil || !eqStrings(ids, []string{"8101", "8102"}) {
		t.Fatalf("FindCcActorIDs 应读回真实行集，实得 %v err=%v", ids, err)
	}
}

// TestIssue141JdbcRepeatCcAddsNoRowAndFiresNothing ①不新增行（原行 id 也不变，没有删旧插新）
// ＋ ④不 fire 码 4：门面手动腿连发两次同一个人。
func TestIssue141JdbcRepeatCcAddsNoRowAndFiresNothing(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	instanceID := newInstance141(t, db, 91415151, "CC141-REPEAT")

	manualCc141(t, f, instanceID, "8201")
	if n := ccRowCount141(t, db, instanceID); n != 1 {
		t.Fatalf("首次抄送应落 1 行, 实得 %d", n)
	}
	if len(*events) != 1 {
		t.Fatalf("首次抄送应 fire 1 次, 实得 %d", len(*events))
	}
	rowID := ccRows141(t, db, instanceID, "8201")[0].ID

	*events = nil
	manualCc141(t, f, instanceID, "8201")

	got := ccRows141(t, db, instanceID, "8201")
	if len(got) != 1 {
		t.Fatalf("①重复抄送不得新增行，实得 %d 行（判重被摘掉即此档红）", len(got))
	}
	if got[0].ID != rowID {
		t.Fatalf("①原行 id 必须不变（没有删旧插新），实得 %d want %d", got[0].ID, rowID)
	}
	if len(*events) != 0 {
		t.Fatalf("④没发生创建就不得发码 4（spec 11.2 原则 1「码=事实」），实得 %v", ccEvtActors(*events))
	}
}

// TestIssue141JdbcRepeatCcDoesNotResetUnreadState ②不重置未读：SQL 置已读（state=1）后
// 重复抄送，state 必须仍是 1（owner 2026-09-29 明确"不需要重置"，不产生"再提醒一次"语义）。
func TestIssue141JdbcRepeatCcDoesNotResetUnreadState(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91415161, "CC141-READ")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "8301"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	if err := repo.UpdateCcStatus(ctx, instanceID, "8301"); err != nil {
		t.Fatalf("置已读: %v", err)
	}
	before := ccRows141(t, db, instanceID, "8301")
	if len(before) != 1 || before[0].State != 1 {
		t.Fatalf("置读后应恰好 1 行且 state=1，实得 %+v", before)
	}

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "8301"); err != nil {
		t.Fatalf("重复建 cc: %v", err)
	}
	after := ccRows141(t, db, instanceID, "8301")
	if len(after) != 1 {
		t.Fatalf("①重复抄送不得新增行，实得 %d 行", len(after))
	}
	if after[0].State != 1 {
		t.Fatalf("②重复抄送不得把已读抹回未读，实得 state=%d", after[0].State)
	}
}

// TestIssue141JdbcRepeatCcDoesNotTouchOriginalRowTimes ③不更新原行时间：原行
// create_time/update_time 逐字不变（哨值取证，见 stampCcTimes141 的说明）。
func TestIssue141JdbcRepeatCcDoesNotTouchOriginalRowTimes(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	repo := jdbc.New(db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91415171, "CC141-TIME")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "8401"); err != nil {
		t.Fatalf("建 cc: %v", err)
	}
	createStamp, updateStamp := stampCcTimes141(t, db, instanceID, "8401")

	if err := repo.CreateCcInstance(ctx, instanceID, "zhangsan", "8401"); err != nil {
		t.Fatalf("重复建 cc: %v", err)
	}
	after := ccRows141(t, db, instanceID, "8401")
	if len(after) != 1 {
		t.Fatalf("①重复抄送不得新增行，实得 %d 行（判重被摘掉即此档红）", len(after))
	}
	if after[0].CreateTime != createStamp || after[0].UpdateTime != updateStamp {
		t.Fatalf("③重复抄送不得刷新原行时间，实得 create=%v update=%v want %v/%v",
			after[0].CreateTime, after[0].UpdateTime, createStamp, updateStamp)
	}
}

// TestIssue141JdbcRepeatCcFiresOnlyForNewlyCreatedSubset ④的子集档：第二次同时给
// 「已知人＋新人」⇒ 只为新人建行、只为新人 fire（逐人 fire 的入参是实际新建的子集）。
func TestIssue141JdbcRepeatCcFiresOnlyForNewlyCreatedSubset(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, repo, events := newCc141Facade(t, db)
	ctx := context.Background()
	instanceID := newInstance141(t, db, 91415181, "CC141-SUBSET")

	manualCc141(t, f, instanceID, "8501", "8502")
	if n := ccRowCount141(t, db, instanceID); n != 2 {
		t.Fatalf("首轮应 2 行, 实得 %d", n)
	}
	if len(*events) != 2 {
		t.Fatalf("首轮应 fire 2 次, 实得 %d", len(*events))
	}

	*events = nil
	manualCc141(t, f, instanceID, "8501", "8503")

	if n := ccRowCount141(t, db, instanceID); n != 3 {
		t.Fatalf("第二轮只该新增 8503 一行，实得总行数 %d", n)
	}
	if got := ccEvtActors(*events); !eqStrings(got, []string{"8503"}) {
		t.Fatalf("逐人 fire 的入参应是实际新建的子集，实得 %v want [8503]", got)
	}
	if len(*events) != 1 {
		t.Fatalf("子集只有 1 人 ⇒ 只 fire 1 次，实得 %d 次", len(*events))
	}
	if rows := ccRows141(t, db, instanceID, "8503"); len(rows) != 1 {
		t.Fatalf("新人 8503 的行应真在库里，实得 %d 行", len(rows))
	}
	ids, err := repo.FindCcActorIDs(ctx, instanceID)
	if err != nil || !eqStrings(ids, []string{"8501", "8502", "8503"}) {
		t.Fatalf("FindCcActorIDs 应读回 8501/8502/8503，实得 %v err=%v", ids, err)
	}
}

// TestIssue141JdbcDuplicateWithinOneCallCollapses 同一次调用内重复给同一个人 ⇒ 只落一行、
// 只 fire 一次（子集里也不该出现两次）。
func TestIssue141JdbcDuplicateWithinOneCallCollapses(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	instanceID := newInstance141(t, db, 91415191, "CC141-INCALL")

	manualCc141(t, f, instanceID, "8701", "8701")

	if n := ccRowCount141(t, db, instanceID); n != 1 {
		t.Fatalf("同一次调用内的重复不应新增第二行，实得 %d 行", n)
	}
	if got := ccEvtActors(*events); !eqStrings(got, []string{"8701"}) {
		t.Fatalf("同一次调用内的重复只 fire 一次，实得 %v", got)
	}
}

// TestIssue141JdbcCcDedupIsScopedToInstanceNotGlobal 反向哨兵：判重作用域是按实例，
// 不是全局——不同实例上的同一个人各自建行、各 fire。
func TestIssue141JdbcCcDedupIsScopedToInstanceNotGlobal(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	seedCc141Define(t, db)
	f, _, events := newCc141Facade(t, db)
	first := newInstance141(t, db, 91415201, "CC141-SCOPE-1")
	second := newInstance141(t, db, 91415202, "CC141-SCOPE-2")

	manualCc141(t, f, first, "8801")
	manualCc141(t, f, second, "8801")

	if n := ccRowCount141(t, db, first); n != 1 {
		t.Fatalf("实例一应有自己的 1 行 cc, 实得 %d", n)
	}
	if n := ccRowCount141(t, db, second); n != 1 {
		t.Fatalf("实例二不受实例一影响，同一个人照样建行，实得 %d", n)
	}
	if len(*events) != 2 {
		t.Fatalf("两个实例各 fire 一次，实得 %d 次（判重越界成全局即此档红）", len(*events))
	}
}
