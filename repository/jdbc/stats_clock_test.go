// issues/125 的行为判据：逾期计数的 now 必须来自**引擎进程钟**，不是数据库会话钟。
//
// 为什么需要真库：这一格的自变量是 `expire_time < <某把钟>`，而 MySQL 的 `NOW()` 读的是
// `@@session.time_zone` 的墙钟 —— 只把 SQL 里的 `NOW()` 换成绑参，静态普查能看出来，
// "换个仓储实现数字就变 / 换一台 RDS 就错 8 小时"这个可观测面必须真机才咬得住。
//
// 用法：go test ./repository/jdbc/ -run TestStatsOverdue -v
//
//	JEFFLOW_DB_DRIVER / JEFFLOW_DB_DSN 覆盖连接（默认开发服务器 MySQL 的 jeeflow 库）
//
// 判据形状（四条，缺一条就是死格）：
//  1. **夹具必须有牙**：同一批夹具行用 `expire_time < NOW()` 直查，在 `+08:00` 与 `+00:00`
//     两个会话时区下读数**必须不同**。不同 ⇒ 会话钟确实在动、样本确实落在两把钟的夹层里；
//     相同 ⇒ 本测试当场判失败（这就是立案时踩过的"恒绿死格"，绝不允许它伪装成通过）。
//  2. **修完必须与时区无关**：走仓储方法（绑参）在两个会话时区下读数**逐位相同**，
//     且等于按注入钟在 Go 侧算出来的期望值。
//  3. **有牙**：注入钟前移 2 小时 ⇒ 逾期数必须变（+1h 那条从"未逾期"转"已逾期"）。
//     只断"键在/数对"而钟挪不动，等于没测。
//  4. **回归**：pending 半边不受影响；`expire_time IS NULL` 的行钟拨到 +30 天仍不计入。
//
// 夹具只写自己的 id 段（930001–930099），进出都按 id 删，不碰库里别人的在办任务；
// 统计口径是全表的，所以断言一律用"库里其余行 + 我的夹具"的**差量**，天然抗并发插入。
package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	statsPiID  = int64(930000) // 夹具挂在专属实例 id 上，便于误留时定位
	statsIDMin = int64(930001)
	statsIDMax = int64(930099)

	tzEast8 = "+08:00" // 与 160 现状同基准
	tzUTC   = "+00:00" // 差 8 小时的对照会话
)

func statsDriver() string {
	if d := os.Getenv("JEFFLOW_DB_DRIVER"); d != "" {
		return d
	}
	return "mysql"
}

func statsDSN() string {
	if d := os.Getenv("JEFFLOW_DB_DSN"); d != "" {
		return d
	}
	return "root:8Eli#gr#AUk@tcp(192.168.1.160:3306)/jeeflow?parseTime=true&charset=utf8mb4"
}

// statsPh 与仓储内部同一套占位符约定（pgx 走 $n）
func statsPh(q string) string {
	style := "?"
	if statsDriver() == "pgx" {
		style = "$n"
	}
	return ConvertPlaceholder(q, style)
}

// statsOpen 开一个**单连接**的库句柄：会话级 `SET time_zone` 只对当前物理连接生效，
// 连接池会把两个时区的读数混在不同连接上（假绿来源之一）。
// 每个时区各用一个句柄，且下面的 statsProbeTZ 会用回读自证"SET 真的作用于被测路径"。
func statsOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(statsDriver(), statsDSN())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db(%s jeeflow): %v —— 本案判据要真库，不许静默跳过", statsDriver(), err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// statsApplyTZ 在该句柄的唯一连接上设会话时区，然后**经连接池回读**自证生效
// （回读走的就是仓储用的那条路径；若驱动在连接归还时重置了会话变量，这里会当场红，
// 而不是让"两个时区读数相同"被误读成"修好了"）。返回 NOW() 与 UTC 的秒差。
func statsApplyTZ(t *testing.T, db *sql.DB, tz string) int {
	t.Helper()
	if tz != tzEast8 && tz != tzUTC {
		t.Fatalf("非法时区值 %q", tz)
	}
	ctx := context.Background()
	if statsDriver() == "mysql" {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire conn: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "SET time_zone = '"+tz+"'"); err != nil {
			conn.Close()
			t.Fatalf("SET time_zone=%s: %v", tz, err)
		}
		conn.Close()
	}
	var cur string
	var offset int
	if err := db.QueryRowContext(ctx, statsPh(
		"SELECT @@session.time_zone, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW())")).Scan(&cur, &offset); err != nil {
		t.Fatalf("回读会话时区: %v", err)
	}
	if statsDriver() == "mysql" && cur != tz {
		t.Fatalf("SET time_zone 后经连接池回读得到 %q（期望 %q）⇒ 会话变量没落到被测路径上，本案判据在此环境不成立", cur, tz)
	}
	return offset
}

// statsFixture 四行夹具：①无到期时间 ②早于 BASE 1 小时 ③晚于 BASE 1 小时 ④晚于 BASE 8 小时
// 全部 task_state=10（在办）。返回 BASE（注入钟的值）。
func statsFixture(t *testing.T, db *sql.DB) time.Time {
	t.Helper()
	ctx := context.Background()
	base := time.Now().Truncate(time.Millisecond)
	rows := []struct {
		id     int64
		expire interface{}
	}{
		{statsIDMin, nil},
		{statsIDMin + 1, base.Add(-time.Hour)},
		{statsIDMin + 2, base.Add(time.Hour)},
		{statsIDMin + 3, base.Add(8 * time.Hour)},
	}
	const ins = "INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name, task_state, expire_time, create_time) VALUES (?,?,?,?,?,?,?)"
	for i, r := range rows {
		if _, err := db.ExecContext(ctx, statsPh(ins), r.id, statsPiID,
			fmt.Sprintf("t125_%d", i), "125 判据夹具", 10, r.expire, base); err != nil {
			t.Fatalf("插入夹具行 id=%d: %v", r.id, err)
		}
	}
	return base
}

// statsPurge 按 id 段清场（前后各一次，幂等）
func statsPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	res, err := db.ExecContext(context.Background(), statsPh(
		"DELETE FROM wf_process_task WHERE id BETWEEN ? AND ?"), statsIDMin, statsIDMax)
	if err != nil {
		t.Fatalf("清场: %v", err)
	}
	if n, _ := res.RowsAffected(); n > 4 {
		t.Fatalf("清场删除了 %d 行（夹具只有 4 行）⇒ id 段撞了别人的数据，停手", n)
	}
}

// statsOthers 库里**除夹具外**在办任务的 (pending, overdue)，overdue 按传入钟算（与仓储同口径）
func statsOthers(t *testing.T, db *sql.DB, now time.Time) (int, int) {
	t.Helper()
	var pending, overdue int
	if err := db.QueryRowContext(context.Background(), statsPh(`SELECT
		COUNT(*) AS pending,
		COALESCE(SUM(CASE WHEN expire_time IS NOT NULL AND expire_time < ? THEN 1 ELSE 0 END), 0) AS overdue
	FROM wf_process_task WHERE task_state = 10 AND id NOT BETWEEN ? AND ?`),
		now, statsIDMin, statsIDMax).Scan(&pending, &overdue); err != nil {
		t.Fatalf("统计其余行: %v", err)
	}
	return pending, overdue
}

// statsRawNow 旧写法（SQL 里的 NOW()）在**当前会话时区**下对我那四行夹具的逾期计数 —— 判据 1 的探针
func statsRawNow(t *testing.T, db *sql.DB) int {
	t.Helper()
	var overdue int
	if err := db.QueryRowContext(context.Background(), statsPh(`SELECT
		COALESCE(SUM(CASE WHEN expire_time IS NOT NULL AND expire_time < NOW() THEN 1 ELSE 0 END), 0)
	FROM wf_process_task WHERE task_state = 10 AND id BETWEEN ? AND ?`),
		statsIDMin, statsIDMax).Scan(&overdue); err != nil {
		t.Fatalf("直查 NOW(): %v", err)
	}
	return overdue
}

func statsTimePtr(v time.Time) *time.Time { return &v }

// statsExpectedOverdue 在 Go 侧按注入钟算夹具的逾期数（期望值的独立算法，不复用被测 SQL）
func statsExpectedOverdue(base time.Time, shift time.Duration) int {
	n := 0
	for _, e := range []*time.Time{nil, statsTimePtr(base.Add(-time.Hour)),
		statsTimePtr(base.Add(time.Hour)), statsTimePtr(base.Add(8 * time.Hour))} {
		if e != nil && e.Before(base.Add(shift)) {
			n++
		}
	}
	return n
}

func TestStatsOverdueUsesEngineClockNotSessionTimeZone(t *testing.T) {
	setup := statsOpen(t)
	ctx := context.Background()

	statsPurge(t, setup)
	t.Cleanup(func() { statsPurge(t, setup) })
	base := statsFixture(t, setup)

	// 夹具落位自证：四行都在办
	var mine int
	if err := setup.QueryRowContext(ctx, statsPh(
		"SELECT COUNT(*) FROM wf_process_task WHERE task_state = 10 AND id BETWEEN ? AND ?"),
		statsIDMin, statsIDMax).Scan(&mine); err != nil {
		t.Fatalf("数夹具行: %v", err)
	}
	if mine != 4 {
		t.Fatalf("夹具应有 4 条在办行，实得 %d", mine)
	}

	// ── 判据 1+2：会话钟在动（夹具有牙），而仓储读数不跟着动 ──────────────────
	offsets := map[string]int{}
	rawByTZ := map[string]int{}
	fixedByTZ := map[string][2]int{}
	for _, tz := range []string{tzEast8, tzUTC} {
		db := statsOpen(t) // 每个时区独占一条连接
		offsets[tz] = statsApplyTZ(t, db, tz)
		rawByTZ[tz] = statsRawNow(t, db) // 旧写法：读数是会话钟的函数

		repo := New(db)
		repo.statsClock = func() time.Time { return base }
		p, o, err := repo.StatsPendingAndOverdueCount(ctx)
		if err != nil {
			t.Fatalf("StatsPendingAndOverdueCount(%s): %v", tz, err)
		}
		fixedByTZ[tz] = [2]int{p, o}
	}

	if statsDriver() == "mysql" {
		if offsets[tzEast8] == offsets[tzUTC] {
			t.Fatalf("SET time_zone 没改动 NOW() 与 UTC 的差（两边都 %d 秒）⇒ 会话钟探针失效，本测试无从判基准", offsets[tzEast8])
		}
		if rawByTZ[tzEast8] == rawByTZ[tzUTC] {
			t.Fatalf("夹具在两把钟下读数相同（都 %d）⇒ 样本没落进时区夹层，这是恒绿死格，判失败", rawByTZ[tzEast8])
		}
		t.Logf("旧写法（SQL NOW()）的读数：会话 %s 得 %d 条逾期，会话 %s 得 %d 条（两钟偏移差 %d 秒）⇒ 夹具确有牙",
			tzEast8, rawByTZ[tzEast8], tzUTC, rawByTZ[tzUTC], offsets[tzEast8]-offsets[tzUTC])
	} else {
		t.Logf("注：%s 驱动没有 MySQL 那套会话时区，判据 1（会话钟在动）这一腿在此驱动上不成立；"+
			"绑参后的等值/有牙/回归三条照常跑（本案病灶的实弹口径是 MySQL）", statsDriver())
	}

	// 修完：两个会话时区下读数逐位相同
	if fixedByTZ[tzEast8] != fixedByTZ[tzUTC] {
		t.Errorf("逾期读数被数据库会话时区带跑了：tz=%s 得 %v，tz=%s 得 %v",
			tzEast8, fixedByTZ[tzEast8], tzUTC, fixedByTZ[tzUTC])
	}

	// 且等于 Go 侧独立算出的期望（夹具部分 pending=4、overdue=1）
	othersP, othersO := statsOthers(t, setup, base)
	wantPending, wantOverdue := othersP+4, othersO+statsExpectedOverdue(base, 0)
	got := fixedByTZ[tzEast8]
	if got[0] != wantPending || got[1] != wantOverdue {
		t.Errorf("注入钟 BASE 下的期望 (pending=%d, overdue=%d)，实得 (pending=%d, overdue=%d)",
			wantPending, wantOverdue, got[0], got[1])
	}

	// ── 判据 3（有牙）：注入钟前移 2 小时 ⇒ +1h 那条转为逾期 ──────────────────
	moved := base.Add(2 * time.Hour)
	repo2 := New(setup)
	repo2.statsClock = func() time.Time { return moved }
	movedOthersP, movedOthersO := statsOthers(t, setup, moved)
	p2, o2, err := repo2.StatsPendingAndOverdueCount(ctx)
	if err != nil {
		t.Fatalf("StatsPendingAndOverdueCount(钟+2h): %v", err)
	}
	if want := movedOthersO + statsExpectedOverdue(base, 2*time.Hour); o2 != want || o2 <= got[1] {
		t.Errorf("钟前移 2 小时逾期数没跟着动：期望 %d（且应 > 原读数 %d），实得 %d", want, got[1], o2)
	}
	if p2 != movedOthersP+4 {
		t.Errorf("pending 半边被改动波及：期望 %d，实得 %d", movedOthersP+4, p2)
	}

	// ── 判据 4（回归 + 边界）：NULL 到期时间永不计入；钟拨到 +30 天只算 3 条 ────
	far := base.Add(30 * 24 * time.Hour)
	repo3 := New(setup)
	repo3.statsClock = func() time.Time { return far }
	farOthersP, farOthersO := statsOthers(t, setup, far)
	p3, o3, err := repo3.StatsPendingAndOverdueCount(ctx)
	if err != nil {
		t.Fatalf("StatsPendingAndOverdueCount(钟+30d): %v", err)
	}
	if want := farOthersO + statsExpectedOverdue(base, 30*24*time.Hour); o3 != want {
		t.Errorf("钟拨到 +30 天：夹具应只有 3 条逾期（NULL 那行不计），期望 %d 实得 %d", want, o3)
	}
	if p3 != farOthersP+4 {
		t.Errorf("钟拨到 +30 天 pending 应不变：期望 %d 实得 %d", farOthersP+4, p3)
	}

	// 同一注入值重复调用必须稳定（防"一次调用里取两次钟"）
	repo4 := New(setup)
	repo4.statsClock = func() time.Time { return base }
	var readings [][2]int
	for i := 0; i < 2; i++ {
		p, o, err := repo4.StatsPendingAndOverdueCount(ctx)
		if err != nil {
			t.Fatalf("重复调用第 %d 次: %v", i+1, err)
		}
		readings = append(readings, [2]int{p, o})
	}
	if readings[0] != readings[1] {
		t.Errorf("同注入钟下两次调用读数不稳：%v vs %v", readings[0], readings[1])
	}
}

// TestStatsSQLSourceHasNoNow 把"本函数的 SQL 里不许再出现时间函数"钉进用例
// （静态普查门禁 dbclock_census.py 管八栈，这条管这里不被后人顺手改回 NOW()）。
//
// 两个坑留档：① 本仓源码检出是 **CRLF**，按 `\n}\n` 找函数尾会找不到、于是把整个文件尾部
// 全吞进来；② 本案修复自己写的注释里就有 `NOW()` 字样——**不剥注释直接数会当场假红**。
// 所以先归一行尾、再剥整行注释、最后只在函数体内数。
func TestStatsSQLSourceHasNoNow(t *testing.T) {
	raw, err := os.ReadFile("jdbc.go")
	if err != nil {
		t.Fatalf("读 jdbc.go: %v", err)
	}
	src := strings.ReplaceAll(string(raw), "\r\n", "\n")
	const head = "func (r *Repository) StatsPendingAndOverdueCount"
	i := strings.Index(src, head)
	if i < 0 {
		t.Fatalf("在 jdbc.go 里找不到 %s —— 方法被改名/挪走，这条判据要先跟着调整", head)
	}
	rest := src[i+len(head):]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("函数体切不出边界（行尾分隔符没匹配上，本文件行尾 = %d CRLF）", strings.Count(string(raw), "\r\n"))
	}
	body := src[i : i+len(head)+end+3]

	var code strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue // 整行注释里写 NOW() 是在解释缺陷，不是病灶
		}
		code.WriteString(line)
		code.WriteString("\n")
	}
	if n := strings.Count(code.String(), "NOW()"); n != 0 {
		t.Errorf("StatsPendingAndOverdueCount 的代码里仍有 NOW()（命中 %d 处）⇒ 又把钟交给数据库了", n)
	}
	if !strings.Contains(code.String(), "expire_time < ?") {
		t.Error("逾期判据不是绑参形状（没找到 `expire_time < ?`）")
	}
	if !strings.Contains(code.String(), "r.statsNow()") {
		t.Error("逾期判据没走 statsNow() 时钟出口")
	}

	// 默认钟必须是"现在"，不能是零值
	repo := &Repository{}
	if time.Since(repo.statsNow()) > time.Minute {
		t.Errorf("statsNow() 默认值不像当前时刻：%v", repo.statsNow())
	}
}
