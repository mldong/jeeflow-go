// issues/137 A · 批二 §3-4（A 案）· go 腿的**真库一路**：`wf_process_instance.expire_time`
// 这一列进 MySQL DATETIME(3) 的读数（默认连 192.168.1.160:3306/jeeflow，与同包其余真库用例同一套夹具）。
//
// 为什么必须在真库再走一遍：内存仓的 SaveInstance 存的是**那一刻的结构体拷贝**，
// "只赋聚合对象、INSERT 不绑这一列"这种半截实现也能让内存侧的断言变绿；本栈 INSERT
// （repository/jdbc/jdbc.go:405-407）文本上确实带了 expire_time，但"列里真落了值、且落的是时刻"
// 只有在库上读出来才算（issues/113／142 同族教训）。
//
// 三格分工：
//   - TestInstanceExpireTimeMysqlColumnCarriesEvaluatedInstant —— 判据 1 的真库读数：
//     配 `"2h"` ⇒ 列里是**时刻**，且与同行 create_time 的差落进 2h 带宽（不是"非空"空判）。
//   - TestInstanceExpireTimeMysqlColumnStaysNullWhenUnconfigured —— 判据 3 的真库读数：
//     顶层没配 ⇒ 列 IS NULL（不是 now()、不是零值时刻、不是空串）。
//   - TestInstanceExpireTimeMysqlRejectsRawExpressionString —— 判据 1 的反面证据：
//     这一列**装不下**表达式原串（工单案文按 1366 记，兄弟栈 rust/php 本轮实测服务端给的是
//     1292/22007 `Incorrect datetime value`；本格把实得的服务端原文打出来，不锁错误码）。
//     ⇒ "先存原串回头再解析"在本栈根本没有出路。
//
// 跑法：
//
//	go test ./repository/jdbc/ -run TestInstanceExpireTime -count=1
//
// ⚠️ 本仓 `go test ./...` 不可重入（internal/flowsutil 会把 Java 源镜像进共享 flows/，
// 叠跑会造出"读取流程定义 JSON 失败"这类假红）；串行跑用 `-p 1`。
package jdbc_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// exp137aFlow 顶层带（或不带）expireTime 的最小线性流程 start → approve → end。
// **内联夹具**，不放进共享 flows/：internal/flowsutil.Dir() 的镜像步骤会按 Java 源删除本仓多出的
// 孤儿 *.json（放进去下一轮就被删掉，判据随之凭空消失），批二工单 §3-9 对 php 腿也是同一口径。
// rootSeg 传**完整顶层键片段**（`"expireTime":"2h",`），传 "" 即"缺键"档。
func exp137aFlow(name, rootSeg string) string {
	return `{"name":"` + name + `","displayName":"实例级到期真库取证","type":"approval",` +
		rootSeg + `"nodes":[` +
		`{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},` +
		`{"id":"approve","type":"snaker:task","properties":{"assignee":"zhangsan"},"text":{"value":"审批"}},` +
		`{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],` +
		`"edges":[` +
		`{"id":"e0","sourceNodeId":"start","targetNodeId":"approve","properties":{}},` +
		`{"id":"e1","sourceNodeId":"approve","targetNodeId":"end","properties":{}}]}`
}

// exp137aStart 放定义 → 起引擎（真库仓）→ 发起，返回实例 id。
// 定义 id 用同包既有的固定段（defineID=900001）；收尾清理由各测试自己按同包惯例
// `defer db.Close()` + `defer cleanup(t, db)` 登记（后者注册更晚 ⇒ 先清数据再关连接，
// 用 t.Cleanup 会在 Close 之后跑、拿的是已关闭的连接）。
func exp137aStart(t *testing.T, db *sql.DB, name, rootSeg string) int64 {
	t.Helper()
	cleanup(t, db)
	insertDefine(t, db, name, []byte(exp137aFlow(name, rootSeg)))
	repo := jdbc.New(db)
	eng := newEngine(t, repo)
	inst, err := eng.StartProcessInstanceByID(context.Background(), defineID, "zhangsan",
		map[string]interface{}{"BUSINESS_NO": "BIZ-137A"})
	if err != nil {
		t.Fatalf("发起流程失败: %v", err)
	}
	if inst == nil || inst.ID == 0 {
		// 前置判点，不是本案判据：本栈发起腿的 `e.repo.SaveInstance(ctx, inst)`
		// （engine_impl.go:78）**没有检查返回的 error**——变异对照实测到"这一列被写成库装不下的值"
		// （零值时刻 0001-01-01 ⇒ MySQL 1292）时，INSERT 被服务端拒了，引擎却照样往下跑，
		// 末尾 FindInstanceByID 读不到行 ⇒ 返回 (nil, nil)。这一句把它兜成明确失败，
		// 免得"发起成功了"被误读。（该吞错是本栈既有形状，与 issues/60 那条"错误必须传播"同源，
		// 批二 §3-4 范围外，未动——见交付报告"扫到没动"。）
		t.Fatalf("发起返回的实例为空/无 id: %+v", inst)
	}
	return inst.ID
}

// exp137aColumns 直查实例行的 expire_time / create_time（**绕过聚合水合**，直接读列）。
// NullTime 让 NULL 投成 Valid=false，而不是零值时刻——判据 3 要的就是这个区分。
func exp137aColumns(t *testing.T, db *sql.DB, instID int64) (expire, create sql.NullTime) {
	t.Helper()
	var exp, crt sql.NullTime
	err := db.QueryRowContext(context.Background(),
		ph("SELECT expire_time, create_time FROM wf_process_instance WHERE id = ?"), instID).
		Scan(&exp, &crt)
	if err != nil {
		t.Fatalf("读回实例列 %d: %v", instID, err)
	}
	return exp, crt
}

// exp137aSQLMode 读服务端的 sql_mode＋版本，让"原串为什么进不了这一列"在读数里自证一次。
func exp137aSQLMode(t *testing.T, db *sql.DB) string {
	t.Helper()
	var mode, ver string
	if err := db.QueryRowContext(context.Background(), "SELECT @@sql_mode, @@version").Scan(&mode, &ver); err != nil {
		return "读不到（" + err.Error() + "）"
	}
	return ver + " / " + mode
}

// 判据 1（真库读数）：配顶层 `"2h"` ⇒ DATETIME(3) 列里落的是**求值结果**（时刻），
// 与**同行** create_time 的差落在 2h 带宽内。判点打在库列上，不是引擎返回值上。
func TestInstanceExpireTimeMysqlColumnCarriesEvaluatedInstant(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	defer cleanup(t, db)

	instID := exp137aStart(t, db, "go-137a-2h", `"expireTime":"2h",`)
	expire, create := exp137aColumns(t, db, instID)
	if !expire.Valid {
		t.Fatalf("实例 %d 配了顶层 expireTime=\"2h\"，真库 expire_time 列却是 NULL（求值结果没落到列上）", instID)
	}
	if !create.Valid {
		t.Fatalf("实例 %d 的 create_time 为 NULL，带宽判据没有基准", instID)
	}
	delta := expire.Time.Sub(create.Time)
	if delta < 2*time.Hour-5*time.Second || delta > 2*time.Hour+60*time.Second {
		t.Errorf("真库 expire − create = %v，期望 2h0m0s±(5s,60s)；≈0＝占位 now()，零值时刻＝原串没被求值", delta)
	}
	t.Logf("真库读数：instance_id=%d expire_time=%s create_time=%s 差=%v 服务端=%s",
		instID, expire.Time.Format("2006-01-02 15:04:05.000"),
		create.Time.Format("2006-01-02 15:04:05.000"), delta, exp137aSQLMode(t, db))

	// 列 → 聚合水合那一路也要读得出同一个时刻（门面 detail 走的就是这条）：
	// 与上面直查列的值比对，防"写进去了但扫描层把这一列丢了"（issues/110 同族）。
	repo := jdbc.New(db)
	back, err := repo.FindInstanceByID(context.Background(), instID)
	if err != nil || back == nil {
		t.Fatalf("聚合水合读回实例: %v", err)
	}
	if back.ExpireTime == nil || !back.ExpireTime.Equal(expire.Time) {
		t.Errorf("水合读回 expire_time = %v，期望与库列同值 %v", back.ExpireTime, expire.Time)
	}
}

// 判据 3（真库读数）：顶层**没配** expireTime ⇒ 该列 IS NULL。
// 钉的是"不许赋 now()、不许赋零值时刻、不许赋空串"——NullTime.Valid 是唯一能区分
// "NULL" 与"零值时刻"的读法，所以这里不用 COALESCE/IFNULL 把 NULL 兜成非空。
func TestInstanceExpireTimeMysqlColumnStaysNullWhenUnconfigured(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	defer cleanup(t, db)

	instID := exp137aStart(t, db, "go-137a-none", "")
	expire, _ := exp137aColumns(t, db, instID)
	if expire.Valid {
		t.Errorf("顶层没配 expireTime，真库 expire_time 却被写成 %v（期望 NULL：不赋 now()、不赋零值时刻、不赋空串）",
			expire.Time.Format("2006-01-02 15:04:05.000"))
	}
	t.Logf("真库读数：instance_id=%d expire_time IS NULL = %v", instID, !expire.Valid)

	// 自证：这一格的 NULL 不是"实例根本没建出来"——同一行必须查得到 state（DOING）
	var state int
	if err := db.QueryRowContext(context.Background(),
		ph("SELECT state FROM wf_process_instance WHERE id = ?"), instID).Scan(&state); err != nil {
		t.Fatalf("自证实例行: %v", err)
	}
	if state != int(model.InstanceStateDoing) {
		t.Fatalf("自证失败：实例 state = %d，期望 10（DOING）——夹具没跑起来，本格就是空转", state)
	}
}

// 判据 1 的反面证据：表达式**原串**装不进这一列。
// STRICT_TRANS_TABLES 下服务端直接报错（工单案文按 1366 记，兄弟栈 rust/php 本轮实测拿到的是
// 1292/22007 `Incorrect datetime value: '2h' for column 'expire_time'` ⇒ 本格打印实得原文、
// 不锁错误码，免得换库/换 sql_mode 就假红）。
// 万一某台库是宽松模式（只告警不报错），退回判"存下来的值不是原串"：
// 两种结局都说明"搬原串"这条路在本栈走不通，判据不靠错误码吃饭。
func TestInstanceExpireTimeMysqlRejectsRawExpressionString(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	const probeID = int64(9137000001) // 本文件专用探测 id（不与 tsIDGen 的毫秒段时间戳 id 相撞）
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, ph("DELETE FROM wf_process_instance WHERE id = ?"), probeID); err != nil {
		t.Fatalf("前置清理探测行: %v", err)
	}
	// 探测行清理登记在 db.Close 之后 ⇒ defer 栈 LIFO，先删行再关连接
	defer func() {
		_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_instance WHERE id = ?"), probeID)
	}()

	_, err := db.ExecContext(ctx, ph(
		"INSERT INTO wf_process_instance (id, process_define_id, state, expire_time, create_time, create_user) "+
			"VALUES (?, ?, 10, ?, ?, 'go-137a')"),
		probeID, defineID, "2h", time.Now())
	if err != nil {
		t.Logf("服务端原文（原串 \"2h\" 进 DATETIME 列）: %v ｜ 服务端=%s", err, exp137aSQLMode(t, db))
		return
	}
	// 宽松模式退路口径：CAST 成 CHAR 读回来比（零值日期在驱动层就解析不出，同样走红/走日志）
	var stored sql.NullString
	if err := db.QueryRowContext(ctx,
		ph("SELECT CAST(expire_time AS CHAR) FROM wf_process_instance WHERE id = ?"), probeID).Scan(&stored); err != nil {
		t.Logf("原串插入竟未报错，但读回列时驱动就解析不出（库里存的是非法零值日期，同样证明原串进不了这一列）: %v", err)
		return
	}
	if stored.Valid && strings.TrimSpace(stored.String) == "2h" {
		t.Errorf("这一列竟原样存下了表达式原串 %q（判据 1 的前提在本环境不成立，需回报 owner）", stored.String)
	}
	t.Logf("该库为宽松模式：原串未报错，但存下来的值是 %v（不是 \"2h\"）｜ 服务端=%s",
		stored, exp137aSQLMode(t, db))
}
