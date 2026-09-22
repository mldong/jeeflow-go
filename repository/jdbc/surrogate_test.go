// 委托代理运行期生效 · SQL 仓侧（issues/116，规范 06 §4.5 / 08 用例 26、27）。
//
//	同包用例：
//	  TestSurrogateQueryParityJdbc    四判据对拍（与 memory/ext_parity_test.go 共用
//	                                  internal/surrparity 的数据集与期望表）
//	  TestSurrogateAutoApplyPersists  ①窗口内配"张三→李四"→ wf_process_task_actor 里
//	                                  李四真有一行、张三那行仍在；②窗外 / enabled=0 /
//	                                  自委托 → 李四无行；③未配扩展仓储 → 建单不被打断；
//	                                  ④显式关闭 → 回到仅台账
//
// 数据用 define 段 900001（沿用本包 cleanup/insertDefine），委托行用 910000~910099 段。
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/internal/surrparity"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// ─── 四判据对拍（SQL 仓）────────────────────────────────────────────────────────

func clearParityRows(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE id >= ? AND id <= ?"),
		surrparity.IDLow, surrparity.IDHigh); err != nil {
		t.Fatalf("clear parity surrogates: %v", err)
	}
}

func TestSurrogateQueryParityJdbc(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	clearParityRows(t, db)
	defer clearParityRows(t, db)

	ext := jdbc.NewExt(db)
	now := time.Now().Truncate(time.Millisecond)
	// 同一份夹具自证（与 memory/ext_parity_test.go 同一函数同一份数据），再对拍 SQL 仓。
	// ⚠️ 条款 1.4 在 SQL 侧的钉住点是 ext.go 的 `ORDER BY id DESC`：H2/InnoDB 按主键序回行，
	// 夹具里打乱的插入序在 SQL 侧不改变答案，两侧仍必须对同一期望负责（见 surrparity 包注释）。
	surrparity.Verify(t, now)
	surrparity.Run(t, ext, func(r surrparity.Row) error {
		var processName interface{} = r.ProcessName
		if r.NullProcessName {
			processName = nil // SQL 侧 NULL，内存侧 ""——判据 a 要求两者同属"全流程兜底"
		}
		var start, end interface{}
		if r.HasStart {
			start = now.Add(r.StartOff)
		}
		if r.HasEnd {
			end = now.Add(r.EndOff)
		}
		_, err := db.ExecContext(context.Background(), ph(
			"INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, start_time, end_time, enabled, create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
			r.ID, processName, r.Operator, r.Surrogate, start, end, r.Enabled,
			now, "go-test", now, "go-test")
		return err
	}, now)
}

// ─── 运行期自动并入参与者（SQL 落库断言）──────────────────────────────────────

const surrFlowContent = `{"name":"surr116","displayName":"委托生效流程","type":"approval",
 "nodes":[
   {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
   {"id":"task1","type":"snaker:task","properties":{"form":"f1","assignee":"zhangsan","taskType":0,"performType":0},"text":{"value":"审批"}},
   {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
 "edges":[
   {"id":"e0","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
   {"id":"e1","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`

// actorRows 读回 wf_process_task_actor 的 actor_id（断言落在持久值上，issues/113 教训）
func actorRows(t *testing.T, db *sql.DB, taskID int64) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), ph(
		"SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id ASC"), taskID)
	if err != nil {
		t.Fatalf("read task actors: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan actor: %v", err)
		}
		out = append(out, a)
	}
	return out
}

// startSurrFlow 用给定引擎装配状态发起一条流程，返回新建任务的参与者读回值
func startSurrFlow(t *testing.T, db *sql.DB, eng *engine.EngineImpl, repo *jdbc.Repository) []string {
	t.Helper()
	ctx := context.Background()
	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "boss1", map[string]interface{}{"BUSINESS_NO": "BIZ-SURR-116"})
	if err != nil {
		t.Fatalf("建单被打断: %v", err)
	}
	tasks, err := repo.FindDoingTasks(ctx, inst.ID, nil)
	if err != nil {
		t.Fatalf("find doing: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("doing 任务数 = %d, want 1", len(tasks))
	}
	return actorRows(t, db, tasks[0].ID)
}

func hasActor(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func putSurrogate(t *testing.T, ext *jdbc.ExtRepository, operator, surrogate, processName string, start, end *time.Time, enabled int) {
	t.Helper()
	if err := ext.SaveSurrogate(context.Background(), &model.ProcessSurrogate{
		Operator: operator, Surrogate: surrogate, ProcessName: processName,
		StartTime: start, EndTime: end, Enabled: enabled,
		CreateUser: "go-test", UpdateUser: "go-test",
	}); err != nil {
		t.Fatalf("save surrogate: %v", err)
	}
}

func ptrOf(ts time.Time) *time.Time { return &ts }

// TestSurrogateAutoApplyPersists issues/116 运行期语义：委托在**建单那一刻**并入参与者
// 并随任务落 wf_process_task_actor（不是靠事后 AddTaskActor 补写）。
func TestSurrogateAutoApplyPersists(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	cleanup(t, db)
	defer cleanup(t, db)
	ctx := context.Background()

	insertDefine(t, db, "surr116", []byte(surrFlowContent))
	repo := jdbc.New(db)
	ext := jdbc.NewExt(db)
	delSurr := func() {
		_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "zhangsan")
	}
	delSurr()
	defer delSurr()

	now := time.Now()
	inWindow := ptrOf(now.Add(-time.Hour))

	// ① 正向：窗口内配"张三→李四"
	putSurrogate(t, ext, "zhangsan", "lisi", "surr116", inWindow, ptrOf(now.Add(time.Hour)), 1)

	// Option 装配路（集成方一行开起来）
	eng := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil,
		engine.WithSurrogateRepository(ext))
	actors := startSurrFlow(t, db, eng, repo)
	if !hasActor(actors, "zhangsan") {
		t.Fatalf("① 授权人必须保留（任一可办），实际 actor 表读回 %v", actors)
	}
	if !hasActor(actors, "lisi") {
		t.Fatalf("① 被委托人未落库到 wf_process_task_actor，读回 %v", actors)
	}
	// 每人一行，未重复插入
	if len(actors) != 2 {
		t.Fatalf("① actor 行数 = %d (%v), want 2", len(actors), actors)
	}
	if id := mustFirstSurrogateID(t, db, "zhangsan"); id != 0 {
		if err := ext.RemoveSurrogate(ctx, id); err != nil {
			t.Fatalf("remove surrogate: %v", err)
		}
	}

	// ② 负向：窗外 / enabled=0 / 自委托 → 李四无行
	negatives := []struct {
		name            string
		operator, agent string
		start, end      *time.Time
		enabled         int
	}{
		{"窗外-已过期", "zhangsan", "lisi", ptrOf(now.Add(-10 * time.Hour)), ptrOf(now.Add(-9 * time.Hour)), 1},
		{"窗外-未开始", "zhangsan", "lisi", ptrOf(now.Add(9 * time.Hour)), nil, 1},
		{"enabled=0", "zhangsan", "lisi", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 0},
		{"自委托", "zhangsan", "zhangsan", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1},
	}
	for _, n := range negatives {
		t.Run(n.name, func(t *testing.T) {
			putSurrogate(t, ext, n.operator, n.agent, "surr116", n.start, n.end, n.enabled)
			defer func() {
				_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ? AND surrogate = ?"), n.operator, n.agent)
			}()
			eng := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil,
				engine.WithSurrogateRepository(ext))
			actors := startSurrFlow(t, db, eng, repo)
			if hasActor(actors, "lisi") || len(actors) != 1 || actors[0] != "zhangsan" {
				t.Fatalf("%s：不应产生代理人参与者，读回 %v", n.name, actors)
			}
		})
	}

	// ③ 未配置扩展仓储 → 建单不被打断（静默跳过，不抛错）
	engNoExt := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil)
	putSurrogate(t, ext, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	actors = startSurrFlow(t, db, engNoExt, repo)
	if len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("③ 未配扩展仓储应只落授权人，读回 %v", actors)
	}

	// ④ 显式关闭 → 回到"仅台账"
	engOff := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil,
		engine.WithSurrogateRepository(ext), engine.WithSurrogateAutoApply(false))
	if engOff.SurrogateAutoApply() {
		t.Fatalf("④ WithSurrogateAutoApply(false) 后开关仍为开启")
	}
	actors = startSurrFlow(t, db, engOff, repo)
	if len(actors) != 1 || actors[0] != "zhangsan" {
		t.Fatalf("④ 显式关闭后不应并入代理人，读回 %v", actors)
	}
	// 台账仍在（关闭只影响运行期，不影响 processSurrogate/*）
	if id := mustFirstSurrogateID(t, db, "zhangsan"); id == 0 {
		t.Fatalf("④ 关闭运行期后委托台账记录应仍存在")
	}
}

// TestSurrogateAutoApplyViaFacade 门面装配路：facade.New 注入扩展仓储即默认开启
// （集成方零额外配置，issues/116「引擎内置、默认开启」）。
func TestSurrogateAutoApplyViaFacade(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	cleanup(t, db)
	defer cleanup(t, db)
	ctx := context.Background()

	insertDefine(t, db, "surr116", []byte(surrFlowContent))
	repo := jdbc.New(db)
	ext := jdbc.NewExt(db)
	_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "zhangsan")
	defer db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "zhangsan")

	now := time.Now()
	putSurrogate(t, ext, "zhangsan", "lisi", "surr116", ptrOf(now.Add(-time.Hour)), ptrOf(now.Add(time.Hour)), 1)
	eng := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil)
	if eng.SurrogateRepository() != nil {
		t.Fatalf("装配前引擎不应已持有委托仓储")
	}
	// facade.New 自动把 ext 接到引擎的委托能力上（集成方零额外配置）
	fac := facade.New(eng, repo, ext)
	if fac == nil || eng.SurrogateRepository() == nil {
		t.Fatalf("facade.New 后引擎应默认获得委托扩展仓储")
	}
	if !eng.SurrogateAutoApply() {
		t.Fatalf("委托自动生效应默认开启")
	}
	inst, err := eng.StartProcessInstanceByID(ctx, defineID, "boss1", map[string]interface{}{"BUSINESS_NO": "BIZ-SURR-F"})
	if err != nil {
		t.Fatalf("facade 装配路建单被打断: %v", err)
	}
	tasks, _ := repo.FindDoingTasks(ctx, inst.ID, nil)
	if len(tasks) != 1 {
		t.Fatalf("doing 任务数 = %d, want 1", len(tasks))
	}
	actors := actorRows(t, db, tasks[0].ID)
	if !hasActor(actors, "lisi") {
		t.Fatalf("facade.New 装配后应默认开启委托生效，读回 %v", actors)
	}
}

func mustFirstSurrogateID(t *testing.T, db *sql.DB, operator string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRowContext(context.Background(), ph(
		"SELECT id FROM wf_process_surrogate WHERE operator = ? ORDER BY id DESC LIMIT 1"), operator).Scan(&id)
	if err != nil {
		return 0
	}
	return id
}

// ─── issues/123 任务 A/B（SQL 仓 + 建单落库断言）───────────────────────────────

// TestSurrogateIneffectiveExactRowStillFallsBackToAllFlow 钉跨作用域回落（SQL 仓侧，
// 与 engine/surrogate_test.go 同名用例同数据集，双仓必须同答案）：
// 全流程委托（process_name 空、窗内 enabled=1、更旧）+ 针对本流程的一条**更新但不生效**的委托
// ⇒ 代理人仍须由全流程委托并入。Java 参考实现既有测试
// JdbcProcessExtRepositoryTest#testSurrogateCrudAndGet 钉的正是「精确已过期 → 兜底全流程」。
func TestSurrogateIneffectiveExactRowStillFallsBackToAllFlow(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	cleanup(t, db)
	defer cleanup(t, db)
	ctx := context.Background()

	insertDefine(t, db, "surr116", []byte(surrFlowContent))
	repo := jdbc.New(db)
	ext := jdbc.NewExt(db)
	delAll := func() {
		_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "zhangsan")
	}
	delAll()
	defer delAll()

	now := time.Now()
	at := func(off time.Duration) *time.Time { p := now.Add(off); return &p }
	cases := []struct {
		name       string
		agent      string
		start, end *time.Time
		enabled    int
		wantAgent  string
	}{
		{"窗外（未来）⇒ 回落", "wangwu", at(9 * time.Hour), at(10 * time.Hour), 1, "lisi"},
		{"窗外（已过期）⇒ 回落", "wangwu", at(-10 * time.Hour), at(-9 * time.Hour), 1, "lisi"},
		{"enabled=0 ⇒ 回落", "wangwu", at(-time.Hour), at(time.Hour), 0, "lisi"},
		{"enabled=2 脏值 ⇒ 回落", "wangwu", at(-time.Hour), at(time.Hour), 2, "lisi"},
		{"自委托 ⇒ 回落", "zhangsan", at(-time.Hour), at(time.Hour), 1, "lisi"},
		{"正向对照：精确生效 ⇒ 用精确的代理人", "wangwu", at(-time.Hour), at(time.Hour), 1, "wangwu"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			delAll()
			putSurrogate(t, ext, "zhangsan", "lisi", "", at(-time.Hour), at(time.Hour), 1)
			putSurrogate(t, ext, "zhangsan", c.agent, "surr116", c.start, c.end, c.enabled)

			// 夹具自证：确有两条、且最新那条是本流程那条（否则"回落"是空转）
			var cnt int
			var newestName string
			if err := db.QueryRowContext(ctx, ph(
				"SELECT COUNT(*) FROM wf_process_surrogate WHERE operator = ?"), "zhangsan").Scan(&cnt); err != nil || cnt != 2 {
				t.Fatalf("台账应有 2 条，实际 cnt=%d err=%v", cnt, err)
			}
			if err := db.QueryRowContext(ctx, ph(
				"SELECT process_name FROM wf_process_surrogate WHERE operator = ? ORDER BY id DESC LIMIT 1"),
				"zhangsan").Scan(&newestName); err != nil || newestName != "surr116" {
				t.Fatalf("最新一条应落在 surr116 作用域，实际 %q err=%v", newestName, err)
			}

			eng := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil,
				engine.WithSurrogateRepository(ext))
			actors := startSurrFlow(t, db, eng, repo)
			if len(actors) != 2 || !hasActor(actors, "zhangsan") || !hasActor(actors, c.wantAgent) {
				t.Fatalf("%s：期望 [zhangsan %s]，读回 %v", c.name, c.wantAgent, actors)
			}
			if c.wantAgent == "lisi" && hasActor(actors, "wangwu") {
				t.Fatalf("%s：本流程那条不生效，wangwu 不得进参与者，读回 %v", c.name, actors)
			}
			if c.wantAgent == "wangwu" && hasActor(actors, "lisi") {
				t.Fatalf("%s：精确作用域生效时全流程代理人不得并列，读回 %v", c.name, actors)
			}
		})
	}
}

// TestSurrogateNewestRowDecidesNoMerge 同一授权人+同一流程先有一条「窗内 enabled=1」，
// 之后用户再新建一条**更新**的窗外 / enabled=0 / enabled=2 脏值 / 自委托记录：
// 建单时必须**不**并入代理人——规范 06 §4.5 条款 1.4 要求「先按 id 取最新一条，再由四判据
// 裁决这一条」。反过来写（SQL 先把不生效的滤掉，剩下的才取最新）等价于
// 「历史上留过一条窗内委托就永久生效」，正是 issues/123 里 13 栈 L2-17/L2-18 全红的成因。
//
// 每组都配一条正向对照（最新一条窗内 enabled=1 ⇒ 必须并入），防判据被写反成「恒不并入」
// 后本用例仍空转绿（issues/123 任务 B）。
func TestSurrogateNewestRowDecidesNoMerge(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	cleanup(t, db)
	defer cleanup(t, db)
	ctx := context.Background()

	insertDefine(t, db, "surr116", []byte(surrFlowContent))
	repo := jdbc.New(db)
	ext := jdbc.NewExt(db)
	delAll := func() {
		_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "zhangsan")
	}
	delAll()
	defer delAll()

	now := time.Now()
	at := func(off time.Duration) *time.Time { p := now.Add(off); return &p }

	cases := []struct {
		name               string
		agent              string
		start, end         *time.Time
		enabled            int
		newestMustBeMerged bool
	}{
		{"A1 最新一条窗外（未来窗口）", "lisi", at(9 * time.Hour), at(10 * time.Hour), 1, false},
		{"A2 最新一条窗外（已过期）", "lisi", at(-10 * time.Hour), at(-9 * time.Hour), 1, false},
		{"A3 最新一条 enabled=0", "lisi", at(-time.Hour), at(time.Hour), 0, false},
		{"A4 最新一条 enabled=2 脏值", "lisi", at(-time.Hour), at(time.Hour), 2, false},
		{"A5 最新一条自委托", "zhangsan", at(-time.Hour), at(time.Hour), 1, false},
		{"B 正向对照：最新一条窗内 enabled=1 ⇒ 并入", "lisi", at(-time.Hour), at(time.Hour), 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			delAll() // 每组从"只有一条窗内生效行"开始
			// 更旧的一条：窗内 + enabled=1 —— 旧写法会把它当成"最新生效行"永远命中
			putSurrogate(t, ext, "zhangsan", "lisi", "surr116", at(-time.Hour), at(time.Hour), 1)
			// 更新的一条：按某判据不生效（正向对照组则是生效的）
			putSurrogate(t, ext, "zhangsan", c.agent, "surr116", c.start, c.end, c.enabled)

			// 种子自证：台账确有两条，"不并入"不是因为数据没进去（issues/113 教训）
			var cnt int
			if err := db.QueryRowContext(ctx, ph(
				"SELECT COUNT(*) FROM wf_process_surrogate WHERE operator = ?"), "zhangsan").Scan(&cnt); err != nil || cnt != 2 {
				t.Fatalf("%s：台账应有 2 条（更旧生效行 + 最新一条），实际 cnt=%d err=%v", c.name, cnt, err)
			}

			eng := engine.New(repo, &noopUserProvider{}, &tsIDGen{base: time.Now().UnixMilli() * 1000}, nil,
				engine.WithSurrogateRepository(ext))
			actors := startSurrFlow(t, db, eng, repo)
			if c.newestMustBeMerged {
				if !hasActor(actors, "lisi") || !hasActor(actors, "zhangsan") {
					t.Fatalf("%s：最新一条生效 ⇒ 代理人应并入且授权人保留，读回 %v", c.name, actors)
				}
				if len(actors) != 2 {
					t.Fatalf("%s：参与者 = %v, want [zhangsan lisi]", c.name, actors)
				}
				return
			}
			if len(actors) != 1 || actors[0] != "zhangsan" {
				t.Fatalf("%s：最新一条不生效 ⇒ 不得并入代理人、也不得回落到更旧那条，读回 %v", c.name, actors)
			}
		})
	}
}
