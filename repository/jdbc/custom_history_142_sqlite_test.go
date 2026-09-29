// issues/142 A 批（go 腿）· §6.2 第 1 条"历史行必须真落库"的 **SQL 侧取证**
// （T0，临时文件 sqlite 库，不连 MySQL/PG、不碰 160、不起服务、不占端口）。
//
// 为什么必须在 SQL 仓再走一遍：内存仓的 SaveInstance/UpdateInstance 存的是**同一个 variables
// map 引用**，"只塞进聚合/内存结构"这种半截实现也能让内存侧的断言变绿；只有 SQL 仓会
// 在 INSERT/UPDATE 那一刻把值序列化进列，之后**查得到才算做到**。
// 这正是 §6.2 第 1 条点名的两处缺陷之一（java `CustomModel.exec` 丢弃 createHistoryTask 的返回值
// 只进 `instance.tasks`，而 `persistTasks` 只保存 `exec.getProcessTaskList()`、
// `updateInstance` 的级联又只 UPDATE 已有行 ⇒ 那条 DONE 行永远进不了库；c# 同形）。
// 本栈反着做：`engine_impl.go` 记录类腿走 `repo.SaveTask`（那条 INSERT 通道）。
//
// 跑法：
//
//	go test ./repository/jdbc/ -run 'TestIssue142Jdbc'
package jdbc_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
)

// c142OpenSQLite 临时**文件**库（不是 :memory:），建本栈五张 wf_* 表。
//
// 为什么不复用同目录的 openSQLite129：那一格是 `:memory:` + `SetMaxOpenConns(1)`，
// 而本栈 jdbc 仓的 `findTasksByState` 在**外层游标未关**时就发第二条
// `findTaskActors` 查询（repository/jdbc/jdbc.go:523-538，游标 + 批查参与者），
// 引擎发起路径末尾的 `FindInstanceByID` 必走这条嵌套（engine_impl.go:86）。
// `:memory:` 库是连接私有的 ⇒ 只能单连接 ⇒ 嵌套查询等第二把连接**永远等不到**，
// 实测整包 hang 到 go test 10 分钟超时（这是本仓既有的取证局限，141 那批 sqlite 用例
// 都绕开了引擎发起路径；本轮要取证"历史行真进库"就绕不开，故改用文件库 + 多连接）。
// 建表 DDL 与 openSQLite129 逐字一致，只在连接形态上不同。
func c142OpenSQLite(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "c142.db")) +
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(4) // 允许引擎读路径的嵌套查询（见上面注释）
	ctx := context.Background()
	stmts := []string{
		`CREATE TABLE wf_process_define (
			id INTEGER PRIMARY KEY, name TEXT NOT NULL, display_name TEXT NOT NULL, type TEXT,
			state INTEGER, content BLOB, version INTEGER, create_time TIMESTAMP, create_user TEXT,
			update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_instance (
			id INTEGER PRIMARY KEY, parent_id INTEGER, process_define_id INTEGER, state INTEGER,
			parent_node_name TEXT, business_no TEXT, operator TEXT, expire_time TIMESTAMP, variable TEXT,
			create_time TIMESTAMP, create_user TEXT, update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_task (
			id INTEGER PRIMARY KEY, process_instance_id INTEGER NOT NULL, task_name TEXT NOT NULL,
			display_name TEXT NOT NULL, task_type INTEGER, perform_type INTEGER, task_state INTEGER,
			operator TEXT, finish_time TIMESTAMP, expire_time TIMESTAMP, form_key TEXT,
			task_parent_id INTEGER, variable TEXT, create_time TIMESTAMP, create_user TEXT,
			update_time TIMESTAMP, update_user TEXT)`,
		`CREATE TABLE wf_process_task_actor (
			id INTEGER PRIMARY KEY, process_task_id INTEGER NOT NULL, actor_id TEXT NOT NULL,
			create_time TIMESTAMP, create_user TEXT)`,
		`CREATE TABLE wf_process_cc_instance (
			id INTEGER PRIMARY KEY, process_instance_id INTEGER NOT NULL, actor_id TEXT NOT NULL,
			state INTEGER, create_time TIMESTAMP, create_user TEXT, update_time TIMESTAMP, update_user TEXT)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

// 本案专用 id 段（9142xxxx，避开 129/141 的 9141xxxx 与 T1 真库用例的 900001）
const c142DefineID = int64(91420001)

const c142Clazz = "com.mldong.jeeflow.test.TestCustomHandler"

// c142StubHandler 记录类处理器桩：返回固定值，用来验"clazz 真被执行 + 返回值真进库"。
type c142StubHandler struct{ ret interface{} }

func (h c142StubHandler) Handle(*model.FlowNode, *model.ProcessInstance, string, map[string]interface{}) (interface{}, error) {
	return h.ret, nil
}

// c142NewEngine SQL 仓 + 引擎 + 注册表（注册 clazz 原名 → 桩处理器）。
// events 收全部事件，用于在 SQL 侧也钉"记录类不发码 3"。
func c142NewEngine(t *testing.T, db *sql.DB, events *[]string) (*engine.EngineImpl, *jdbc.Repository, *engine.HandlerRegistry) {
	t.Helper()
	repo := jdbc.New(db)
	eng := engine.New(repo, &noopUserProvider{},
		&tsIDGen{base: time.Now().UnixMilli()*4000 + testStackSeq*1000}, nil)
	eng.SetExtensions(&engine.Extensions{
		Listeners: []engine.ProcessEventListener{func(evt engine.ProcessEvent) {
			if events != nil {
				*events = append(*events, evt.Type.SpecName()+":"+evt.NodeID)
			}
		}},
	})
	reg := engine.NewHandlerRegistry()
	eng.SetRegistry(reg)
	return eng, repo, reg
}

func c142PutDefine(t *testing.T, db *sql.DB, name, content string) int64 {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DELETE FROM wf_process_define WHERE id=?`, c142DefineID); err != nil {
		t.Fatalf("清定义: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO wf_process_define (id,name,display_name,type,state,content,version) VALUES (?,?,?, 'approval',1,?,1)`,
		c142DefineID, name, "142 SQL 侧取证", []byte(content)); err != nil {
		t.Fatalf("放定义: %v", err)
	}
	return c142DefineID
}

// c142TaskRows 按节点名读回 wf_process_task 真实行（取证直接查表，不看引擎返回值）。
func c142TaskRows(t *testing.T, db *sql.DB, node string) []map[string]interface{} {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, task_name, display_name, task_state, IFNULL(operator,'<NULL>') AS operator,
		        IFNULL(task_parent_id,-1) AS parent, IFNULL(form_key,'<NULL>') AS form_key,
		        perform_type, task_type, IFNULL(variable,'{}') AS variable,
		        CASE WHEN finish_time IS NULL THEN 0 ELSE 1 END AS has_finish,
		        CASE WHEN expire_time IS NULL THEN 0 ELSE 1 END AS has_expire
		 FROM wf_process_task WHERE task_name=? ORDER BY id`, node)
	if err != nil {
		t.Fatalf("取证 task 行: %v", err)
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var (
			id                                                 int64
			taskName, displayName, operator, formKey, variable string
			state, parent, perform, taskType, hasFinish, hasEx int
		)
		if err := rows.Scan(&id, &taskName, &displayName, &state, &operator, &parent, &formKey,
			&perform, &taskType, &variable, &hasFinish, &hasEx); err != nil {
			t.Fatalf("scan task 行: %v", err)
		}
		out = append(out, map[string]interface{}{
			"id": id, "task_name": taskName, "display_name": displayName, "task_state": state,
			"operator": operator, "task_parent_id": parent, "form_key": formKey,
			"perform_type": perform, "task_type": taskType, "variable": variable,
			"has_finish_time": hasFinish, "has_expire_time": hasEx,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历 task 行: %v", err)
	}
	return out
}

func c142countOn(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("计数 %q: %v", q, err)
	}
	return n
}

func c142instanceState(t *testing.T, db *sql.DB, id int64) int {
	t.Helper()
	var s int
	if err := db.QueryRowContext(context.Background(),
		`SELECT state FROM wf_process_instance WHERE id=?`, id).Scan(&s); err != nil {
		t.Fatalf("读实例 state: %v", err)
	}
	return s
}

func c142instanceVariable(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var v string
	if err := db.QueryRowContext(context.Background(),
		`SELECT IFNULL(variable,'') FROM wf_process_instance WHERE id=?`, id).Scan(&v); err != nil {
		t.Fatalf("读实例 variable: %v", err)
	}
	return v
}

// TestIssue142JdbcCustomHistoryRowReallyInserted start→custom(注册处理器)→end 走 SQL 仓：
// wf_process_task 里查得到那条 task_state=20 的行；参与者表带的是留痕主体（不是空串）；
// 实例继续到 end；返回值真进实例 variable 列；码 3 不为它出现；库里一条待办都没有。
//
// 改前必红（还原成"custom 走 createTask"）：那一行的 task_state 是 10、
// form/待办计数与事件序列全都不符；还原成"只 append 进聚合不 SaveTask"：
// 这一格直接红在"库里查不到行"（就是 java/c# 那个洞的形状）。
func TestIssue142JdbcCustomHistoryRowReallyInserted(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	c142PutDefine(t, db, "c142-custom", `{"name":"c142-custom","displayName":"142记录类","type":"approval","nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"custom1","type":"snaker:custom","properties":{"clazz":"`+c142Clazz+`"},"text":{"value":"通知外部系统"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e1","sourceNodeId":"start","targetNodeId":"custom1","properties":{}},
	  {"id":"e2","sourceNodeId":"custom1","targetNodeId":"end","properties":{}}]}`)

	var events []string
	eng, repo, reg := c142NewEngine(t, db, &events)
	reg.RegisterCustom(c142Clazz, c142StubHandler{ret: "SQL-142"})

	inst, err := eng.StartProcessInstanceByID(context.Background(), c142DefineID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	rows := c142TaskRows(t, db, "custom1")
	if len(rows) != 1 {
		t.Fatalf("§6.2 第 1 条：custom 的历史行必须真落库，实得 %d 行", len(rows))
	}
	row := rows[0]
	if row["task_state"].(int) != int(model.TaskStateDone) {
		t.Fatalf("落库那条应 task_state=20，实得 %v", row["task_state"])
	}
	if row["operator"].(string) != "zhangsan" {
		t.Fatalf("留痕主体（operator 列）应为当前操作人，实得 %q", row["operator"])
	}
	if row["task_parent_id"].(int) != 0 {
		t.Fatalf("建单不变量①：发起路径 task_parent_id 应落 0（不是 NULL），实得 %v", row["task_parent_id"])
	}
	if row["has_expire_time"].(int) != 0 {
		t.Fatalf("记录类行不该有到期时间，实得 %v", row)
	}
	if row["has_finish_time"].(int) != 1 {
		t.Fatal("记录类行 finish_time 应有值（本栈 DONE 行口径）")
	}
	if row["display_name"].(string) != "通知外部系统" {
		t.Fatalf("display_name 应取节点文本，实得 %q", row["display_name"])
	}
	// 参与者表：一行，值是留痕主体（不是空串／'<nil>'／'null' 那族垃圾形状）
	if n := c142countOn(t, db, `SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id=? AND actor_id='zhangsan'`, row["id"].(int64)); n != 1 {
		t.Fatalf("历史行应带 1 条参与者（留痕主体），实得 %d", n)
	}
	if n := c142countOn(t, db, `SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id=? AND TRIM(actor_id)=''`, row["id"].(int64)); n != 0 {
		t.Fatalf("不许往 actor_id 灌空归属值（issues/129／141 B 病根），实得 %d 条", n)
	}
	// 库里没有任何待办
	if n := c142countOn(t, db, `SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id=? AND task_state=10`, inst.ID); n != 0 {
		t.Fatalf("记录类节点不该产生待办，实得 %d 条", n)
	}
	// 实例真走到 end（state=20 落在行上，不是只在内存里）
	if s := c142instanceState(t, db, inst.ID); s != int(model.InstanceStateDone) {
		t.Fatalf("实例应办结 state=20，实得 %d", s)
	}
	// 返回值真进实例变量列
	if v := c142instanceVariable(t, db, inst.ID); !strings.Contains(v, "custom_return_val") || !strings.Contains(v, "SQL-142") {
		t.Fatalf("clazz 返回值应落进实例 variable 列，实得 %q", v)
	}
	// 码 3 不为它出现
	for _, e := range events {
		if strings.Contains(e, "PROCESS_TASK_START") {
			t.Fatalf("记录类节点不该 fire 码 3，实得事件 %v", events)
		}
	}
	// SPI 读路也查得到（门面/UI 就靠这条）
	back, err := repo.FindTaskByID(context.Background(), row["id"].(int64))
	if err != nil || back == nil || back.TaskState != model.TaskStateDone {
		t.Fatalf("FindTaskByID 读回历史行不符: %+v/%v", back, err)
	}
}

// TestIssue142JdbcZeroActorRowInsertedWithoutActorRows 任务类零参与者在 SQL 仓的形状：
// wf_process_task 有那一行（task_state=10），wf_process_task_actor **零行**
// ——黑洞关掉的同时不制造空归属值。再验加派参与者后同一行能被办动（SQL 仓一路闭环）。
//
// 改前必红：还原成 `if len(actors) == 0 { return nil }` ⇒ 库里那一行都没有。
func TestIssue142JdbcZeroActorRowInsertedWithoutActorRows(t *testing.T) {
	db := c142OpenSQLite(t)
	defer db.Close()
	c142PutDefine(t, db, "c142-zero", `{"name":"c142-zero","displayName":"142零参与者","type":"approval","nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"task1","type":"snaker:task","properties":{"performType":0},"text":{"value":"没人可办那一格"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e1","sourceNodeId":"start","targetNodeId":"task1","properties":{}},
	  {"id":"e2","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`)

	var events []string
	eng, repo, _ := c142NewEngine(t, db, &events)
	inst, err := eng.StartProcessInstanceByID(context.Background(), c142DefineID, "zhangsan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	rows := c142TaskRows(t, db, "task1")
	if len(rows) != 1 {
		t.Fatalf("零参与者也应落一条 DOING 行（§6.2 第 3 条），实得 %d 行", len(rows))
	}
	row := rows[0]
	if row["task_state"].(int) != int(model.TaskStateDoing) {
		t.Fatalf("行状态应为 10，实得 %v", row["task_state"])
	}
	if n := c142countOn(t, db, `SELECT COUNT(*) FROM wf_process_task_actor WHERE process_task_id=?`, row["id"].(int64)); n != 0 {
		t.Fatalf("零参与者行不该有任何 actor 行，实得 %d", n)
	}
	if op := row["operator"].(string); op == "<nil>" || op == "null" {
		t.Fatalf("operator 列不许出现垃圾字面串，实得 %q", op)
	}
	if s := c142instanceState(t, db, inst.ID); s != int(model.InstanceStateDoing) {
		t.Fatalf("实例应停在 state=10（这一格确实没办完），实得 %d", s)
	}
	// 加派参与者 ⇒ 同一行办得动 ⇒ SQL 仓一路闭环到 end
	if err := repo.AddTaskActor(context.Background(), row["id"].(int64), []string{"leader01"}); err != nil {
		t.Fatalf("加派参与者: %v", err)
	}
	if _, err := eng.ExecuteProcessTask(context.Background(), row["id"].(int64), "leader01",
		map[string]interface{}{"submitType": 1}); err != nil {
		t.Fatalf("加派后应办得动: %v", err)
	}
	if s := c142instanceState(t, db, inst.ID); s != int(model.InstanceStateDone) {
		t.Fatalf("办结后实例应 state=20，实得 %d", s)
	}
	if n := c142countOn(t, db, `SELECT COUNT(*) FROM wf_process_task WHERE process_instance_id=? AND task_state=10`, inst.ID); n != 0 {
		t.Fatalf("办结后不该留待办，实得 %d", n)
	}
}
