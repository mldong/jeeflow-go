// issues/137 §3-1（spec 06-facade.md §2.12）门面内部异常出口的 go 腿取证。
//
// 判据形状照 java 基准 FacadeInternalErrorNoLeakTest：**两侧都要有牙**。
//
//	负向＝运行时/解析器/驱动/第三方 provider 写的原文一律不得进对外 msg，
//	      出口必须**逐字**等于「流程处理失败」（断言用相等不用 Contains——
//	      宽松匹配会让"固定文案＋原文尾巴"蒙过去，issues/134 同口径）；
//	正向＝引擎自己写的契约文案必须**逐字**留在 msg。这一组同样重要：判据过宽会把契约面
//	      静默改掉，而八栈＋十三壳＋前端 toast 都按这些原文对齐（java 侧这类文案一处测试都没钉）。
//	      本文件把 spec 06「失败 msg 跨栈统一文案」表里的逐字串钉进来，另加两条 go 特有的：
//	      ① 引擎写的 **ASCII** 契约文案（`operator hacker not allowed`，既有 facade_test.go:1888
//	      就靠它断言参与者错误）——按"有没有中文"收窄会当场红；
//	      ② `id 缺失或非法: <strconv 原文>` 这条**既有形状**（spec 06 removeTaskActor 第 8 条
//	      明确"各栈既有形状为准、不作跨栈判据"，既有 mustIDsRejected 注释写着"Go 侧允许带诊断后缀"）。
//
// 第三侧＝原文没被"整个丢掉"：必须能证明它进了**日志与错误链**（只断言 msg 的话，
// 把原文直接扔掉也能绿，排查能力静默归零，正是 facade_cause_chain_139_test.go 立过的同一面旗）。
//
// 夹具姿势照 java：不改产品代码去造异常，而是给门面塞一个**按测试意图返回 error / 直接 panic 的
// 仓储**（java 用 Proxy.newProxyInstance，go 用接口内嵌＋覆写单个方法），走**真实出口路径**。
package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

// ─── 夹具 ──────────────────────────────────────────────────────────────────────

// i137Repo 按测试意图让 processInstance/page 这条腿返回 error 或直接 panic。
// 其余方法一律用不到（真被调到就是内嵌的 nil 接口解引用 panic ⇒ 夹具失效会响亮暴露，不会假绿）。
type i137Repo struct {
	spi.ProcessRepository
	pageErr   error
	pagePanic interface{}
	doPanic   bool
}

func (r *i137Repo) PageInstances(context.Context, spi.PageQuery, string) ([]*model.InstanceRow, int, error) {
	if r.doPanic {
		panic(r.pagePanic)
	}
	return nil, 0, r.pageErr
}

// i137BizRepo bizData 那条腿的夹具：只实现 bizData 真会调到的两个查询，
// 让流程走到 metaReader（persist 侧驱动错误的注入口）。
type i137BizRepo struct {
	spi.ProcessRepository
	defineContent []byte
}

func (r *i137BizRepo) FindInstanceByID(context.Context, int64) (*model.ProcessInstance, error) {
	return &model.ProcessInstance{ID: 91137001, DefineID: 91137002, State: model.InstanceStateDoing}, nil
}

func (r *i137BizRepo) FindDefineByID(context.Context, int64) (*model.ProcessDefine, error) {
	return &model.ProcessDefine{ID: 91137002, Name: "i137", Content: r.defineContent}, nil
}

// i137MetaReader persist.MetaReader 的替身：返回调用方指定的 error（模拟驱动层失败）。
type i137MetaReader struct{ err error }

func (m *i137MetaReader) ReadByProcessInstance(string, interface{}) (interface{}, error) {
	return nil, m.err
}

// i137CaptureLog 把标准库 log 的输出接到 buf（门面用的就是既有 logger：log.Printf("[jeeflow] …")）。
// 这是"原文只进日志"那一半的取证口——java 基准用 Logger.addHandler，go 侧等价物就是换 output。
func i137CaptureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0) // 去掉时间戳前缀，断言只对着我们写的那段文案
	t.Cleanup(func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) })
	return &buf
}

// i137MysqlErr 真驱动错误类型（*mysql.MySQLError，定义在引擎模块之外）。
// 用它而不是自定义类型，是为了让判据 5「动态类型的包路径不在引擎模块内」真被走到：
// 测试文件里自定义的类型 PkgPath 恰恰是 github.com/mldong/jeeflow-go/facade（在引擎模块内），
// 拿它当"第三方"会把判据 5 测成恒假绿。
func i137MysqlErr() error {
	return &mysql.MySQLError{
		Number:   1146,
		SQLState: [5]byte{'4', '2', 'S', '0', '2'},
		Message:  "Table 'jeeflow.wf_process_instance' doesn't exist",
	}
}

// i137RuntimeError 从真实 panic 里恢复出一个 runtime.Error（java NPE 那一档的 go 对应物）。
// shape="map" ⇒ 写 nil map（assignment to entry in nil map）；shape="ptr" ⇒ 解引用 nil 指针
// （runtime error: invalid memory address or nil pointer dereference）。
func i137RuntimeError(t *testing.T, shape string) error {
	t.Helper()
	var out error
	func() {
		defer func() {
			p := recover()
			if p == nil {
				t.Fatalf("夹具失效：%s 形状没能 panic 出 runtime.Error", shape)
			}
			out = panicAsError(p)
		}()
		switch shape {
		case "map":
			var m map[string]string
			m["actor_id"] = "leader" // 写 nil map
		default:
			var p *int
			sum := *p + 1 // 解引用 nil 指针（有读有写，编译器消不掉）
			_ = sum
		}
	}()
	var rtErr runtime.Error
	if !errors.As(out, &rtErr) {
		t.Fatalf("夹具失效：恢复出来的不是 runtime.Error，实得 %T: %v", out, out)
	}
	return out
}

// i137JSONSyntaxError 真的从 json.Unmarshal 里取一个 *json.SyntaxError（不手写字符串冒充）。
func i137JSONSyntaxError(t *testing.T) error {
	t.Helper()
	var m map[string]interface{}
	err := json.Unmarshal([]byte(`{"name":"broken",`), &m)
	if err == nil {
		t.Fatal("夹具失效：坏 JSON 竟然解析成功")
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("夹具失效：期望 *json.SyntaxError，实得 %T: %v", err, err)
	}
	return err
}

// i137NumError 真的从 strconv.Atoi 里取一个 *strconv.NumError，
// 文案逐字就是启动词点名的那句 strconv.Atoi: parsing "x": invalid syntax。
func i137NumError(t *testing.T) error {
	t.Helper()
	_, err := strconv.Atoi("x")
	if err == nil {
		t.Fatal("夹具失效：Atoi(\"x\") 竟然没报错")
	}
	if got := err.Error(); got != `strconv.Atoi: parsing "x": invalid syntax` {
		t.Fatalf("夹具失效：NumError 文案变了，实得 %q", got)
	}
	return err
}

// i137MustInternalFailure 断言失败信封：code=99999999 ＋ msg **逐字**等于固定文案
// ＋ 原文的任何标志性片段都没漏进 msg。
func i137MustInternalFailure(t *testing.T, label string, r map[string]interface{}, rawText string) {
	t.Helper()
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("%s 应失败且 code=99999999，实得 %v", label, r)
	}
	msg, ok := r["msg"].(string)
	if !ok {
		t.Fatalf("%s msg 应为 string，实得 %T (%v)", label, r["msg"], r["msg"])
	}
	if msg != msgInternalFailure {
		t.Fatalf("%s msg 应逐字等值 %q，实得 %q ⇒ 内部原文泄漏或文案漂移", label, msgInternalFailure, msg)
	}
	if rawText != "" && strings.Contains(msg, rawText) {
		t.Fatalf("%s msg 里出现了原文片段 %q: %q", label, rawText, msg)
	}
	// data 也不许带原文（spec §2.12：不得拼进任何其它对外字段）
	if r["data"] != nil {
		t.Fatalf("%s 失败信封 data 应为 nil，实得 %v", label, r["data"])
	}
}

// i137FlowPage 走真实出口路径调一次 processInstance/page。
func i137FlowPage(repo spi.ProcessRepository) map[string]interface{} {
	f := &Facade{repo: repo}
	return f.Flow("processInstance/page", map[string]interface{}{
		"operator": "user1", "pageNum": 1, "pageSize": 10,
	})
}

// ─── ① 判别式本体（纯函数，无副作用） ────────────────────────────────────────────

// TestIssue137IsForeignDetailClassifiesByAuthor 判据本体：按「这段文案是谁写的」分类。
// 表里 want=false 那一半与 want=true 那一半同等重要——前者是"别把契约面收窄掉"的护栏。
func TestIssue137IsForeignDetailClassifiesByAuthor(t *testing.T) {
	jsonSyntax := i137JSONSyntaxError(t)
	numErr := i137NumError(t)
	mysqlErr := i137MysqlErr()
	rtMap := i137RuntimeError(t, "map")
	rtPtr := i137RuntimeError(t, "ptr")

	var jsonTypeErr error
	{
		var target struct {
			Name string `json:"name"`
		}
		jsonTypeErr = json.Unmarshal([]byte(`{"name":123}`), &target)
		if jsonTypeErr == nil {
			t.Fatal("夹具失效：类型不符的 JSON 竟然解析成功")
		}
	}

	cases := []struct {
		label string
		err   error
		want  bool // true ⇒ 属内部信息 ⇒ 出口只给固定文案
	}{
		// ── 内部：运行时/解析器/IO/驱动/第三方 provider ──
		{"nil error 不判内部", nil, false},
		{"json 语法错原文", jsonSyntax, true},
		{"json 类型不符原文", jsonTypeErr, true},
		{"strconv.Atoi 原文", numErr, true},
		{"runtime.Error·写 nil map", rtMap, true},
		{"runtime.Error·解引用 nil 指针", rtPtr, true},
		{"裸包装 %w（包装层一个字没写）", fmt.Errorf("%w", errors.New("内部驱动细节 12345")), true},
		{"DB 驱动错误（类型定义在引擎模块外）", mysqlErr, true},
		{"驱动错误被 provider 前缀包一层", fmt.Errorf("acmereco: page instances: %w", mysqlErr), true},
		{"persist 腿 %w 包驱动错误", fmt.Errorf("persist: query biz_demo failed: %w", mysqlErr), true},
		{"引擎腿 %w 包解析器错误", fmt.Errorf("parse flow: %w", jsonSyntax), true},
		{"io.EOF 被前缀包一层", fmt.Errorf("read define content: %w", io.EOF), true},
		{"%v 压平后只剩字符串（无链无类型）", fmt.Errorf("persist: parse meta i137.json failed: %v", jsonSyntax), true},
		{"provider 返回网络层原文（无类型）", errors.New("dial tcp 10.0.0.7:3306: connect: connection refused"), true},
		{"文案为空串", errors.New(""), true},
		{"文案为纯空白", errors.New("   \t "), true},

		// ── 契约：引擎自己写的文案，一律逐字透出 ──
		{"spec06 operator 必填", errors.New("operator 必填"), false},
		{"spec06 任务不存在", errors.New("任务不存在"), false},
		{"spec06 任务非进行中，不可摘除参与人", errors.New("任务非进行中，不可摘除参与人"), false},
		{"spec06 至少需保留一名参与人", errors.New("至少需保留一名参与人"), false},
		{"spec06 无权限摘除该任务参与人", errors.New("无权限摘除该任务参与人"), false},
		{"spec06 processTaskId/actorIds 缺失", errors.New("processTaskId/actorIds 缺失"), false},
		{"门面 流程定义解析失败", errors.New("流程定义解析失败"), false},
		{"门面 业务数据读取器未注册", errors.New("业务数据读取器未注册（facade.SetMetaReader(...)）"), false},
		// 判据 2 的关键格：facadeFixedError 的 cause 里躺着的正是 *json.SyntaxError，
		// 先走链就会把这条跨栈逐字契约文案误判成内部（139 那批测试会当场红）。
		{"139 固定文案＋解析器 cause（判据 2 必须先于 3/4/5）",
			fixedMsgError(msgReadFlowDefineJSONFailed, jsonSyntax), false},
		{"139 固定文案＋驱动 cause", fixedMsgError(msgReadFlowDefineJSONFailed, mysqlErr), false},
		// 引擎写的 ASCII 契约文案：既有 facade_test.go:1888 就靠它断言参与者错误
		{"引擎 ASCII 契约文案 operator … not allowed", fmt.Errorf("operator %s not allowed", "hacker"), false},
		{"引擎 ASCII 契约文案 task not found", fmt.Errorf("task not found: %d", int64(91137)), false},
		{"panic 收敛出来的门面自有文案", errors.New("未配置 ProcessExtRepository（扩展仓储）"), false},
		// 既有形状：门面用 %v 把 strconv 原文拼进自己的中文诊断后缀（无链、无类型）。
		// spec 06 removeTaskActor 第 8 条明确"各栈既有形状为准、不作跨栈判据"，
		// 既有 mustIDsRejected 注释写着"Go 侧允许带诊断后缀，故用 contains 而非相等"⇒ 不收窄。
		{"既有形状 id 缺失或非法: <strconv 原文>", fmt.Errorf("id 缺失或非法: %v", numErr), false},
		{"既有形状 超出 float64 精确范围", fmt.Errorf("id %v 超出 float64 精确范围（2^53），请以字符串传递", 2.0843205438341243e+18), false},
	}

	for _, c := range cases {
		if got := isForeignDetail(c.err); got != c.want {
			t.Errorf("%s：isForeignDetail = %v，want %v（err=%v）", c.label, got, c.want, c.err)
		}
	}
}

// TestIssue137ForeignTextSignatureIsPureStringJudgement 文案签名那半段单独可测
// （spec §2.12「文案判据与副作用各自可测」）：不必先造一个被 %v 压平的 error 才能验它。
//
// ⚠️ 签名表只是判据 4 的**兜底**（给 %v 压平后无链无类型的文案用）。带类型的驱动错误走判据 4/5
// 更稳（*mysql.MySQLError 的 PkgPath 在引擎模块外），不靠签名——所以下面 mysql 那格取的是
// 唯一键冲突这种**签名表里确有对应串**的形状（Duplicate entry），而不是"表不存在"
// （`Error 1146 (42S02): Table … doesn't exist` 不含任何签名串，靠类型判，不靠文案判）。
func TestIssue137ForeignTextSignatureIsPureStringJudgement(t *testing.T) {
	for _, foreign := range []string{
		`invalid character 'n' looking for beginning of value`,
		"unexpected end of JSON input",
		"json: cannot unmarshal number into Go struct field .name of type string",
		"runtime error: invalid memory address or nil pointer dereference",
		"assignment to entry in nil map",
		"index out of range [3] with length 0",
		"Error 1062 (23000): Duplicate entry '8601' for key 'wf_process_task_actor.PRIMARY'",
		"dial tcp 10.0.0.7:3306: connectex: No connection could be made",
		"sql: Scan error on column index 0, name \"id\": converting driver.Value",
		"persist: parse meta i137.json failed: unexpected EOF",
	} {
		if !foreignTextSignature(foreign) {
			t.Errorf("应判为外来文案：%q", foreign)
		}
	}
	for _, own := range []string{
		"operator 必填", "任务不存在", "任务非进行中，不可摘除参与人", "至少需保留一名参与人",
		"读取流程定义 JSON 失败", "流程定义解析失败", "未配置 ProcessExtRepository（扩展仓储）",
		"operator hacker not allowed", "task not found: 91137", "id 缺失或非法: 缺失",
		`id 缺失或非法: strconv.ParseInt: parsing "": invalid syntax`,
		"unsupported type for nullTimeScan: *string",
		"未知 action: foo/bar",
	} {
		if foreignTextSignature(own) {
			t.Errorf("引擎自己的文案被签名误伤（会静默改掉契约面）：%q", own)
		}
	}
}

// TestIssue137ForeignTypeOriginDerefsPointer 判据 5 的回归护栏（本轮实测踩到的坑）。
//
// reflect.Type.PkgPath() 只对**具名**类型非空，而 error 几乎全是指针形状 ⇒ 直接取一律得到空串，
// 判据 5 恒假、整条形同虚设：DB 驱动错误因此原样外透（本轮第一次跑就红在这儿）。
// 谁把它"简化"回 reflect.TypeOf(err).PkgPath() 都会当场红。
func TestIssue137ForeignTypeOriginDerefsPointer(t *testing.T) {
	cases := []struct {
		label    string
		err      error
		wantPkg  string // errorTypePkgPath：只看**最外层**动态类型的出身
		wantFore bool   // foreignTypeOrigin：走**整条链**，任一环出身在引擎模块外即 true
	}{
		{"驱动错误（第三方模块的具名类型）", i137MysqlErr(), "github.com/go-sql-driver/mysql", true},
		{"json 解析器（标准库但非 errors/fmt）", i137JSONSyntaxError(t), "encoding/json", true},
		{"strconv（标准库但非 errors/fmt）", i137NumError(t), "strconv", true},
		{"errors.New（引擎借来的笔）", errors.New("operator 必填"), "errors", false},
		{"fmt.Errorf 无 %w（引擎借来的笔）", fmt.Errorf("operator %s not allowed", "hacker"), "errors", false},
		// 包装层出身是 fmt（不外来），但链上那一环是 encoding/json ⇒ 整条链判外来
		{"fmt.Errorf 有 %w（外层 fmt、内层 json）", fmt.Errorf("parse flow: %w", i137JSONSyntaxError(t)), "fmt", true},
		// ⚠️ 这一格是"判据 2 必须排在 3/4/5 之前"的活证：139 固定文案载体的 cause 里躺着
		// *json.SyntaxError，单看出身它**是**外来 ⇒ 若判据 2 不先短路，isForeignDetail 会把
		// 「读取流程定义 JSON 失败」这条跨栈逐字契约文案换成固定文案（139 那批测试当场红）。
		{"139 固定文案载体（引擎模块内，但 cause 是外来的）",
			fixedMsgError(msgReadFlowDefineJSONFailed, i137JSONSyntaxError(t)),
			"github.com/mldong/jeeflow-go/facade", true},
	}
	for _, c := range cases {
		if got := errorTypePkgPath(c.err); got != c.wantPkg {
			t.Errorf("%s：errorTypePkgPath = %q，want %q（剥指针那步失效了？）", c.label, got, c.wantPkg)
		}
		if got := foreignTypeOrigin(c.err); got != c.wantFore {
			t.Errorf("%s：foreignTypeOrigin = %v，want %v", c.label, got, c.wantFore)
		}
	}
	// 判据 2 短路必须压过判据 5：契约文案仍逐字透出
	fixed := fixedMsgError(msgReadFlowDefineJSONFailed, i137JSONSyntaxError(t))
	if isForeignDetail(fixed) {
		t.Fatal("判据 2 未短路：139 固定文案被链上的解析器 cause 带成内部（契约面被静默改掉）")
	}
}

// ─── ② 覆盖面第一处：门面顶层 error 出口 ─────────────────────────────────────────

// TestIssue137ForeignErrorNeverReachesMsg 各泄漏形状走真实出口 ⇒ msg 逐字「流程处理失败」。
func TestIssue137ForeignErrorNeverReachesMsg(t *testing.T) {
	jsonSyntax := i137JSONSyntaxError(t)
	numErr := i137NumError(t)
	mysqlErr := i137MysqlErr()

	cases := []struct {
		label string
		err   error
		leak  string // 一旦出现在 msg 里就算泄漏的标志性片段
	}{
		{"json.Unmarshal 原文", jsonSyntax, "invalid character"},
		{"json.Unmarshal 截断", func() error {
			var m map[string]interface{}
			return json.Unmarshal([]byte(`{"a":`), &m)
		}(), "unexpected end of JSON input"},
		{"strconv.Atoi 原文", numErr, `parsing "x"`},
		{"runtime.Error·写 nil map", i137RuntimeError(t, "map"), "assignment to entry in nil map"},
		{"runtime.Error·解引用 nil 指针", i137RuntimeError(t, "ptr"), "invalid memory address"},
		{"裸包装 fmt.Errorf(%w)", fmt.Errorf("%w", errors.New("内部驱动细节 12345")), "12345"},
		{"DB 驱动错误", mysqlErr, "1146"},
		{"驱动错误＋provider 前缀", fmt.Errorf("acmereco: page instances: %w", mysqlErr), "acmereco"},
		{"persist 腿包驱动错误", fmt.Errorf("persist: query biz_demo failed: %w", mysqlErr), "persist:"},
		{"引擎腿包解析器错误", fmt.Errorf("parse flow: %w", jsonSyntax), "parse flow"},
		{"%v 压平成纯字符串", fmt.Errorf("persist: parse meta i137.json failed: %v", jsonSyntax), "persist:"},
		{"第三方 provider 的网络层原文", errors.New("dial tcp 10.0.0.7:3306: connect: connection refused"), "10.0.0.7"},
		{"文案为空串", errors.New(""), ""},
	}

	for _, c := range cases {
		logBuf := i137CaptureLog(t)
		r := i137FlowPage(&i137Repo{pageErr: c.err})
		i137MustInternalFailure(t, c.label, r, c.leak)

		// 第三侧：原文进了日志（只断言 msg 的话，"把原文整个丢掉"也能绿）
		logged := logBuf.String()
		if !strings.Contains(logged, msgInternalFailure) {
			t.Errorf("%s：日志应记下对外给的是固定文案，实得 %q", c.label, logged)
		}
		if !strings.Contains(logged, "processInstance/page") {
			t.Errorf("%s：日志要指出是哪个 action，实得 %q", c.label, logged)
		}
		if want := c.err.Error(); want != "" && !strings.Contains(logged, want) {
			t.Errorf("%s：原文没进日志（排查能力归零），期望日志含 %q，实得 %q", c.label, want, logged)
		}
	}
}

// ─── ③ 覆盖面第一处之二：recover（panic）出口 ────────────────────────────────────

// TestIssue137RecoveredPanicKeepsInternalsOutOfMsg 门面**有** recover（facade.go Flow 顶部），
// 所以 panic 不会崩进程、而是变成 msg ⇒ 这一档必须同过判别式。
// 旧形状 errorResult(fmt.Sprintf("%v", p)) 对任何 panic 值都照搬。
func TestIssue137RecoveredPanicKeepsInternalsOutOfMsg(t *testing.T) {
	cases := []struct {
		label string
		p     interface{}
		leak  string
	}{
		// 真从 panic 里恢复出来的 runtime.Error（保留动态类型，走判据 4）
		{"nil map 写入恢复出的 runtime.Error", i137RuntimeError(t, "map"), "assignment to entry in nil map"},
		{"nil 指针解引用恢复出的 runtime.Error", i137RuntimeError(t, "ptr"), "invalid memory address"},
		{"persist 腿 panic 包驱动错误", fmt.Errorf("persist: %w", i137MysqlErr()), "1146"},
		{"persist 腿 panic 纯字符串＋解析器原文", fmt.Sprintf("persist: parse meta i137.json failed: %v", i137JSONSyntaxError(t)), "invalid character"},
		{"裸字符串运行时原文", "runtime error: invalid memory address or nil pointer dereference", "invalid memory address"},
	}

	for _, c := range cases {
		logBuf := i137CaptureLog(t)
		r := i137FlowPage(&i137Repo{doPanic: true, pagePanic: c.p})
		i137MustInternalFailure(t, "panic/"+c.label, r, c.leak)
		if !strings.Contains(logBuf.String(), "processInstance/page") {
			t.Errorf("panic/%s：日志要指出是哪个 action，实得 %q", c.label, logBuf.String())
		}
	}
}

// TestIssue137NilMapPanicRealPath 不靠夹具塞 panic 值，直接让门面内部真的写 nil map
// （启动词点名的"nil map/slice 解引用恢复出来的 runtime.Error"那条真路径）。
func TestIssue137NilMapPanicRealPath(t *testing.T) {
	logBuf := i137CaptureLog(t)
	r := i137FlowPage(&i137NilMapPanicRepo{})
	i137MustInternalFailure(t, "真路径 nil map panic", r, "nil map")
	if !strings.Contains(logBuf.String(), "assignment to entry in nil map") {
		t.Errorf("原文应进日志，实得 %q", logBuf.String())
	}
}

// i137NilMapPanicRepo PageInstances 里真的写一次 nil map ⇒ 门面 recover 到 runtime.Error。
type i137NilMapPanicRepo struct{ spi.ProcessRepository }

func (r *i137NilMapPanicRepo) PageInstances(context.Context, spi.PageQuery, string) ([]*model.InstanceRow, int, error) {
	var m map[string]string
	m["actor_id"] = "leader" // 写 nil map ⇒ runtime.Error
	return nil, 0, nil
}

// ─── ④ 覆盖面第二处：bizData / JSON 解析族 ───────────────────────────────────────

// TestIssue137BizDataDriverErrorKeepsInternalsOutOfMsg bizData 的 metaReader 腿：
// persist 侧真实形状是 fmt.Errorf("persist: query %s failed: %w", table, driverErr)，
// 139 那轮只钉了 deploy/redeploy 的 JSON 腿，这条驱动腿本轮由顶层判别式兜住。
func TestIssue137BizDataDriverErrorKeepsInternalsOutOfMsg(t *testing.T) {
	logBuf := i137CaptureLog(t)
	driverErr := i137MysqlErr()
	f := &Facade{
		repo:       &i137BizRepo{defineContent: []byte(`{"name":"i137","relTableName":"biz_demo"}`)},
		metaReader: &i137MetaReader{err: fmt.Errorf("persist: query %s failed: %w", "biz_demo", driverErr)},
	}
	r := f.Flow("processInstance/bizData", map[string]interface{}{"processInstanceId": int64(91137001)})
	i137MustInternalFailure(t, "bizData/驱动错误", r, "biz_demo")
	if !strings.Contains(logBuf.String(), "processInstance/bizData") {
		t.Errorf("日志要指出是哪个 action，实得 %q", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "1146") {
		t.Errorf("驱动原文应进日志，实得 %q", logBuf.String())
	}
}

// TestIssue137BizDataJSONLegStillGivesContractText bizData 的 JSON 腿本来就干净
// （facade.go 用 errors.New("流程定义解析失败")，没把解析器原文拼进去）⇒ 本格是**证据**：
// 坏 JSON 的定义内容仍给引擎自己那句契约文案，没被本轮判别式误伤成固定文案。
func TestIssue137BizDataJSONLegStillGivesContractText(t *testing.T) {
	logBuf := i137CaptureLog(t)
	f := &Facade{
		repo:       &i137BizRepo{defineContent: []byte(`{"name":"broken",`)},
		metaReader: &i137MetaReader{},
	}
	r := f.Flow("processInstance/bizData", map[string]interface{}{"processInstanceId": int64(91137001)})
	if code, _ := r["code"].(int); code != 99999999 {
		t.Fatalf("坏 JSON 的定义应失败，实得 %v", r)
	}
	if msg, _ := r["msg"].(string); msg != "流程定义解析失败" {
		t.Fatalf("bizData JSON 腿应逐字给引擎契约文案「流程定义解析失败」，实得 %q", msg)
	}
	if logBuf.Len() != 0 {
		t.Errorf("引擎自己写的文案不该被记成内部异常日志，实得 %q", logBuf.String())
	}
}

// TestIssue137ParseFamilyKeepsFixedTextAndCause 139 那轮已到位的解析族，本轮的**证据格**：
// ① 出口 msg 逐字是「读取流程定义 JSON 失败」（不是固定文案，也没带解析器原文）；
// ② 原文确实留在错误链上（errors.Unwrap / errors.As 都取得回）——判据 2 之所以要"不下探 cause"
// 正是为了保住这一对分工。
func TestIssue137ParseFamilyKeepsFixedTextAndCause(t *testing.T) {
	logBuf := i137CaptureLog(t)
	f := &Facade{}
	_, err := f.deploy(map[string]interface{}{"content": `{"name":"broken",`})
	if err == nil {
		t.Fatal("坏 JSON 的 deploy 应返回 error")
	}
	// 出口映射走的就是本轮改过的那一行
	r := errorResult(outwardFailureMsg("processDefine/deploy", err))
	if msg, _ := r["msg"].(string); msg != msgReadFlowDefineJSONFailed {
		t.Fatalf("解析族出口 msg 应逐字 %q（判据 2 逐字透出），实得 %q —— 若实得 %q 则是判据过宽、契约面被静默改掉",
			msgReadFlowDefineJSONFailed, msg, msgInternalFailure)
	}
	// 原文仍在链上
	if errors.Unwrap(err) == nil {
		t.Fatal("cause 丢了：错误链只剩固定文案")
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("cause 应为 *json.SyntaxError，实得 %T: %v", err, err)
	}
	if logBuf.Len() != 0 {
		t.Errorf("引擎契约文案不该触发内部异常日志，实得 %q", logBuf.String())
	}
}

// ─── ⑤ 正向：引擎契约文案逐字透出（判据过宽的护栏） ───────────────────────────────

// TestIssue137EngineContractTextStillPassesThroughVerbatim 走真实出口路径逐字断言。
// 收窄成"一律固定文案"⇒ 本函数整片红（这就是改前红样的第二条变异要打的靶）。
func TestIssue137EngineContractTextStillPassesThroughVerbatim(t *testing.T) {
	cases := []struct {
		label string
		err   error
	}{
		{"spec06 operator 必填", errors.New("operator 必填")},
		{"spec06 fromActor 必填", errors.New("fromActor 必填")},
		{"spec06 toActor 必填", errors.New("toActor 必填")},
		{"spec06 任务不存在", errors.New("任务不存在")},
		{"spec06 任务非进行中，不可摘除参与人", errors.New("任务非进行中，不可摘除参与人")},
		{"spec06 至少需保留一名参与人", errors.New("至少需保留一名参与人")},
		{"spec06 无权限撤回该流程实例", errors.New("无权限撤回该流程实例")},
		{"spec06 原办理人不是该任务参与人", errors.New("原办理人不是该任务参与人")},
		{"spec06 目标人已是该任务参与人", errors.New("目标人已是该任务参与人")},
		{"spec06 上一步任务ID为空", errors.New("上一步任务ID为空，无法驳回至上一步处理")},
		{"139 读取流程定义 JSON 失败（带解析器 cause）",
			fixedMsgError(msgReadFlowDefineJSONFailed, i137JSONSyntaxError(t))},
		{"门面 流程定义不存在", errors.New("流程定义不存在")},
		{"门面 业务数据读取器未注册", errors.New("业务数据读取器未注册（facade.SetMetaReader(...)）")},
		{"引擎 ASCII 契约文案（既有 facade_test.go:1888 依赖）", fmt.Errorf("operator %s not allowed", "hacker")},
		{"引擎 ASCII 契约文案 task not found", fmt.Errorf("task not found: %d", int64(91137))},
		{"既有形状 id 缺失或非法＋strconv 后缀", fmt.Errorf("id 缺失或非法: %v", i137NumError(t))},
	}

	for _, c := range cases {
		logBuf := i137CaptureLog(t)
		r := i137FlowPage(&i137Repo{pageErr: c.err})
		if code, _ := r["code"].(int); code != 99999999 {
			t.Fatalf("%s 应失败且 code=99999999，实得 %v", c.label, r)
		}
		msg, _ := r["msg"].(string)
		if msg != c.err.Error() {
			t.Errorf("%s：msg 应逐字透出 %q，实得 %q", c.label, c.err.Error(), msg)
		}
		if msg == msgInternalFailure {
			t.Errorf("%s：契约文案被换成了固定文案（判据过宽 ⇒ 契约面被静默改掉）", c.label)
		}
		// 引擎自己写的文案不该被记成内部异常（java 基准同一条断言：CAPTURED.isEmpty()）
		if logBuf.Len() != 0 {
			t.Errorf("%s：引擎契约文案不该触发内部异常日志，实得 %q", c.label, logBuf.String())
		}
	}
}

// TestIssue137RealPathContractTextsVerbatim 不靠夹具塞 error，走真门面路径取几条逐字文案
// （证明判别式接上去以后真实调用链上的契约面没动）。
func TestIssue137RealPathContractTextsVerbatim(t *testing.T) {
	logBuf := i137CaptureLog(t)

	// 未知 action：直接 return，不过判别式，逐字仍带 action 名
	f := &Facade{}
	if r := f.Flow("foo/bar", nil); r["msg"] != "未知 action: foo/bar" {
		t.Errorf("未知 action 文案应逐字透出，实得 %v", r["msg"])
	}
	// operator 必填（withdraw 在碰仓储之前就报）
	if r := f.Flow("processInstance/withdraw", map[string]interface{}{"id": int64(91137001)}); r["msg"] != "operator 必填" {
		t.Errorf("withdraw 缺 operator 应逐字「operator 必填」，实得 %v", r["msg"])
	}
	// 扩展仓储未配置：门面自己 panic 出来的引擎契约文案，recover 后仍逐字透出
	if r := f.Flow("processDesign/page", nil); r["msg"] != "未配置 ProcessExtRepository（扩展仓储）" {
		t.Errorf("未配扩展仓储应逐字透出门面自有文案，实得 %v", r["msg"])
	}
	if r := f.Flow("processTask/removeTaskActor", map[string]interface{}{
		"processTaskId": int64(1), "actorIds": []interface{}{"leader"},
	}); r["msg"] != "operator 必填" {
		t.Errorf("removeTaskActor 缺 operator 应逐字「operator 必填」，实得 %v", r["msg"])
	}
	// 三条都是引擎自己写的文案 ⇒ 一条内部异常日志都不该有
	if logBuf.Len() != 0 {
		t.Errorf("引擎契约文案不该触发内部异常日志，实得 %q", logBuf.String())
	}
}

// ─── ⑥ 第三侧：原文没被丢掉（日志＋错误链各自可测） ──────────────────────────────

// TestIssue137ForeignDetailGoesToLogAndErrorChain 副作用半段。
// 只断言 msg 的话，"把原文整个丢掉"也能绿 —— 排查能力静默归零，正是 139 那批立过的同一面旗。
func TestIssue137ForeignDetailGoesToLogAndErrorChain(t *testing.T) {
	driverErr := i137MysqlErr()
	wrapped := fmt.Errorf("persist: query biz_demo failed: %w", driverErr)

	// ① 日志：原文＋完整错误链＋action 都在，且明说对外只给了固定文案
	logBuf := i137CaptureLog(t)
	if got := outwardFailureMsg("processInstance/page", wrapped); got != msgInternalFailure {
		t.Fatalf("出口 msg 应为固定文案，实得 %q", got)
	}
	logged := logBuf.String()
	for _, want := range []string{
		"processInstance/page",           // 哪个 action
		"persist: query biz_demo failed", // 外层原文
		"Error 1146",                     // 内层驱动原文（证明链被逐层展开，不是只打最外层）
		"*fmt.wrapError",                 // 每层的动态类型
		"*mysql.MySQLError",              //
		msgInternalFailure,               // 对外给的是哪句
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("日志应含 %q，实得 %q", want, logged)
		}
	}

	// ② 错误链：固定文案载体的 cause 用 errors.Unwrap / errors.As 都取得回
	fixed := fixedMsgError(msgInternalFailure, wrapped)
	if fixed.Error() != msgInternalFailure {
		t.Fatalf("Error() 应逐字是固定文案，实得 %q", fixed.Error())
	}
	if errors.Unwrap(fixed) != wrapped {
		t.Fatalf("errors.Unwrap 应取回原始 error，实得 %v", errors.Unwrap(fixed))
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(fixed, &mysqlErr) {
		t.Fatal("errors.As 应能穿透两层拿到驱动错误")
	}
	if mysqlErr.Number != 1146 {
		t.Fatalf("驱动错误码应原样保留，实得 %d", mysqlErr.Number)
	}

	// ③ panic 腿的日志带上栈（java 基准 log.log(SEVERE, …, e) 的 go 等价物）
	logBuf.Reset()
	if got := outwardPanicMsg("processTask/execute", wrapped); got != msgInternalFailure {
		t.Fatalf("panic 出口 msg 应为固定文案，实得 %q", got)
	}
	if !strings.Contains(logBuf.String(), "goroutine ") {
		t.Errorf("panic 腿日志应带栈，实得 %q", logBuf.String())
	}
}

// TestIssue137PanicAsErrorKeepsTypeAndChain panic 值归一：是 error 就**保留动态类型与链**
// （判据 3/4/5 才有东西可判），不是 error 才按 %v 转文案。
// 旧形状一律 fmt.Sprintf("%v", p) ⇒ 类型信息当场蒸发，只剩字符串。
func TestIssue137PanicAsErrorKeepsTypeAndChain(t *testing.T) {
	driverErr := i137MysqlErr()
	wrapped := fmt.Errorf("persist: %w", driverErr)

	got := panicAsError(wrapped)
	if got != wrapped {
		t.Fatalf("panic 值是 error 时应原样保留，实得 %T: %v", got, got)
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(got, &mysqlErr) {
		t.Fatal("归一后仍应能 As 出驱动错误（类型信息没被蒸发）")
	}

	rtErr := i137RuntimeError(t, "map")
	if !isForeignDetail(panicAsError(rtErr)) {
		t.Fatal("归一后的 runtime.Error 仍应判为内部")
	}

	// 不是 error 的 panic 值（门面自己那句字符串）按 %v 转文案，逐字保留
	str := panicAsError("未配置 ProcessExtRepository（扩展仓储）")
	if str.Error() != "未配置 ProcessExtRepository（扩展仓储）" {
		t.Fatalf("字符串 panic 值应逐字转成文案，实得 %q", str.Error())
	}
	if isForeignDetail(str) {
		t.Fatal("门面自己写的 panic 文案不该被判成内部（会把契约文案换成固定文案）")
	}
}
