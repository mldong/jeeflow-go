// issues/137 §3-1（spec 06-facade.md §2.12）门面内部异常出口 —— 固定文案「流程处理失败」
// ＋「这段文案是谁写的」判别式（go 腿）。
//
// # 本栈的落点与 java 基准的差别（动手前现读普查的结论，别照抄 java）
//
// go 没有异常机制，门面拿到的全是 error 值，于是五条判据的落点跟 java 不一样：
//
//	判据 1（message 为 null）    ⇒ go：Error() 是空串/纯空白。recover 到 nil 也落这一档。
//	判据 2（契约异常族）         ⇒ go：*facadeFixedError。它是本模块**唯一**的具名 error 类型
//	                                （issues/139 那枚；核法：grep `^func .*) Error() string` 全仓只命中它
//	                                ——注意别用不带 `^func` 的形状去 grep，会命中注释里的转述自造假阳性），
//	                                语义正是"这段 msg 是给外面看的、原文只在链上"⇒ 直接复用，
//	                                不另起一套（spec §2.11 尾注「不要抄第二份」同一条精神）。
//	判据 3（裸包装）             ⇒ go：err.Error() 与 errors.Unwrap(err).Error() 逐字相同，
//	                                即 fmt.Errorf("%w", e) 这种包装层一个字都没写的形状。
//	判据 4（运行时/IO/驱动类型族）⇒ go：runtime.Error（recover 出来的 nil 解引用/越界/除零/类型断言）、
//	                                *reflect.ValueError、encoding/json 四型、*strconv.NumError、
//	                                net/os/fs/syscall 那族、io.EOF 与 context 两个哨兵。
//	判据 5（抛出点不在引擎主包）  ⇒ go：**部分落地**。error 值不带栈，`runtime.Caller` 只在构造点可用、
//	                                事后取不到 ⇒ 无法按"抛出点"判。可用的是**动态类型的包路径**
//	                                （reflect.TypeOf(err).PkgPath()）：凡类型定义在引擎模块之外
//	                                （驱动、第三方 provider 自己的错误类型、非 errors/fmt 的标准库类型）
//	                                一律算外来。这一条**不用枚举驱动**就兜住了 mysql/pgx/sqlite 全家。
//	                                ⚠️ 缺位的一角：第三方 provider 返回**无类型**的
//	                                errors.New("...")/fmt.Errorf("...") 时，go 侧拿不到任何来源信号
//	                                （动态类型都是 *errors.errorString，与引擎自己写的契约文案同型），
//	                                只能靠判据 4 补充的文案签名兜；签名也没有的那种（例如 provider 返回
//	                                errors.New("boom")）**挡不住**。java 靠栈帧能挡，go 挡不住，这是本栈的
//	                                真实缺口，不要用"看起来覆盖了"糊过去。
//
// # 判据 4 在 go 侧必须额外补一条「文案签名」
//
// java 的异常对象一路带着类型上来，go 不是：本仓 25 处 `fmt.Errorf("...: %v", err)` 用 %v（不是 %w）
// 把下层 error **压平成字符串**，链断了、类型也没了（errors.Unwrap 返回 nil），判据 3/4/5 全部无从下手。
// 典型是 persist/meta.go:167 `panic(fmt.Sprintf("persist: parse meta %s failed: %v", path, err))`
// —— panic 值是个纯字符串，里头躺着 JSON 解析器原文。所以对**已经没有类型信息**的文案，只能按签名认。
//
// ⚠️ 签名表**故意不收** strconv 那句 `invalid syntax`/`value out of range`：门面既有形状是
// `fmt.Errorf("id 缺失或非法: %v", err)`（toInt64 那条腿，25 处同形），spec 06 removeTaskActor 第 8 条
// 明确写「processTaskId 给了但不是数字/超出精度 ⇒ 各栈既有形状为准、不作跨栈判据」，
// 且既有测试 facade_test.go mustIDsRejected 的注释就写着「Go 侧允许带诊断后缀，故用 contains 而非相等」。
// 收进来会把那条既有形状改成固定文案 ⇒ 既有门禁当场红。**裸**的 *strconv.NumError 仍由判据 4 按类型挡下
// （它没被 %v 压平时类型还在），所以"解析器原文不得外透"这条并不因此失守。
package facade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

// msgInternalFailure 门面捕获到**非引擎契约异常**时对外只说这一句（八栈逐字同一串，不许改措辞）。
// owner 2026-10-02 第 3 问拍 A，进第八条逐字契约文本；java 基准＝JeeflowFacade.INTERNAL_FAILURE_MSG。
const msgInternalFailure = "流程处理失败"

// engineModulePath 判据 5 的"引擎主包"在 go 侧的对应物（java 是 com.mldong.jeeflow. 前缀）。
const engineModulePath = "github.com/mldong/jeeflow-go/"

// plainErrorPkgs 标准库里**引擎自己用来写契约文案**的两个包：errors.New ⇒ *errors.errorString、
// fmt.Errorf ⇒ *errors.errorString（无 %w）/ *fmt.wrapError（有 %w）。这两个包的类型是"引擎借来的笔"，
// 不代表外来 —— 不排除它们，判据 5 会把全部 58 处 errors.New 契约文案一并判成内部（灾难性收窄）。
var plainErrorPkgs = map[string]bool{"errors": true, "fmt": true}

// foreignTextSignatures 判据 4 的 go 补充：类型信息被 %v 压平后，只能按文案签名认。
// 每条都是运行时/标准库/驱动**自己**写的、引擎文案里不可能出现的串（已全仓 grep 核对零误伤）。
var foreignTextSignatures = []string{
	// runtime：panic 值经 %v 压平后只剩这些串
	"runtime error:", "invalid memory address", "nil pointer dereference",
	"index out of range", "slice bounds out of range", "assignment to entry in nil map",
	"integer divide by zero", "interface conversion:", "close of nil channel",
	"negative shift amount", "comparing uncomparable type",
	// encoding/json 解析器
	"invalid character ", "unexpected end of JSON input", "cannot unmarshal ",
	"json: unsupported", "json: cannot unmarshal", "unexpected EOF",
	// database/sql 与驱动
	"sql: ", "driver: ", "unsupported Scan", "converting driver.Value",
	"SQLSTATE", "database is locked", "Duplicate entry ", "deadlock found",
	// net / os
	"dial tcp", "connectex:", "connection refused", "connection reset by peer",
	"i/o timeout", "no such host", "permission denied",
}

// isForeignDetail 判「这条 error 的文案能不能原样进对外 msg」——**纯函数**，无副作用（判据本体）。
// 返回 true ⇒ 属内部实现细节 ⇒ 出口只给 msgInternalFailure，原文只进日志与错误链。
//
// 对应 java JeeflowFacade.isForeignDetail(type, message, cause, trace)：go 的 error 值本身就带着
// 这四样（动态类型＝type、Error()＝message、Unwrap()＝cause；只有 trace 在 go 缺位，见文件头判据 5）。
// 副作用那一半是 logForeignFailure，两者各自可测（spec §2.12「判据形状」）。
//
// 顺序即优先级，**判据 2 必须在 3/4/5 之前**：facadeFixedError 的 cause 里躺着的正是
// *json.SyntaxError 这类外来类型，先走链就会把「读取流程定义 JSON 失败」这条跨栈逐字契约文案
// 换成固定文案（既有 facade_msg_139_test.go / facade_cause_chain_139_test.go 会当场红）。
func isForeignDetail(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// 判据 1：文案为空/纯空白 ⇒ 内部（对外给空 msg 等于没给原因）
	if strings.TrimSpace(msg) == "" {
		return true
	}
	// 判据 2：引擎自己的「对外文案载体」（issues/139 那枚）⇒ 逐字透出。
	// 只看**顶层动态类型**、不下探 cause —— 139 的全部意义就是"msg 固定、原文只在链上"。
	if _, ok := err.(*facadeFixedError); ok {
		return false
	}
	// 判据 3：裸包装 —— 包装层没写一个字，文案是下层写的
	if u := errors.Unwrap(err); u != nil && u.Error() == msg {
		return true
	}
	// 判据 4：运行时/反射/解析器/IO/驱动类型族（errors.As 自己会走整条链）
	if runtimeInternalType(err) {
		return true
	}
	// 判据 5：链上任一环的动态类型定义在引擎模块之外（驱动/第三方 provider/JDK 等价物）
	if foreignTypeOrigin(err) {
		return true
	}
	// 判据 4 的 go 补充：%v 把类型压平成字符串后，只能按文案签名认（理由见文件头）
	return foreignTextSignature(msg)
}

// runtimeInternalType 由 go 运行时/反射层/标准库解析器/IO 层构造的错误族
// （java JeeflowFacade.jvmInternal 的 go 对应件；其 message 一律是内部信息）。
//
// ⚠️ 引擎拿来当**契约文案载体**的 errors.New/fmt.Errorf **不在**这一族里（与 java 把
// RuntimeException/IAE/ISE 排除在外同一条理由）：本仓 58 处 errors.New 全是这个形状。
func runtimeInternalType(err error) bool {
	var rtErr runtime.Error // recover 出来的 nil 解引用/越界/除零/类型断言
	if errors.As(err, &rtErr) {
		return true
	}
	var reflectErr *reflect.ValueError
	if errors.As(err, &reflectErr) {
		return true
	}
	var jsonSyntax *json.SyntaxError
	if errors.As(err, &jsonSyntax) {
		return true
	}
	var jsonType *json.UnmarshalTypeError
	if errors.As(err, &jsonType) {
		return true
	}
	var jsonInvalid *json.InvalidUnmarshalError
	if errors.As(err, &jsonInvalid) {
		return true
	}
	var jsonMarshal *json.MarshalerError
	if errors.As(err, &jsonMarshal) {
		return true
	}
	var numErr *strconv.NumError // Atoi/ParseInt/ParseFloat 的 ErrSyntax/ErrRange 都包在这里头
	if errors.As(err, &numErr) {
		return true
	}
	var netOp *net.OpError
	if errors.As(err, &netOp) {
		return true
	}
	var netAddr *net.AddrError
	if errors.As(err, &netAddr) {
		return true
	}
	var netDNS *net.DNSError
	if errors.As(err, &netDNS) {
		return true
	}
	var pathErr *fs.PathError // os.PathError 是它的别名
	if errors.As(err, &pathErr) {
		return true
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return true
	}
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return true
	}
	// 哨兵值：这四个本身没有可辨识的动态类型（或类型在 context 包里），按值判
	for _, sentinel := range []error{io.EOF, io.ErrUnexpectedEOF, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// foreignTypeOrigin 沿错误链看**动态类型的包路径**：定义在引擎模块之外 ⇒ 这段文案不是引擎写的。
// java thrownInsideEngine(trace) 的 go 对应件——go 的 error 不带栈，能拿到的只有类型的出身。
func foreignTypeOrigin(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		pkg := errorTypePkgPath(e)
		switch {
		case pkg == "":
			// 无包路径（匿名形状/内建类型），判不了出身，交给其余判据
		case strings.HasPrefix(pkg, engineModulePath):
			// 引擎模块内定义的类型 ⇒ 引擎自己写的
		case plainErrorPkgs[pkg]:
			// errors.New / fmt.Errorf 借来的笔 ⇒ 不能算外来（见 plainErrorPkgs 注释）
		default:
			return true
		}
	}
	return false
}

// errorTypePkgPath 取动态类型的**具名**出身包路径。
//
// ⚠️ 不能直接用 reflect.TypeOf(err).PkgPath()：PkgPath 只对**具名**类型非空，而 error 几乎全是
// 指针形状（*mysql.MySQLError、*json.SyntaxError、*errors.errorString），直接取一律得到空串
// ⇒ 判据 5 恒假、整条形同虚设（本轮实测踩过：mysql 驱动错误因此原样外透）。必须先剥指针。
func errorTypePkgPath(err error) string {
	t := reflect.TypeOf(err)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}
	return t.PkgPath()
}

// foreignTextSignature 纯字符串判据（判据 4 的 go 补充）：文案里有没有运行时/解析器/驱动自己的签名。
// 单独抽出来是为了"文案判据可独立测"——不必先构造出一个被 %v 压平的 error 才能验这一条。
func foreignTextSignature(msg string) bool {
	for _, sig := range foreignTextSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// outwardFailureMsg 门面失败出口的 msg 映射：判别式说过 ⇒ 原文换固定文案＋进日志；否则逐字透出。
// 这是判据（isForeignDetail）与副作用（logForeignFailure）的装配点，本身不做判断。
func outwardFailureMsg(action string, err error) string {
	if !isForeignDetail(err) {
		return err.Error()
	}
	logForeignFailure(action, err, nil)
	return msgInternalFailure
}

// outwardPanicMsg recover 出口的 msg 映射。
// 旧形状是 errorResult(fmt.Sprintf("%v", p))：任何 panic 值都原样进对外 msg（nil 解引用得到的
// "runtime error: invalid memory address or nil pointer dereference" 直接糊到用户脸上）。
// 现在先把 panic 值归一成 error —— 是 error 就**保留链与动态类型**（判据 3/4/5 才有东西可判），
// 不是就按 %v 转文案（再交给判据 4 的签名兜）—— 然后走与 error 出口**同一条**判别式。
func outwardPanicMsg(action string, p interface{}) string {
	err := panicAsError(p)
	if !isForeignDetail(err) {
		return err.Error()
	}
	logForeignFailure(action, err, debug.Stack())
	return msgInternalFailure
}

// panicAsError 把 recover 到的值归一成 error：本身是 error 就原样保留（链与类型都在），
// 否则按 %v 转成文案（string panic 走这一档，例如门面自己那句「未配置 ProcessExtRepository（扩展仓储）」）。
func panicAsError(p interface{}) error {
	if err, ok := p.(error); ok {
		return err
	}
	return fmt.Errorf("%v", p)
}

// logForeignFailure 副作用那一半：原文连同**完整错误链**进日志（java 是 log.log(SEVERE, msg, e) 带 cause）。
// 对外 msg 里一个字都不许有 —— 这条日志是集成方唯一还能看到真因的地方，所以链要逐层展开、不能只打最外层。
// stack 非空（panic 腿）时一并打出去。
func logForeignFailure(action string, err error, stack []byte) {
	if len(stack) > 0 {
		log.Printf("[jeeflow] ERROR action=%s 内部失败，对外只给固定文案「%s」；原文与错误链（不进 msg）：%s\n%s",
			action, msgInternalFailure, errorChain(err), stack)
		return
	}
	log.Printf("[jeeflow] ERROR action=%s 内部失败，对外只给固定文案「%s」；原文与错误链（不进 msg）：%s",
		action, msgInternalFailure, errorChain(err))
}

// errorChain 把错误链逐层展开成一行：`外层类型: 外层文案 <- 内层类型: 内层文案`。
// 只给日志用（对外字段一律不许出现原文，spec §2.12）。
func errorChain(err error) string {
	parts := make([]string, 0, 4)
	for e := err; e != nil; e = errors.Unwrap(e) {
		parts = append(parts, fmt.Sprintf("%T: %v", e, e))
	}
	return strings.Join(parts, " <- ")
}
