// issues/126 案 A · 任务行 expire_time 的**求值器**与**建单写点**（基准＝boot2 内置版）。
// issues/137 A · 批二 §3-4（A 案）· 同把尺子再加一处**实例级**写点（发起腿，见 applyInstanceExpireTime）。
//
// 参考实现并排：
//   - Java 求值器 `FlowUtil.processTime(String, FlowData)`（jeeflow-java jeeflow-core util/FlowUtil.java:64）
//   - Java 写点 `ProcessInstance.applyExpireTime(...)`（commit d9e9397：一个 helper + 四处调用同一实现）
//   - boot2 内置版 `ProcessTaskServiceImpl` :213 普通建单 / :386 回退新建 / :524 会签建单
//
// 本栈修前形状与 java 还不一样：engine 里**一句** ExpireTime 都没有（java 至少有句占位 now()），
// 建单路径从未赋过这一列 ⇒ wf_process_task.expire_time 恒 NULL ⇒ 逾期统计恒 0
// （案文 §1「go 常规不赋」；跨栈判据见门禁 L2-27）。所以 go 这栈是"先移植求值器、再接四处写点"，
// 不是改四行——求值器按 §1.5 的逐字语义落在本栈既有 util 位置（engine/）。
package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

const (
	// PropExpireTime 任务节点上配的到期表达式属性键
	// （对齐 Java TaskParser.EXPIRE_TIME_KEY ← 设计器 JSON 的 properties.expireTime）
	PropExpireTime = "expireTime"
	// expireTimeLayout 绝对时刻档格式，对齐 Java SimpleDateFormat 的 "yyyy-MM-dd HH:mm:ss"
	expireTimeLayout = "2006-01-02 15:04:05"
)

// ProcessTime 解析节点到期表达式，三档顺序**不可变**（逐字对齐 Java FlowUtil.processTime）：
//
//	⓪ 表达式为 nil/空/纯空白 ⇒ nil（这一列留 NULL，不造默认值）
//	① args 里存在**键名等于表达式原串**的项 ⇒ 取该项的值：time.Time / 毫秒时间戳 /
//	   "yyyy-MM-dd HH:mm:ss" 字符串 → 该时刻；字符串解析失败 → **nil**（不是 now，也不落穿）；
//	   值类型不认识（bool / map / slice / nil / 非整数数值…）→ **落穿**到后面两档
//	   （Java/C# 都是落穿，不得改成提前 return nil）
//	② 以 s|m|h|d 结尾且前缀是**非负**整数 ⇒ 当前时间 + N 秒/分/时/天
//	   （**d 走日历加天 AddDate(0,0,n)**，不乘 86400——跨夏令时两者不等价，
//	   Java 用 Calendar.add(DAY_OF_MONTH) 同一量纲。
//	   前缀为负（`-5h`/`-5d`）算**不合法**，与坏前缀一样落穿到 ③ ⇒ 结果 nil（issues/137 D，
//	   owner 2026-10-01 拍"判非负"：放行负偏移＝建单即逾期）；带 '+' 的前缀照旧合法）
//	③ 否则把表达式本身按 "yyyy-MM-dd HH:mm:ss" 解析 → 该时刻；失败 → nil
//
// **任何一档都不允许返回 now()**：本案病灶恰是"非空但错"的占位 now()（建单即逾期、
// expire−create≈0），一旦让 now() 当兜底，L2-27 那格「同一行 expire−create 必须≈表达式偏移」
// 就永远抓不到东西。
//
// 与 Java 的两处语言差异（都不改变判点）：
//   - Java `Integer.parseInt` 遇 "xh" 这类坏前缀抛 NumberFormatException；Go 无异常 ⇒ 落穿到
//     绝对档 ⇒ 结果仍是 nil（两栈都不会因此拿到 now() 或假时刻）。
//   - Java 毫秒档只认 `Long`；Go 里 JSON 数字一律解成 float64 ⇒ 整数值（含 int/int64/整值 float64）
//     同档处理，非整数值（Java 的 Double 档）按"类型不认识"落穿，与 Java 对 Double 的行为一致。
func ProcessTime(expr string, args map[string]interface{}) *time.Time {
	if strings.TrimSpace(expr) == "" {
		return nil
	}
	// ① 变量档（优先于相对档：args 里真有个键叫 "2h" 时取的是变量值，不是 now+2h）
	if v, ok := args[expr]; ok {
		switch val := v.(type) {
		case time.Time:
			return &val
		case string:
			if t, err := time.ParseInLocation(expireTimeLayout, val, time.Local); err == nil {
				return &t
			}
			return nil // 字符串档解析失败即终局（对齐 Java catch → return null），不落穿
		case int64:
			return epochMillis(val)
		case int:
			return epochMillis(int64(val))
		case float64:
			if !math.IsInf(val, 0) && val == math.Trunc(val) {
				return epochMillis(int64(val))
			}
			// 非整数 ⇒ Java 侧 Double 不匹配任何 instanceof 档 ⇒ 落到下面的相对/绝对档
		}
	}
	// ② 相对档
	if t, ok := relativeTime(expr, time.Now()); ok {
		return &t
	}
	// ③ 绝对档
	if t, err := time.ParseInLocation(expireTimeLayout, expr, time.Local); err == nil {
		return &t
	}
	return nil
}

// epochMillis 毫秒时间戳 → 本地时区时刻（对齐 Java toLocalDateTime(new Date(l))）
func epochMillis(ms int64) *time.Time {
	t := time.UnixMilli(ms)
	return &t
}

// relativeTime 相对档 Ns/Nm/Nh/Nd；前缀非整数**或前缀是负数**或后缀不识别 ⇒ ok=false（交回调用方走绝对档）
func relativeTime(expr string, base time.Time) (time.Time, bool) {
	if len(expr) < 2 {
		return time.Time{}, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(expr[:len(expr)-1]))
	if err != nil {
		return time.Time{}, false
	}
	// issues/137 D（owner 2026-10-01 拍"判非负"）：**负数前缀同样算不合法**，按"解析不出来"处理
	// ——ok=false 交回调用方落穿绝对档，仍解析不出即 nil（NULL）。两条理由（对齐 java
	// FlowUtil.parseIntOrNull）：
	//   1. **任何一档都不许退回当前时间**：放行 `-5h` 算出的是一个**过去**的时刻 ⇒ 新建的行当场就
	//      逾期，比"没配到期时间"更难发现，也正是 issues/126 占位 now() 病灶的同一形状。
	//   2. **只裁负、不裁加号**：判负发生在 Atoi **之后**，不是在词法上剥掉符号位。各栈整数解析
	//      （python `[+-]?`、node `[-+]?\d+`、php `[+-]?\d{1,18}`、go 的 Atoi）都收 '+'，
	//      把 '+' 一并裁掉等于新造一处跨栈分叉——`+2h` 仍是合法的 now+7200s。
	// 判点位置：四档（s/m/h/d）的单位分派在下面的 switch，前缀解析**只有这一处共用**（`d` 档走
	// AddDate 用的也是同一个 n，另加天数分支）⇒ 这一判同时拦住四档，没有漏网的那一档。
	if n < 0 {
		return time.Time{}, false
	}
	// 按字节取末位是安全的：s/m/h/d 都是 ASCII，UTF-8 续字节一律 >=0x80，不会误匹配多字节字符
	switch expr[len(expr)-1] {
	case 's':
		return base.Add(time.Duration(n) * time.Second), true
	case 'm':
		return base.Add(time.Duration(n) * time.Minute), true
	case 'h':
		return base.Add(time.Duration(n) * time.Hour), true
	case 'd':
		return base.AddDate(0, 0, n), true
	}
	return time.Time{}, false
}

// applyExpireTime 四处建单写点共用的赋值口（对齐 Java ProcessInstance.applyExpireTime）：
// 节点没配表达式 ⇒ 直接 return，这一列保持 NULL（owner 2026-09-28：不造默认值、不写空串、不写 0）；
// 配了但算不出 ⇒ ProcessTime 给 nil，同样保持 NULL。
//
// args 变量源两档，**不许搞混**（搞混会让"表达式是个变量名"这一档跨栈给出不同答案）：
//   - 建单三处（普通 / 串行会签首位 / 并行会签全员）＝**实例变量** inst.Variables
//     （＝ Java this.variables ＝ boot2 的 execution.getArgs()）
//   - 回退/跳转新建＝**随行拷贝那份变量**（＝ boot2 的 hisVariable）
func applyExpireTime(task *model.ProcessTask, expr string, args map[string]interface{}) {
	if task == nil || strings.TrimSpace(expr) == "" {
		return
	}
	task.ExpireTime = ProcessTime(expr, args)
}

// applyInstanceExpireTime **实例级** expire_time 的写点（issues/137 A · 批二 §3-4，A 案：
// 列值＝定义级顶层表达式的**求值结果**＝一个时刻，不是表达式原串）。
//
// 逐字对齐基准 java `JeeflowEngineImpl.java:93-96`（＝boot2 内置版
// `ProcessInstanceServiceImpl.java:157-160` 同形状）：
//
//	String expireTime = model.getExpireTime();                       // 流程 JSON 根上的 expireTime 键
//	if (StringUtils.isNotEmpty(expireTime))                          // 没配 ⇒ 一个字都不写
//	    instance.setExpireTime(FlowUtil.processTime(expireTime, args));
//
// 三条落点语义（判据 3／4）：
//   - **没配**（JSON 缺键 / 键值为 null / 空串 ⇒ expr == ""）⇒ 直接 return，这一列保持 NULL：
//     不赋 now()、不赋空串、不赋 0。纯空白串进不了 isNotEmpty 那道门以外的一切——它落到
//     ProcessTime 的 ⓪ 档（TrimSpace 后为空）⇒ nil ⇒ 结局同样是 NULL。
//   - **配了但算不出**（误配 `not-a-time` / 小数前缀 / 负数档 `-5h`，issues/137 C+D）⇒ ProcessTime
//     给 nil ⇒ NULL，沿用任务级既有的"落穿即没有到期时间"语义，**不许兜底 now()**（issues/126 红线）。
//   - 写进去的必须是**求值结果**（判据 1）：本栈这一列是 `*time.Time`，落库经
//     `repository/jdbc/jdbc.go:405` 的 INSERT 直接绑进 `DATETIME(3)`；把原串搬过去在
//     160 那台 `@@sql_mode` 含 STRICT_TRANS_TABLES 的 MySQL 上是服务端硬错（兄弟栈 rust/php
//     本轮实测拿到的是 1292/22007 `Incorrect datetime value: '2h' for column 'expire_time'`），
//     连库都进不去 ⇒ 求值这一步没有"先存原串回头再解析"的余地。
//
// args 语义（判据 2）：调用方传的是发起腿里那份**发起参数** `vars`
// （`mergeVars(args, nil)` 之后又被 `addUserInfo` / `addAutoGenTitle` 就地注入 u_* 与 autoGenTitle，
// engine_impl.go:64-66），对齐 java 那边同被这两个 add* 就地改写过的同一个 `args` 对象。
// 于是变量档能命中 `u_userId` 这类**引擎注入**的键，而不只是调用方原始入参——
// 传错成原始 args 会让这一档跨栈给出不同答案。
//
// 求值器与任务级四处写点**共用同一个** ProcessTime（判据 5），档位顺序与语义一字未改；
// owner 2026-09-28 口径「实例的处理方式和任务的差不多吧，算法一致」（issues/137 §状态行 A）。
func applyInstanceExpireTime(inst *model.ProcessInstance, expr string, args map[string]interface{}) {
	if inst == nil || expr == "" {
		return
	}
	inst.ExpireTime = ProcessTime(expr, args)
}

// expireExprOf 读任务节点上配的到期表达式；未配 / 配成 null ⇒ ""（走"保持 NULL"分支）。
//
// 刻意不走既有的 stringFromProps：那条对 JSON null 会 fmt.Sprint 出 "<nil>" 这种非空垃圾串，
// 等于把"没配"伪造成"配了个解析不出的表达式"（同为 NULL 结局，但表达式档被白走一遍）。
// 非字符串值仍按 fmt.Sprint 处理，对齐 Java FlowData.getStr 的 v.toString()。
func expireExprOf(node *model.FlowNode) string {
	if node == nil || node.Properties == nil {
		return ""
	}
	switch v := node.Properties[PropExpireTime].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}
