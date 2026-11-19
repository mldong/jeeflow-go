// 任务时限动作（spec 04 · 任务时限动作）：引擎写侧在任务建单/完成/废弃/实例终态时
// 经 spi.TimeActionSink 登记或取消"到点动作"，由集成层的执行队列消费者到点执行。
//
// 分工（工作流时限方案 §1）：
//   - 引擎负责字段求值、动作行建/取消联动、可注入时钟；不启动后台线程、不依赖 sys 表/Web 框架；
//   - 集成层实现 sink（sys_task_execution_queue）、到点消费者与三类处理器；
//   - 未注入 sink 的引擎（存量使用方、纯内存测试）全部时限动作静默跳过，既有行为零变化。
package engine

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/spi"
)

const (
	// PropReminderTime 首次提醒时刻（规范 02 §4.5）
	PropReminderTime = "reminderTime"
	// PropReminderRepeat 首次提醒之后的重复间隔（正整数 s/m/h/d；d 为日历日）
	PropReminderRepeat = "reminderRepeat"
	// PropAutoExecute 到期自动同意开关：仅精确 "Y" 开启（N/未填/未知值一律关闭）
	PropAutoExecute = "autoExecute"
	// PropCallback 任务完成处理器注册名（不是 URL）
	PropCallback = "callback"
)

// WithClock 注入引擎时钟（动作到点计算、到期表达式相对档锚点、任务创建时刻）。
// 不注入 ⇒ time.Now；跨语言边界测试必须注入固定时钟，禁止混用进程钟/浏览器钟/数据库钟。
func WithClock(now func() time.Time) Option {
	return func(e *EngineImpl) {
		if now != nil {
			e.clock = now
		}
	}
}

// WithTimeActionSink 注入时限动作凭证仓储（集成层按框架执行队列实现，spec 04 §动作行契约）。
func WithTimeActionSink(sink spi.TimeActionSink) Option {
	return func(e *EngineImpl) {
		e.timeActionSink = sink
	}
}

// now 引擎统一时钟。
func (e *EngineImpl) now() time.Time {
	if e.clock != nil {
		return e.clock()
	}
	return time.Now()
}

// propRawString 读节点属性原串（不做 trim；与 expireTime 的"表达式原串即键名/时间串"同尺）。
func propRawString(node *model.FlowNode, key string) string {
	if node == nil || node.Properties == nil {
		return ""
	}
	switch v := node.Properties[key].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// AutoExecuteEnabled autoExecute 是否开启：**仅精确 "Y"**（规范 02 §4.5；N/未填/未知值不当开启）。
func AutoExecuteEnabled(node *model.FlowNode) bool {
	return propRawString(node, PropAutoExecute) == "Y"
}

// CallbackName 完成处理器注册名（空＝不调用）。名称两端空白裁掉：注册名是标识符，
// 手填多余空白属误配噪声，裁掉不会把两个不同处理器折叠成一个。
func CallbackName(node *model.FlowNode) string {
	return strings.TrimSpace(propRawString(node, PropCallback))
}

// ReminderRepeat 按正整数 + s/m/h/d 解析重复间隔；返回 ok=false 表示空/非法（调用方记诊断）。
// d 走日历加天（与到期语义一致），s/m/h 为固定时间量。不接受 cron、0、负数、小数、变量名。
type ReminderRepeat struct {
	N    int
	Unit byte
}

// ParseReminderRepeat 解析 reminderRepeat（导出给集成层处理器复用同一把尺子）。
func ParseReminderRepeat(raw string) (ReminderRepeat, bool) {
	s := strings.TrimSpace(raw)
	if len(s) < 2 {
		return ReminderRepeat{}, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(s[:len(s)-1]))
	if err != nil || n <= 0 {
		return ReminderRepeat{}, false
	}
	switch s[len(s)-1] {
	case 's', 'm', 'h', 'd':
		return ReminderRepeat{N: n, Unit: s[len(s)-1]}, true
	}
	return ReminderRepeat{}, false
}

// Next 从 from 按间隔推进一格（锚定调用方传入的基准，不自行取 now，避免漂移）。
func (r ReminderRepeat) Next(from time.Time) time.Time {
	switch r.Unit {
	case 's':
		return from.Add(time.Duration(r.N) * time.Second)
	case 'm':
		return from.Add(time.Duration(r.N) * time.Minute)
	case 'h':
		return from.Add(time.Duration(r.N) * time.Hour)
	case 'd':
		return from.AddDate(0, 0, r.N)
	}
	return from
}

// parseReminderTime 首次提醒时刻。档位顺序（规范 02 §4.5）：
//
//	⓪ 空/纯空白 ⇒ nil（不提醒，不回落 expireTime）
//	① 实例变量同名键（日期对象/毫秒时间戳/"yyyy-MM-dd HH:mm:ss" 字符串；字符串解析失败⇒nil，
//	   不落穿；其它类型落穿）
//	② 相对量 Ns/Nm/Nh/Nd，非负整数，锚点＝**任务创建时刻**（不是 now，不随发送漂移）
//	③ 严格 HH:mm：任务创建日当天该时刻，已早于创建时刻取次日
//	④ 完整日期字符串（应用调度时区）
//
// 任何一档都不得回落成"当前时刻"或 expireTime。
func parseReminderTime(expr string, args map[string]interface{}, createTime time.Time) *time.Time {
	if strings.TrimSpace(expr) == "" {
		return nil
	}
	if v, ok := args[expr]; ok {
		switch val := v.(type) {
		case time.Time:
			t := val
			return &t
		case string:
			if t, err := time.ParseInLocation(expireTimeLayout, val, time.Local); err == nil {
				return &t
			}
			return nil // 变量档字符串解析失败即终局（对齐 expireTime 的变量档）
		case int64:
			return epochMillis(val)
		case int:
			return epochMillis(int64(val))
		case float64:
			if val == float64(int64(val)) {
				return epochMillis(int64(val))
			}
			// 非整数 ⇒ 落穿（同 ProcessTime）
		}
	}
	if t, ok := relativeTime(expr, createTime); ok {
		return &t
	}
	if t, ok := parseDailyHHMM(expr, createTime); ok {
		return &t
	}
	if t, err := time.ParseInLocation(expireTimeLayout, expr, time.Local); err == nil {
		return &t
	}
	return nil
}

// parseDailyHHMM 严格 HH:mm（两位时、两位分、0≤时≤23、0≤分≤59）。
func parseDailyHHMM(expr string, base time.Time) (time.Time, bool) {
	if len(expr) != 5 || expr[2] != ':' {
		return time.Time{}, false
	}
	for i := 0; i < len(expr); i++ {
		if i == 2 {
			continue
		}
		if expr[i] < '0' || expr[i] > '9' {
			return time.Time{}, false
		}
	}
	h, _ := strconv.Atoi(expr[:2])
	m, _ := strconv.Atoi(expr[3:])
	if h > 23 || m > 59 {
		return time.Time{}, false
	}
	t := time.Date(base.Year(), base.Month(), base.Day(), h, m, 0, 0, base.Location())
	if t.Before(base) {
		t = t.AddDate(0, 0, 1)
	}
	return t, true
}

// scheduleTimeActions 建单侧动作行（与任务落库同事务）：
// 配了 autoExecute=Y 且有有效到期时间 ⇒ AUTO_COMPLETE；配了有效 reminderTime ⇒ 首条 REMIND。
// allow=false 用于按规范不继承动作的行（协办、动态征询成员）。
func (e *EngineImpl) scheduleTimeActions(ctx context.Context, nt *model.ProcessTask,
	node *model.FlowNode, inst *model.ProcessInstance, allow bool) error {
	if e.timeActionSink == nil || !allow || nt == nil || node == nil || inst == nil {
		return nil
	}
	if nt.TaskState != model.TaskStateDoing || nt.TaskType == int(model.TaskTypeSecondary) {
		return nil
	}
	if AutoExecuteEnabled(node) {
		if nt.ExpireTime == nil {
			log.Printf("[jeeflow] 时限配置诊断 task=%d node=%s autoExecute=Y 但任务无有效到期时间，跳过自动办理",
				nt.ID, node.ID)
		} else {
			if err := e.timeActionSink.InsertTimeAction(ctx, &spi.TimeAction{
				Type:           spi.TimeActionAutoComplete,
				DueAt:          nt.ExpireTime.UTC(),
				InstanceID:     nt.ProcessInstanceID,
				TaskID:         nt.ID,
				NodeKey:        node.ID,
				OccurrenceNo:   0,
				IdempotencyKey: fmt.Sprintf("wf:auto:%d:0", nt.ID),
			}); err != nil {
				return err
			}
		}
	}
	if raw := propRawString(node, PropReminderTime); strings.TrimSpace(raw) != "" {
		if due := parseReminderTime(raw, inst.Variables, nt.CreateTime); due != nil {
			if err := e.timeActionSink.InsertTimeAction(ctx, &spi.TimeAction{
				Type:           spi.TimeActionRemind,
				DueAt:          due.UTC(),
				InstanceID:     nt.ProcessInstanceID,
				TaskID:         nt.ID,
				NodeKey:        node.ID,
				OccurrenceNo:   1,
				IdempotencyKey: fmt.Sprintf("wf:remind:%d:1", nt.ID),
			}); err != nil {
				return err
			}
		} else {
			log.Printf("[jeeflow] 时限配置诊断 task=%d node=%s reminderTime=%q 解析不出，跳过提醒",
				nt.ID, node.ID, raw)
		}
	}
	return nil
}

// insertCallbackAction 任务完成（state 落 20）同事务插入 CALLBACK 动作行（dueAt=now）。
// 调用前应先取消该任务的 pending 提醒/自动行，避免刚插入的回调行被一并删掉。
func (e *EngineImpl) insertCallbackAction(ctx context.Context, task *model.ProcessTask, node *model.FlowNode) error {
	if e.timeActionSink == nil || task == nil || node == nil {
		return nil
	}
	if CallbackName(node) == "" {
		return nil
	}
	return e.timeActionSink.InsertTimeAction(ctx, &spi.TimeAction{
		Type:           spi.TimeActionCallback,
		DueAt:          e.now().UTC(),
		InstanceID:     task.ProcessInstanceID,
		TaskID:         task.ID,
		NodeKey:        node.ID,
		OccurrenceNo:   0,
		IdempotencyKey: fmt.Sprintf("wf:callback:%d:0", task.ID),
	})
}

// cancelTaskTimeActions 删除该任务 pending 的动作行（漏删由处理器执行前复核兜底）。
func (e *EngineImpl) cancelTaskTimeActions(ctx context.Context, taskID int64) error {
	if e.timeActionSink == nil {
		return nil
	}
	return e.timeActionSink.CancelTaskTimeActions(ctx, taskID)
}

// CancelInstanceTimeActions 删除该实例 pending 的动作行（实例终态；门面撤回腿共用）。
func (e *EngineImpl) CancelInstanceTimeActions(ctx context.Context, instanceID int64) error {
	if e.timeActionSink == nil {
		return nil
	}
	return e.timeActionSink.CancelInstanceTimeActions(ctx, instanceID)
}

// lockInstanceRow 事务级实例排他保护（spec 04 · 并发保护）：
// 仓储实现 spi.InstanceLocker 时加实例行锁；未实现则退化为既有进程内互斥（不宣称跨进程）。
func (e *EngineImpl) lockInstanceRow(ctx context.Context, instanceID int64) error {
	if locker, ok := e.repo.(spi.InstanceLocker); ok {
		return locker.LockInstance(ctx, instanceID)
	}
	return nil
}

// LockInstance 供门面自组写路径（如撤回）在事务内取得同一把实例保护。
func (e *EngineImpl) LockInstance(ctx context.Context, instanceID int64) error {
	return e.lockInstanceRow(ctx, instanceID)
}
