// 动态协办（issues/253）的门面腿——逐字照 java 参考实现 JeeflowFacade 的 taskAidant
// ＋ parseWaitCompletion ＋ detail 的 ext.aidant 投影（issues/253 实施轮工作树现读）。
//
// 分层与 java 同构：判据/建单/账本/闸门全在引擎（engine/aidant.go），门面只做参数归一
// 与信封包装（与 recall 同分工）。判据全部排在任何状态变更之前——"报错即零写入"与
// 后加签/征询/撤回写入口同一规矩。
package facade

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mldong/jeeflow-go/engine"
)

// taskAidant 动态协办（issues/253，spec 06 §processTask/aidant）：主办把"这活"交给别人干——
// 按人派生 task_type=1 协办子行，主办行原样保留 DOING，节点推进仍由主办行办结触发。
//
// operator 硬必填（不接受缺省，与 transfer/withdraw/recall 同口径）；actorIds 逗号串与数组
// 两形共用 §2.11 那枚归一单点（toStringSlice2 → spi.NormalizeActors），空 ⇒ 逐字「actorIds 必填」。
// waitCompletion 选填 bool，缺省 false＝主办不等协办办结即可推进。
//
// 成功 data → null（spec 06）；失败 msg 见跨栈统一文案表协办六行。
// 权限码复用 wf:processTask:execute（OR 规则，照 246/247 B 案惯例）。
// 位序照 java taskAidant：operator 空判在前、主键缺失判在后、actorIds 空判第三。
func (f *Facade) taskAidant(ctx context.Context, args map[string]interface{}) error {
	operator := strings.TrimSpace(toStr(args["operator"], ""))
	if operator == "" {
		return errors.New("operator 必填")
	}
	taskID, _ := toInt64(args["processTaskId"])
	if taskID == 0 {
		return errors.New("processTaskId 缺失")
	}
	actorIds := toStringSlice2(args["actorIds"])
	if len(actorIds) == 0 {
		return errors.New(engine.MsgAidantNoActors)
	}
	reason := toStr(args["reason"], "")
	waitCompletion := parseWaitCompletion(args["waitCompletion"])
	_, err := f.engine.AidantTask(ctx, taskID, actorIds, reason, waitCompletion, operator)
	return err
}

// parseWaitCompletion waitCompletion 入参解析：缺省 false（spec 06「选填 bool，缺省 false」）。
// 布尔与 "true"/"false" 字符串两形都认（前端 JSON 与表单串两种来源）；其余值按缺省 false
// 处理——契约只定义了 bool 档，不为非法值新造文案（跨栈统一文案表协办六行之外不加第七行）。
func parseWaitCompletion(value interface{}) bool {
	if value == nil {
		return false
	}
	if b, ok := value.(bool); ok {
		return b
	}
	return strings.EqualFold(strings.TrimSpace(fmt.Sprintf("%v", value)), "true")
}