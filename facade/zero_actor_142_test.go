// issues/142 A 批（go 腿）· 门面侧的用户可感知形状。
//
// 派工里点名的风险：把"任务类零参与者不建行"改成"建一条零参与者 DOING 行"之后，
// 门面 `startAndExecute` 那圈**自动办结**逻辑会不会被推歪（死循环 / 实例状态机走偏）。
// 判据落在门面出口上（todoList/doneList/返回 code），因为这两条正是用户在 UI 上看到的形状：
//
//	§6.1 硬结论 2：记录类节点不产生待办 ⇒ 待办数分母要把它摘出去；
//	§6.2 第 3 条：任务类零参与者必须建 DOING 行（不能"停在 state=10 却零可办行"）。
//
// 自动办结那一圈的结构性护栏（facade/facade.go:226-240）：它遍历的是**发起那一次**的 doing 快照，
// 循环体里新建的行不在这个 slice 上 ⇒ 零参与者行不会自增出新行，也就无从死循环；
// 下面两格把这个论证实测一遍（真死循环的话 go test 会 hang 到超时，也是可读的红）。
package facade_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// z142AddDefine 直接放一条定义（不走 deploy，省掉版本管理的无关分支）。
func z142AddDefine(t *testing.T, repo *memory.Repository, name, content string) int64 {
	t.Helper()
	def := &model.ProcessDefine{Name: name, DisplayName: "142 门面形状", Type: "approval",
		State: 1, Version: 1, Content: []byte(content)}
	repo.AddDefine(def)
	return def.ID
}

// z142Rows 读分页出口的行数（todoList/doneList 同形状：data.rows）。
func z142Rows(t *testing.T, resp map[string]interface{}, what string) []interface{} {
	t.Helper()
	if code, _ := resp["code"].(int); code != 0 {
		t.Fatalf("%s 应成功，实得 %v", what, resp)
	}
	data, _ := resp["data"].(map[string]interface{})
	rows, _ := data["rows"].([]interface{})
	return rows
}

// TestIssue142ZeroActorApplyAutoExecutedByFacade apply 节点没有任何 assignee（零参与者）时，
// 门面发起（startAndExecute）仍能走通：零参与者行建出来 → 被自动办结（门面的既有契约：
// 把 operator 加派进去再办结）→ 令牌推进到 task1，实例停在 state=10 等 leader。
//
// 改前必红：旧形状那一行根本不建 ⇒ 自动办结那圈拿到空 doing ⇒ 实例停在 state=10 且
// **零待办行**（§6.1 点名的死锁黑洞），本格的"剩 task1 一条待办"红成 0 条。
func TestIssue142ZeroActorApplyAutoExecutedByFacade(t *testing.T) {
	f, repo, _ := setupFacade()
	const content = `{"name":"z142-facade-zero","displayName":"零参与者发起自动办结","type":"approval","nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"apply","type":"snaker:task","properties":{"performType":0},"text":{"value":"发起申请"}},
	  {"id":"task1","type":"snaker:task","properties":{"assignee":"leader","performType":0},"text":{"value":"审批"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},
	  {"id":"e2","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},
	  {"id":"e3","sourceNodeId":"task1","targetNodeId":"end","properties":{}}]}`
	defID := z142AddDefine(t, repo, "z142-facade-zero", content)

	r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("零参与者发起应成功（不报错、不死循环），实得 %v", r)
	}
	rows := z142Rows(t, f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "leader", "pageSize": 100}), "todoList(leader)")
	if len(rows) != 1 {
		t.Fatalf("task1 应有 1 条待办（令牌被自动办结推到 task1），实得 %d", len(rows))
	}
	// 发起人的待办是空的（apply 那行被自动办结；零参与者行没有留在黑洞里）
	if rows := z142Rows(t, f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "zhangsan", "pageSize": 100}), "todoList(zhangsan)"); len(rows) != 0 {
		t.Fatalf("zhangsan 不该有待办，实得 %d", len(rows))
	}
	// 实例仍在进行中（task1 未办）
	instID := z142InstID(t, r, "processInstanceId")
	inst, err := repo.FindInstanceByID(context.Background(), instID)
	if err != nil || inst == nil {
		t.Fatalf("读回实例 %d: %v", instID, err)
	}
	if inst.State != model.InstanceStateDoing {
		t.Fatalf("实例应停在 state=10 等 leader，实得 %d", inst.State)
	}
}

// TestIssue142CustomNodeTodoAndDoneCountsByFacade 记录类节点在两个列表里的形状：
//   - todoList **不因为它多一条**（§6.1 硬结论 2）；
//   - doneList 查得到那条留痕（task_state=20，operator 列＝当前操作人）。
//
// 改前必红：旧形状（custom 与 task 同路 + 空参与者 return nil）下 custom 一行都不建，
// doneList 是 0 条（丢留痕＝禁止形状③），实例也停在 state=10。
func TestIssue142CustomNodeTodoAndDoneCountsByFacade(t *testing.T) {
	f, repo, _ := setupFacade()
	const content = `{"name":"z142-facade-custom","displayName":"记录类节点两列表","type":"approval","nodes":[
	  {"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
	  {"id":"custom1","type":"snaker:custom","properties":{"clazz":"com.mldong.demo.NotifyHandler"},"text":{"value":"通知外部系统"}},
	  {"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}],
	 "edges":[
	  {"id":"e1","sourceNodeId":"start","targetNodeId":"custom1","properties":{}},
	  {"id":"e2","sourceNodeId":"custom1","targetNodeId":"end","properties":{}}]}`
	defID := z142AddDefine(t, repo, "z142-facade-custom", content)

	r := f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defID, "operator": "zhangsan"})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("未注册的 clazz 不该打断发起，实得 %v", r)
	}
	if rows := z142Rows(t, f.Flow("processTask/todoList", map[string]interface{}{
		"operator": "zhangsan", "pageSize": 100}), "todoList"); len(rows) != 0 {
		t.Fatalf("记录类节点不该产生待办，实得 %d 条", len(rows))
	}
	done := z142Rows(t, f.Flow("processTask/doneList", map[string]interface{}{
		"operator": "zhangsan", "pageSize": 100}), "doneList")
	if len(done) != 1 {
		t.Fatalf("doneList 应查到那条留痕，实得 %d 条", len(done))
	}
	row := done[0].(map[string]interface{})
	if row["taskName"] != "custom1" {
		t.Fatalf("那条留痕应是 custom1，实得 %v", row["taskName"])
	}
	// 门面出口的结构体字段是 Go 原生值（stringifyIDs 的 struct→map 分支不做 JSON 往返），
	// 所以这一格是 model.TaskState 而不是 float64
	if got := z142AsInt(t, row["taskState"]); got != int(model.TaskStateDone) {
		t.Fatalf("留痕行 taskState 应为 20，实得 %v", row["taskState"])
	}
	// 实例继续到 end
	instID := z142InstID(t, r, "processInstanceId")
	inst, err := repo.FindInstanceByID(context.Background(), instID)
	if err != nil || inst == nil {
		t.Fatalf("读回实例 %d: %v", instID, err)
	}
	if inst.State != model.InstanceStateDone {
		t.Fatalf("实例应办结（state=20），实得 %d", inst.State)
	}
}

// z142InstID 门面把 id 投成字符串（雪花 id 防 JS 精度），这里读回来转 int64。
func z142InstID(t *testing.T, resp map[string]interface{}, key string) int64 {
	t.Helper()
	data, _ := resp["data"].(map[string]interface{})
	s, ok := data[key].(string)
	if !ok {
		t.Fatalf("%s 应是字符串化后的 id（isIDKey 后缀 Id），实得 %#v (%v)", key, data[key], resp)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q 解析失败: %v", key, s, err)
	}
	return n
}

// z142AsInt 把门面出口的原生整型（含命名整型如 model.TaskState）收成 int。
func z142AsInt(t *testing.T, v interface{}) int {
	t.Helper()
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
			return int(rv.Int())
		}
		t.Fatalf("不认识的整型形态 %#v", v)
	}
	return 0
}
