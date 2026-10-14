package facade_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/facade"
	"github.com/mldong/jeeflow-go/memory"
)

// issues/201（audit8 队列 R79·owner 拍 C＝只统一「求值器自身抛异常必须落可诊断日志」，
// 遍历宽度不钉）的 go 腿判据。
//
// 修前形状＝`facade.go` 的 `evalDecisionExpr` 末尾 `if err != nil { return false, nil }`：
// error 就地丢弃 ⇒「表达式求值失败」与「这条分支求值为假」在对外读数上完全同形、且零日志。
// 本栈宽度维持折 false（该边不收、其余边继续遍历），本案只咬"失败必留痕"这一条，
// 所以三枚判据的分工是：①失败⇒有日志且归因齐；③合法判假⇒**不许**有日志（两档必须分得开）；
// ②正常⇒零日志且补全腿照旧产出（阳性对照，防夹具没走到模型腿就自证绿）。
type i201ExprEval struct {
	fail     bool // 求值失败档（抛错）
	allFalse bool // 合法判假档（返回 false，不抛）
}

func (e *i201ExprEval) Eval(expr string, vars map[string]interface{}) (interface{}, error) {
	if e.fail {
		return nil, fmt.Errorf("i201 夹具注入的求值失败（expr=%s）", expr)
	}
	if e.allFalse {
		return false, nil
	}
	if v, ok := vars["amount"]; ok {
		amt := toFloat(v)
		if expr == "amount > 1000" {
			return amt > 1000, nil
		}
		if expr == "amount <= 1000" {
			return amt <= 1000, nil
		}
	}
	return false, nil
}

// 夹具＝03-decision-expr.json：start → apply → task1 → decision1 →(e3 amount>1000 → task2 / e4 → task3) → end。
// 建单与办公用「正常档」求值器（amount=5000 ⇒ 走 e3、停在 task2 活跃），求完才把求值器切档，
// 这样引擎运行时路由不受影响，本用例只测门面 highLight 的补全腿。
func i201Setup(t *testing.T, ev *i201ExprEval) (*facade.Facade, int64, int64) {
	t.Helper()
	repo := memory.New()
	eng := engine.New(repo, &testUserProv{}, &testIDGen{}, ev)
	f := facade.New(eng, repo, memory.NewExt())

	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "03-decision-expr.json"))})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r)
	}
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan", "amount": 5000,
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])

	doing, _ := repo.FindDoingTasks(context.Background(), instanceID, nil)
	for _, tk := range doing {
		if tk.TaskName == "task1" {
			_ = repo.AddTaskActor(context.Background(), tk.ID, []string{"leader"})
			r = f.Flow("processTask/execute", map[string]interface{}{
				"processTaskId": tk.ID, "operator": "leader", "submitType": 1,
			})
			if code, _ := r["code"].(int); code != 0 {
				t.Fatalf("execute task1 failed: %v", r)
			}
		}
	}
	// 夹具形状自证：停在 task2 活跃，否则递归到不了 decision1 的两条出边（②③档恒真）
	doing, _ = repo.FindDoingTasks(context.Background(), instanceID, nil)
	if len(doing) != 1 || doing[0].TaskName != "task2" {
		names := []string{}
		for _, tk := range doing {
			names = append(names, tk.TaskName)
		}
		t.Fatalf("夹具应停在 task2 活跃，实际: %v", names)
	}
	return f, instanceID, defineID
}

func i201RunHighLight(t *testing.T, f *facade.Facade, instanceID int64) (map[string]interface{}, string) {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	r := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("拍 C 口径＝降级面保留，求值失败只许留痕不许变红，实得 %v", r)
	}
	return r["data"].(map[string]interface{}), buf.String()
}

// ① 求值器抛错 ⇒ 落一条可归因 WARN（实例号/定义号/节点/表达式/原因），且对外仍 code=0、
// 本栈宽度维持折 false（e3/e4 都不收＝行为与修前一致）
func TestIssue201EvalFailureLogsWarning(t *testing.T) {
	ev := &i201ExprEval{}
	f, instanceID, defineID := i201Setup(t, ev)
	ev.fail = true

	hl, out := i201RunHighLight(t, f, instanceID)
	if !strings.Contains(out, "决策边表达式求值失败") {
		t.Fatalf("求值失败必须落一条可诊断日志（静默通道就是本案要收的那条），实得 %q", out)
	}
	for _, piece := range []string{
		fmt.Sprintf("instanceId=%d", instanceID),
		fmt.Sprintf("defineId=%d", defineID),
		"nodeId=decision1", "expr=", "err=",
	} {
		if !strings.Contains(out, piece) {
			t.Fatalf("日志缺归因件 %q（缺一件等于没落），实得 %q", piece, out)
		}
	}
	if got := toStrings(hl["historyEdgeNames"].([]interface{})); containsStr2(got, "e3") || containsStr2(got, "e4") {
		t.Fatalf("本栈宽度＝求值失败按 false 处理，e3/e4 都不该收（拍 C 不钉宽度，但行为变了就是越界），实得 %v", got)
	}
	if act := toStrings(hl["activeNodeNames"].([]interface{})); !containsStr2(act, "task2") {
		t.Fatalf("失败只许影响模型补全腿，活跃任务行仍须是 task2，实得 %v", act)
	}
}

// ② 求值器正常 ⇒ 零降级日志，且补全腿真产出 e3（阳性对照：证明这枚夹具走的是模型腿，
// 否则①③两档都能靠"根本没走到"假绿）
func TestIssue201NormalEvalLogsNothing(t *testing.T) {
	ev := &i201ExprEval{}
	f, instanceID, _ := i201Setup(t, ev)

	hl, out := i201RunHighLight(t, f, instanceID)
	if strings.Contains(out, "决策边表达式求值失败") {
		t.Fatalf("正常路径不该出现求值失败日志：%s", out)
	}
	if got := toStrings(hl["historyEdgeNames"].([]interface{})); !containsStr2(got, "e3") {
		t.Fatalf("夹具未走到决策边求值（本用例自证失效），期望收 e3，实得 %v", got)
	}
}

// ③ 求值器合法判假（返回 false、不抛）⇒ 同样不收 e3/e4，但**不许**落求值失败日志：
// "求值为假"与"求值失败"必须分得开，否则这条 WARN 会变成每次高亮都刷屏
func TestIssue201LegitFalseDoesNotLog(t *testing.T) {
	ev := &i201ExprEval{}
	f, instanceID, _ := i201Setup(t, ev)
	ev.allFalse = true

	hl, out := i201RunHighLight(t, f, instanceID)
	if strings.Contains(out, "决策边表达式求值失败") {
		t.Fatalf("合法判假不是失败，不该落日志（本案只统一\"失败必留痕\"）：%s", out)
	}
	if got := toStrings(hl["historyEdgeNames"].([]interface{})); containsStr2(got, "e3") {
		t.Fatalf("判假档 e3 不该收（夹具自证），实得 %v", got)
	}
}
