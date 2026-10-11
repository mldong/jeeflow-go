package facade_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mldong/jeeflow-go/facade"
)

// issues/162 的 go 腿判据（owner 10-11 拍口径＝**保留降级面＋落一条可诊断日志**，
// 形状照 python `facade.py` 的 `_log.warning`／php 的 `error_log`；156／160 两例已同形收口）。
// 旧形状 `if json.Unmarshal(def.Content, &flow) == nil { … }` 把降级判得一声不响——
// 这条静默通道先后藏住过 156（php）与 160（python）两个真分叉。
//
// broken=true 时把已部署定义的 content 换成坏 JSON（表意"定义形状坏"，不是"定义不存在"）。
func i162Setup(t *testing.T, broken bool) (*facade.Facade, int64, int64) {
	t.Helper()
	f, repo, _ := setupFacade()
	r := f.Flow("processDefine/deploy", map[string]interface{}{"content": string(flowContent(t, "01-simple.json"))})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("deploy failed: %v", r)
	}
	defineID := mustI64(r["data"].(map[string]interface{})["processDefineId"])

	r = f.Flow("processInstance/startAndExecute", map[string]interface{}{
		"processDefineId": defineID, "operator": "zhangsan",
	})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("startAndExecute failed: %v", r)
	}
	instanceID := mustI64(r["data"].(map[string]interface{})["processInstanceId"])

	if broken {
		def, err := repo.FindDefineByID(context.Background(), defineID)
		if err != nil || def == nil {
			t.Fatalf("define not found: %v %v", def, err)
		}
		def.Content = []byte(`{"cells": [ 这是坏 JSON`)
		repo.AddDefine(def)
	}
	return f, instanceID, defineID
}

// ① 坏定义 ⇒ 对外仍 code=0、边集/进度留空、任务行照旧，**且日志真拿到**（不接受注释自称）
func TestIssue162BrokenDefineDegradesButLogs(t *testing.T) {
	f, instanceID, defineID := i162Setup(t, true)

	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	r := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("162 口径＝降级面保留（坏定义不该让整条 highLight 变红），实得 %v", r)
	}
	hl := r["data"].(map[string]interface{})
	if got := toStrings(hl["historyEdgeNames"].([]interface{})); len(got) != 0 {
		t.Fatalf("降级档边集应为空，实得 %v", got)
	}
	if np, ok := hl["nodeProgress"].(map[string]interface{}); !ok || len(np) != 0 {
		t.Fatalf("降级档 nodeProgress 应为空对象，实得 %v", hl["nodeProgress"])
	}
	if act := toStrings(hl["activeNodeNames"].([]interface{})); !containsStr2(act, "task1") {
		t.Fatalf("降级只许影响模型补全腿，活跃任务行仍须是 task1，实得 %v", act)
	}

	out := buf.String()
	if !strings.Contains(out, "模型补全腿解码失败") {
		t.Fatalf("降级必须落一条可诊断日志（静默通道就是本案要收的那条），实得 %q", out)
	}
	if !strings.Contains(out, "instanceId=") || !strings.Contains(out, fmt.Sprintf("defineId=%d", defineID)) || !strings.Contains(out, "err=") {
		t.Fatalf("日志要带可归因的三件（实例/定义号取对/原因，缺一件等于没落），实得 %q（defineID=%d）", out, defineID)
	}
}

// ② 回归：好定义 ⇒ 不得有这条 WARN（证明它挂在失败支上，不是每次高亮都刷屏）
func TestIssue162ValidDefineLogsNothing(t *testing.T) {
	f, instanceID, _ := i162Setup(t, false)

	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	r := f.Flow("processInstance/highLight", map[string]interface{}{"id": instanceID})
	if code, _ := r["code"].(int); code != 0 {
		t.Fatalf("highLight failed: %v", r)
	}
	hl := r["data"].(map[string]interface{})
	if got := toStrings(hl["historyEdgeNames"].([]interface{})); len(got) == 0 {
		t.Fatalf("好定义下补全腿应产出边集（本用例若空＝夹具没走到模型腿，②档自证失效）: %v", got)
	}
	if strings.Contains(buf.String(), "模型补全腿解码失败") {
		t.Fatalf("正常路径不该出现降级日志：%s", buf.String())
	}
}
