// Package flowsutil 解析本仓 flows/ 流程定义目录。
//
// 唯一编辑源是 jeeflow-java 仓的 test/resources/flows/。本仓 flows/ 是其副本，
// 入库 commit（单语言用户下载即用，不依赖隔壁 Java 仓）。
//
// Dir() 的语义（维护者与用户统一入口）：
//  1. 环境变量 JEEFLOW_FLOWS_DIR 显式覆盖（容器/特殊部署）
//  2. 否则从当前工作目录向上找第一个含 flows/（且有 .json）的目录 = 本仓根
//     （go test 的 cwd 是包目录，demo 二进制在 Docker 里 cwd=/app，均向上命中）
//  3. 若本仓根的兄弟目录里有 Java 源（维护者机器）→ 精确镜像进本仓 flows/
//     （拷贝所有 *.json + 删除本仓多出的孤儿 *.json，防 id 按文件名排序错位）
//  4. 始终返回本仓 flows/ 路径 —— 所有读取点只读这里，Java 仓不再被直接读取
//
// 用 internal/ 隔离：demo 与仓内测试可 import，外部消费者 import 不到（不进发布 SDK）。
package flowsutil

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// java 源目录相对本仓根的位置（jeeflow-java 与本仓是 jeeflow-hub 下的兄弟目录）
const javaFlowsRel = "../jeeflow-java/jeeflow-core/src/test/resources/flows"

// Dir 返回本仓 flows/ 目录绝对路径；维护者机器上会先把 Java 源精确镜像进来。
func Dir() string {
	if env := os.Getenv("JEEFLOW_FLOWS_DIR"); env != "" {
		return env
	}
	wd, err := os.Getwd()
	if err != nil {
		panic("flowsutil: cannot get working directory")
	}
	root := findFlowsRoot(wd)
	if root == "" {
		panic("flowsutil: no flows/ directory found from " + wd)
	}
	if err := mirror(root); err != nil {
		panic("flowsutil: mirror: " + err.Error())
	}
	return filepath.Join(root, "flows")
}

// findFlowsRoot 从 start 向上找第一个含 flows/ 且内有 .json 的目录（本仓根）。
func findFlowsRoot(start string) string {
	dir := filepath.Clean(start)
	for {
		if hasFlows(dir) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir { // 到文件系统顶
			return ""
		}
		dir = parent
	}
}

func hasFlows(dir string) bool {
	fdir := filepath.Join(dir, "flows")
	entries, err := os.ReadDir(fdir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			return true
		}
	}
	return false
}

// mirror 若 Java 源存在则精确镜像到本仓 flows/（拷所有 + 删孤儿），不存在则原样返回。
// issues/170：复制腿不再直写（先 O_TRUNC 再写会与别的进程的 ReadFile 撞出撕裂读——
// `go test ./...` 各包测试二进制并发启动各自触发本镜像，读到 0 字节/半截 JSON ⇒ 既有
// 用例随机红）。现形状＝「稳态零写 + 写 .tmp + Rename 原子替换」：
//   - 内容已一致的跳过（静态树上整个镜像零写，这是绝大多数测试轮的形态）；
//   - 有变动才写 .tmp（名带本进程 pid，跨进程各写各的）再同目录 Rename——同目录改名
//     对读者是原子的（读者只可能见到旧整份或新整份）；
//   - Windows 语义：Go 的 os.Open sharemode 不带 FILE_SHARE_DELETE，读者正开着目标
//     文件时 Rename 覆盖会 Access Denied ⇒ 短睡重试（读者窗口微秒级），重试耗尽才显式
//     上抛（Dir panic），不再 `_ =` 吞。
func mirror(root string) error {
	src := filepath.Join(root, javaFlowsRel)
	dst := filepath.Join(root, "flows")
	srcEntries, err := os.ReadDir(src)
	if err != nil { // 用户单仓 / 容器：无 Java 源，跳过镜像
		return nil
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}
	srcNames := map[string]bool{}
	for _, e := range srcEntries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		srcNames[e.Name()] = true
		data, rerr := os.ReadFile(filepath.Join(src, e.Name()))
		if rerr != nil {
			continue
		}
		dstPath := filepath.Join(dst, e.Name())
		if old, oerr := os.ReadFile(dstPath); oerr == nil && bytes.Equal(old, data) {
			continue
		}
		tmp := dstPath + ".tmp." + strconv.Itoa(os.Getpid())
		if werr := os.WriteFile(tmp, data, 0o644); werr != nil {
			return fmt.Errorf("写临时件 %s: %w", tmp, werr)
		}
		var rerr2 error
		for attempt := 0; attempt < 100; attempt++ {
			if rerr2 = os.Rename(tmp, dstPath); rerr2 == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if rerr2 != nil {
			return fmt.Errorf("原子替换 %s: %w", e.Name(), rerr2)
		}
	}
	// 孤儿清理：本仓有、Java 源已无的 *.json（防 id 错位）
	dstEntries, _ := os.ReadDir(dst)
	for _, e := range dstEntries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		if !srcNames[e.Name()] {
			_ = os.Remove(filepath.Join(dst, e.Name()))
		}
	}
	return nil
}
