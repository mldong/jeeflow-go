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
	"os"
	"path/filepath"
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
	mirror(root)
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
func mirror(root string) {
	src := filepath.Join(root, javaFlowsRel)
	dst := filepath.Join(root, "flows")
	srcEntries, err := os.ReadDir(src)
	if err != nil { // 用户单仓 / 容器：无 Java 源，跳过镜像
		return
	}
	_ = os.MkdirAll(dst, 0o755)
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
		_ = os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644)
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
}
