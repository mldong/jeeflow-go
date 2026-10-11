// issues/170 · flowsutil.mirror 的镜像语义单测：原子替换路径 / 稳态零写 / 无 .tmp 残件 /
// 错误真上抛。跨进程撕裂读（真正的事故形态）没法在单进程单测里稳定复现——那半边的证据是
// 「还原旧形状后 go test -count=3 ./... 随机红」的变异对照；这里钉的是可确定复现的机制面。
package flowsutil

import (
	"os"
	"path/filepath"
	"testing"
)

// i170Setup 造一个最小维护者布局：<base>/repo（本仓）+ <base>/jeeflow-java/…/flows（Java 源）。
func i170Setup(t *testing.T) (root, src string) {
	t.Helper()
	base := t.TempDir()
	root = filepath.Join(base, "repo")
	src = filepath.Join(base, "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows")
	if err := os.MkdirAll(filepath.Join(root, "flows"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "01-a.json"), []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "02-b.json"), []byte(`{"b":2}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// 孤儿：本仓有、Java 源已无 ⇒ mirror 应删
	if err := os.WriteFile(filepath.Join(root, "flows", "99-orphan.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, src
}

func i170Tmps(t *testing.T, dst string) []string {
	t.Helper()
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	var tmps []string
	for _, e := range entries {
		if name := e.Name(); len(name) > 5 && filepath.Ext(name) != ".json" {
			tmps = append(tmps, name)
		}
	}
	return tmps
}

func TestI170MirrorAtomicAndZeroWrite(t *testing.T) {
	root, src := i170Setup(t)
	dst := filepath.Join(root, "flows")

	// ① 首轮：全量复制 + 删孤儿
	if err := mirror(root); err != nil {
		t.Fatalf("mirror 首轮报错: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "01-a.json")); string(b) != `{"a":1}` {
		t.Fatalf("01-a.json 内容不符: %q", b)
	}
	if _, err := os.Stat(filepath.Join(dst, "02-b.json")); err != nil {
		t.Fatalf("02-b.json 未复制: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "99-orphan.json")); !os.IsNotExist(err) {
		t.Fatalf("孤儿 99-orphan.json 应被删除，实得 err=%v", err)
	}
	if tmps := i170Tmps(t, dst); len(tmps) != 0 {
		t.Fatalf("镜像后不应有非 .json 残件，实得 %v", tmps)
	}

	// ② 稳态零写：内容没变时第二轮不得落笔（mtime 是"真没写"的判据）
	st1, _ := os.Stat(filepath.Join(dst, "01-a.json"))
	if err := mirror(root); err != nil {
		t.Fatalf("mirror 二轮报错: %v", err)
	}
	st2, _ := os.Stat(filepath.Join(dst, "01-a.json"))
	if !st1.ModTime().Equal(st2.ModTime()) {
		t.Fatalf("内容未变时不得重写（mtime 变了）：%v -> %v", st1.ModTime(), st2.ModTime())
	}

	// ③ 源变更：走 .tmp + Rename，落地为新内容且不留残件
	if err := os.WriteFile(filepath.Join(src, "01-a.json"), []byte(`{"a":2}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := mirror(root); err != nil {
		t.Fatalf("mirror 变更轮报错: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "01-a.json")); string(b) != `{"a":2}` {
		t.Fatalf("变更后内容未落地: %q", b)
	}
	if tmps := i170Tmps(t, dst); len(tmps) != 0 {
		t.Fatalf("变更镜像后不应有残件，实得 %v", tmps)
	}

	// ④ 无 Java 源（用户单仓/容器）：静默跳过，不报错、不建目录
	soloRoot := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(soloRoot, "flows"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := mirror(soloRoot); err != nil {
		t.Fatalf("无 Java 源时必须静默跳过，实得: %v", err)
	}
}

// TestI170MirrorErrorNotSwallowed 写/替换失败必须上抛（旧形状 `_ =` 吞掉）。
// 构造：目标名在 dst 里被一个**目录**占住 ⇒ Rename 文件→目录必失败（两平台同形）。
func TestI170MirrorErrorNotSwallowed(t *testing.T) {
	root, src := i170Setup(t)
	dst := filepath.Join(root, "flows")
	if err := os.WriteFile(filepath.Join(src, "03-c.json"), []byte(`{"c":3}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dst, "03-c.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := mirror(root); err == nil {
		t.Fatal("Rename 撞上同名目录必须报错上抛，实得 nil（旧形状的 `_ =` 吞法）")
	}
}
