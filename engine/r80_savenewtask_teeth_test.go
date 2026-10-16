// audit8 R80（issues/175 §6.5 M3b）——`createTaskWithActors` 五个建单写点的**上抛有牙**判据。
//
// 病灶回看：R74 把 `saveNewTask` 的错误改成逐调用点上抛，但变异对照发现这一族的五处
// （零参与者支／PARALLEL 循环／SEQUENTIAL 首位／未知 countersignType 兜底支／普通建单支）
// 摘掉上抛后**五腿＋tier 七格全绿＝等价**——当时的夹具是单参与者线性流，一次只走一个写点，
// 而且没有一格注入"仓储写失败"。本文件补的就是这一格：
// 逐支按"第 k 次 SaveTask 注入失败"表驱动，要求
//  1. 引擎把错误上抛（不得吞）；
//  2. 抛错后**不再继续建单**（读回的 DOING 行数恰为 k-1，防"吞掉错误继续跑完"那一形）；
//  3. 不注入时正常建满（对照组，防判据空转）。
//
// 同包测试（`package engine`）：`createTaskWithActors` 是包内函数，外部测试包够不着；
// 夹具（c137Node/c137UserProv/c137IDGen/c137Inst）复用同目录 137 那份，同包共享顶层命名空间。
package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

// r80Repo 只挡 `SaveTask` 的第 failOn 次调用（0＝不挡）；其余方法由内嵌内存仓原样透传。
type r80Repo struct {
	*memory.Repository
	failOn int
	calls  int
}

var errR80Save = errors.New("r80: SaveTask 注入失败")

func (r *r80Repo) SaveTask(ctx context.Context, t *model.ProcessTask) error {
	r.calls++
	if r.failOn == r.calls {
		return errR80Save
	}
	return r.Repository.SaveTask(ctx, t)
}

func r80Harness(failOn int) (*EngineImpl, *r80Repo) {
	repo := &r80Repo{Repository: memory.New(), failOn: failOn}
	return New(repo, c137UserProv{}, &c137IDGen{}, nil), repo
}

func TestR80CreateTaskWithActors每支写点都上抛(t *testing.T) {
	cases := []struct {
		name   string
		props  map[string]interface{}
		actors []string
		saves  int // 该分支正常走完会调几次 SaveTask
	}{
		{
			name:   "零参与者支（engine_impl.go:688）",
			props:  map[string]interface{}{},
			actors: nil,
			saves:  1,
		},
		{
			name:   "PARALLEL 会签逐人支（:702，循环三次）",
			props:  map[string]interface{}{"performType": 1, "countersignType": "PARALLEL"},
			actors: []string{"a1", "a2", "a3"},
			saves:  3,
		},
		{
			name:   "SEQUENTIAL 首位支（:717）",
			props:  map[string]interface{}{"performType": 1, "countersignType": "SEQUENTIAL"},
			actors: []string{"a1", "a2", "a3"},
			saves:  1,
		},
		{
			name:   "未知 countersignType 兜底支（:726，合成档——16 份共享夹具喂不到 default）",
			props:  map[string]interface{}{"performType": 1, "countersignType": "BOGUS-TYPE"},
			actors: []string{"a1", "a2"},
			saves:  2,
		},
		{
			name:   "普通建单支（:739，多参与者也只建一行）",
			props:  map[string]interface{}{},
			actors: []string{"a1", "a2"},
			saves:  1,
		},
	}

	for _, tc := range cases {
		// 对照组：不注入 ⇒ 建满、不报错（判据不空转）
		e, repo := r80Harness(0)
		flow, node := c137Node(tc.props)
		if err := e.createTaskWithActors(context.Background(), flow, node, c137Inst(), "u1",
			map[string]interface{}{}, tc.actors, 0); err != nil {
			t.Fatalf("%s：不注入时不该报错：%v", tc.name, err)
		}
		if repo.calls != tc.saves {
			t.Fatalf("%s：前置读数 SaveTask 次数＝%d，期望 %d（该支形状变了，判据要跟着改）", tc.name, repo.calls, tc.saves)
		}
		if n := r80Doing(t, repo); n != tc.saves {
			t.Fatalf("%s：对照组应建满 %d 行，实读 %d 行", tc.name, tc.saves, n)
		}

		// 注入档：逐次挡在第 k 个写点上 ⇒ 必须上抛，且停在第 k 行之前
		for k := 1; k <= tc.saves; k++ {
			e2, repo2 := r80Harness(k)
			flow2, node2 := c137Node(tc.props)
			err := e2.createTaskWithActors(context.Background(), flow2, node2, c137Inst(), "u1",
				map[string]interface{}{}, tc.actors, 0)
			if err == nil {
				t.Fatalf("%s：挡在第 %d 次 SaveTask 上必须上抛，实得 nil（＝吞错继续跑）", tc.name, k)
			}
			if !errors.Is(err, errR80Save) {
				t.Fatalf("%s：第 %d 次注入要原样透出仓储错误，实得 %v", tc.name, k, err)
			}
			if n := r80Doing(t, repo2); n != k-1 {
				t.Fatalf("%s：第 %d 次注入后应停在 %d 行（抛错即停），实读 %d 行", tc.name, k, k-1, n)
			}
		}
	}
}

func r80Doing(t *testing.T, repo *r80Repo) int {
	t.Helper()
	tasks, err := repo.FindDoingTasks(context.Background(), 777, nil)
	if err != nil {
		t.Fatalf("读回 DOING 行: %v", err)
	}
	return len(tasks)
}
