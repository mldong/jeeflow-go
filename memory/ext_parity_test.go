// 内存扩展仓储的委托查询判据对拍（issues/116 判据 a/b/c/d，规范 08 用例 27）：
// 数据集与期望表在 internal/surrparity 单点维护，与 repository/jdbc 的
// surrogate_parity_test.go 跑的是同一份，两仓答案不一致即红。
package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/internal/surrparity"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

func TestExtSurrogateQueryParity(t *testing.T) {
	ext := memory.NewExt()
	now := time.Now().Truncate(time.Millisecond)
	surrparity.Run(t, ext, func(r surrparity.Row) error {
		s := &model.ProcessSurrogate{
			ID: r.ID, Operator: r.Operator, Surrogate: r.Surrogate, ProcessName: r.ProcessName,
			Enabled: r.Enabled, CreateUser: "t", UpdateUser: "t",
		}
		if r.HasStart {
			t1 := now.Add(r.StartOff)
			s.StartTime = &t1
		}
		if r.HasEnd {
			t2 := now.Add(r.EndOff)
			s.EndTime = &t2
		}
		return ext.SaveSurrogate(context.Background(), s)
	}, now)
}
