// 实例级排他锁（spec 04 · 并发保护）的 T0：SQLite 单写者档必须是无害 no-op，
// 事务内/事务外调用都不报错。MySQL/PostgreSQL 的 FOR UPDATE 腿属 T1（真库冒烟）。
package jdbc_test

import (
	"context"
	"testing"

	"github.com/mldong/jeeflow-go/repository/jdbc"
)

func TestLockInstanceSQLiteNoop(t *testing.T) {
	db := openSQLite129(t)
	defer db.Close()
	repo := jdbc.New(db)
	ctx := context.Background()

	if err := repo.LockInstance(ctx, 1); err != nil {
		t.Fatalf("事务外 LockInstance 不应报错（行锁随语句释放，直接跳过）: %v", err)
	}
	if err := repo.WithTx(ctx, func(ctx context.Context) error {
		return repo.LockInstance(ctx, 1)
	}); err != nil {
		t.Fatalf("SQLite 单写者档 LockInstance 不应报错: %v", err)
	}
}
