// 扩展仓储 JDBC 集成测试（v1.1.0）——MySQL / PostgreSQL 双库可跑。
//
// 用法与 jdbc_test.go 相同：JEFFLOW_DB_DRIVER / JEFFLOW_DB_DSN 环境变量覆盖。
// 测试数据按固定 operator 标记（goext-*），开头清理，可重复执行。
package jdbc_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mldong/jeeflow-go/model"
	"github.com/mldong/jeeflow-go/repository/jdbc"
	"github.com/mldong/jeeflow-go/spi"
)

// ensureExtTables 幂等建三张扩展表（IF NOT EXISTS；schema 源 jeeflow-java resources）
func ensureExtTables(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var stmts []string
	if testDriver() == "pgx" {
		stmts = []string{
			"CREATE TABLE IF NOT EXISTS wf_process_design (id BIGINT NOT NULL, name VARCHAR(100) NOT NULL, display_name VARCHAR(200) NOT NULL, type VARCHAR(50) DEFAULT 'approval', icon VARCHAR(200), is_deployed INT DEFAULT 0, remark TEXT, create_time TIMESTAMP(3), create_user VARCHAR(64), update_time TIMESTAMP(3), update_user VARCHAR(64), PRIMARY KEY (id))",
			"CREATE INDEX IF NOT EXISTS idx_process_design_name ON wf_process_design (name)",
			"CREATE TABLE IF NOT EXISTS wf_process_design_his (id BIGINT NOT NULL, process_design_id BIGINT NOT NULL, content TEXT, create_time TIMESTAMP(3), create_user VARCHAR(64), PRIMARY KEY (id))",
			"CREATE INDEX IF NOT EXISTS idx_process_design_his_pdid ON wf_process_design_his (process_design_id)",
			"CREATE TABLE IF NOT EXISTS wf_process_surrogate (id BIGINT NOT NULL, process_name VARCHAR(100), operator VARCHAR(64) NOT NULL, surrogate VARCHAR(64) NOT NULL, start_time TIMESTAMP(3), end_time TIMESTAMP(3), enabled INT DEFAULT 1, create_time TIMESTAMP(3), create_user VARCHAR(64), update_time TIMESTAMP(3), update_user VARCHAR(64), PRIMARY KEY (id))",
			"CREATE INDEX IF NOT EXISTS idx_process_surrogate_op ON wf_process_surrogate (operator)",
			"CREATE INDEX IF NOT EXISTS idx_process_surrogate_sur ON wf_process_surrogate (surrogate)",
		}
	} else {
		stmts = []string{
			"CREATE TABLE IF NOT EXISTS wf_process_design (id BIGINT NOT NULL COMMENT '主键', name VARCHAR(100) NOT NULL COMMENT '流程编码', display_name VARCHAR(200) NOT NULL COMMENT '显示名称', type VARCHAR(50) DEFAULT 'approval', icon VARCHAR(200), is_deployed INT DEFAULT 0, remark TEXT, create_time DATETIME(3), create_user VARCHAR(64), update_time DATETIME(3), update_user VARCHAR(64), PRIMARY KEY (id), KEY idx_process_design_name (name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			"CREATE TABLE IF NOT EXISTS wf_process_design_his (id BIGINT NOT NULL COMMENT '主键', process_design_id BIGINT NOT NULL COMMENT '设计ID', content BLOB, create_time DATETIME(3), create_user VARCHAR(64), PRIMARY KEY (id), KEY idx_process_design_his_pdid (process_design_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			"CREATE TABLE IF NOT EXISTS wf_process_surrogate (id BIGINT NOT NULL COMMENT '主键', process_name VARCHAR(100), operator VARCHAR(64) NOT NULL COMMENT '授权人', surrogate VARCHAR(64) NOT NULL COMMENT '代理人', start_time DATETIME(3), end_time DATETIME(3), enabled INT DEFAULT 1, create_time DATETIME(3), create_user VARCHAR(64), update_time DATETIME(3), update_user VARCHAR(64), PRIMARY KEY (id), KEY idx_process_surrogate_op (operator), KEY idx_process_surrogate_sur (surrogate)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		}
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, ph(stmt)); err != nil {
			t.Fatalf("ensure ext table: %v", err)
		}
	}
}

func TestExtDesignCrudAndHis(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	ctx := context.Background()

	// 清理本测试数据（按 name 标记）
	_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_design_his WHERE process_design_id IN (SELECT id FROM wf_process_design WHERE name = ?)"), "goext-design")
	_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_design WHERE name = ?"), "goext-design")

	ext := jdbc.NewExt(db)

	// save + 快照
	d := &model.ProcessDesign{Name: "goext-design", DisplayName: "测试设计", Type: "approval", CreateUser: "t", UpdateUser: "t"}
	if err := ext.SaveDesign(ctx, d); err != nil {
		t.Fatalf("save design: %v", err)
	}
	if d.ID == 0 {
		t.Fatalf("save design should assign id")
	}
	for i, v := range []string{`{"v":1}`, `{"v":2}`} {
		if err := ext.SaveDesignHis(ctx, &model.ProcessDesignHis{
			ProcessDesignID: d.ID,
			Content:         []byte(v),
			CreateUser:      "t",
		}); err != nil {
			t.Fatalf("save his %d: %v", i, err)
		}
	}

	// 查询 + 历史（倒序）
	loaded, err := ext.FindDesignByID(ctx, d.ID)
	if err != nil || loaded == nil || loaded.Name != "goext-design" {
		t.Fatalf("find design: %+v err=%v", loaded, err)
	}
	hisList, err := ext.ListDesignHis(ctx, d.ID)
	if err != nil || len(hisList) != 2 {
		t.Fatalf("his list = %d, want 2", len(hisList))
	}
	if string(hisList[0].Content) != `{"v":2}` {
		t.Fatalf("his[0] = %s, want v2（倒序）", hisList[0].Content)
	}

	// update
	loaded.DisplayName = "测试设计 v2"
	loaded.IsDeployed = 1
	if err := ext.UpdateDesign(ctx, loaded); err != nil {
		t.Fatalf("update design: %v", err)
	}
	updated, _ := ext.FindDesignByID(ctx, d.ID)
	if updated.DisplayName != "测试设计 v2" || updated.IsDeployed != 1 {
		t.Fatalf("update not persisted: %+v", updated)
	}

	// 分页
	rows, total, err := ext.PageDesigns(ctx, spi.PageQuery{PageNum: 1, PageSize: 10, Filters: map[string]interface{}{"name": "goext-design"}})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("page designs: total=%d rows=%d err=%v", total, len(rows), err)
	}

	// remove（连带历史）
	if err := ext.RemoveDesign(ctx, d.ID); err != nil {
		t.Fatalf("remove design: %v", err)
	}
	gone, _ := ext.FindDesignByID(ctx, d.ID)
	if gone != nil {
		t.Fatalf("design should be removed")
	}
	hisGone, _ := ext.ListDesignHis(ctx, d.ID)
	if len(hisGone) != 0 {
		t.Fatalf("his should be cascaded removed, got %d", len(hisGone))
	}
}

func TestExtSurrogateGet(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	ensureExtTables(t, db)
	ctx := context.Background()

	_, _ = db.ExecContext(ctx, ph("DELETE FROM wf_process_surrogate WHERE operator = ?"), "goext-op")
	ext := jdbc.NewExt(db)
	now := time.Now()

	// 全流程委托（时间窗内）
	all := &model.ProcessSurrogate{Operator: "goext-op", Surrogate: "agent-all", Enabled: 1, CreateUser: "t", UpdateUser: "t"}
	if err := ext.SaveSurrogate(ctx, all); err != nil {
		t.Fatalf("save surrogate: %v", err)
	}
	// 指定流程委托（已过期）
	expired := time.Now().Add(-48 * time.Hour)
	spec := &model.ProcessSurrogate{
		Operator: "goext-op", Surrogate: "agent-spec", ProcessName: "leave",
		StartTime: &expired, EndTime: &expired, Enabled: 1, CreateUser: "t", UpdateUser: "t",
	}
	if err := ext.SaveSurrogate(ctx, spec); err != nil {
		t.Fatalf("save surrogate spec: %v", err)
	}

	// leave：精确匹配已过期 → 兜底全流程
	hit, err := ext.GetSurrogate(ctx, "goext-op", "leave", now)
	if err != nil || hit == nil || hit.Surrogate != "agent-all" {
		t.Fatalf("getSurrogate = %+v err=%v, want agent-all（兜底）", hit, err)
	}
	// 其他流程：全流程
	hit, _ = ext.GetSurrogate(ctx, "goext-op", "other", now)
	if hit == nil || hit.Surrogate != "agent-all" {
		t.Fatalf("getSurrogate other = %+v, want agent-all", hit)
	}
	// 无委托
	if hit, _ := ext.GetSurrogate(ctx, "nobody", "leave", now); hit != nil {
		t.Fatalf("nobody should have no surrogate, got %+v", hit)
	}

	// 分页
	rows, total, err := ext.PageSurrogates(ctx, spi.PageQuery{PageNum: 1, PageSize: 10, Filters: map[string]interface{}{"operator": "goext-op"}})
	if err != nil || total != 2 || len(rows) != 2 {
		t.Fatalf("page surrogates: total=%d rows=%d err=%v", total, len(rows), err)
	}

	// 清理
	_ = ext.RemoveSurrogate(ctx, all.ID)
	_ = ext.RemoveSurrogate(ctx, spec.ID)
}
