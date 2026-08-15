package persist

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// issues/65：MySQL VARCHAR Scan 进 []byte 后 JSON 不能编成 Base64。
func TestDecodeDriverValueJSON(t *testing.T) {
	row := map[string]interface{}{
		"company_name": decodeDriverValue([]byte("SYNC_INIT")),
		"contact_name": decodeDriverValue([]byte("办理更新")),
		"amount":       decodeDriverValue(int64(42)),
		"empty":        decodeDriverValue(nil),
		"nil_bytes":    decodeDriverValue([]byte(nil)),
		"when":         decodeDriverValue(time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)),
	}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"SYNC_INIT"`) {
		t.Fatalf("VARCHAR 应是明文, got %s", s)
	}
	if strings.Contains(s, "U1lOQ19JTklU") {
		t.Fatalf("不得 Base64(SYNC_INIT): %s", s)
	}
	if !strings.Contains(s, `"办理更新"`) {
		t.Fatalf("中文 VARCHAR 应是明文, got %s", s)
	}
	if strings.Contains(s, "5Yqe55CG5pu05paw") {
		t.Fatalf("不得 Base64(办理更新): %s", s)
	}
	if decodeDriverValue(int64(7)) != int64(7) {
		t.Fatal("int64 应原样")
	}
	if decodeDriverValue(nil) != nil {
		t.Fatal("nil 应原样")
	}
	if decodeDriverValue([]byte(nil)) != nil {
		t.Fatal("[]byte(nil) 当 NULL")
	}
}
