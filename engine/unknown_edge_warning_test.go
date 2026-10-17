package engine

import (
	"strings"
	"testing"
)

func TestUnknownEdgeWarningCarriesFromTo(t *testing.T) {
	s := unknownEdgeWarning("node-a", "node-b")
	if !strings.Contains(s, "from=node-a") || !strings.Contains(s, "to=node-b") || !strings.Contains(s, "WARNING") {
		t.Fatalf("unexpected warning: %s", s)
	}
}
