package cmd

import (
	"strings"
	"testing"
)

func TestBuildDescriptionNamesTheCommit(t *testing.T) {
	old := BuildCommit
	defer func() { BuildCommit = old }()
	BuildCommit = "0123456789abcdef0123"
	if got := buildDescription(); !strings.HasPrefix(got, "build 0123456789ab") {
		t.Fatalf("description = %q", got)
	}
}
