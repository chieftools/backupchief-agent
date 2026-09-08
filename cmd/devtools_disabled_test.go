//go:build !devtools

package cmd

import (
	"strings"
	"testing"
)

func TestProductionCommandOmitsDevelopmentTools(t *testing.T) {
	root := NewRootCommand("1.2.3")
	root.SetArgs([]string{"dev"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("production command accepted dev tools: %v", err)
	}
}
