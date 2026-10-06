package restic

import (
	"slices"
	"testing"
)

func TestResticInvocationsRemoveStaleRepositoryCaches(t *testing.T) {
	request := testRequest(t)
	request.Operation = "snapshots"

	args, _, err := request.arguments("password", "", "cache", true)
	if err != nil || !slices.Contains(args, "--cleanup-cache") {
		t.Fatalf("snapshot arguments: %v %v", args, err)
	}
}
