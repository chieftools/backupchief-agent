package cmd

import (
	"errors"
	"testing"
)

func TestExitCodeDistinguishesUsageAndOperationOutcomes(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{name: "usage", err: errors.New("synthetic invalid configuration"), want: 2},
		{name: "operation", err: operationError(1, "synthetic operation failure"), want: 1},
		{name: "partial", err: operationError(3, "synthetic partial result"), want: 3},
		{name: "cancelled", err: operationError(130, "synthetic cancellation"), want: 130},
	} {
		t.Run(test.name, func(t *testing.T) {
			if actual := ExitCode(test.err); actual != test.want {
				t.Fatalf("exit code %d, want %d", actual, test.want)
			}
		})
	}
}
