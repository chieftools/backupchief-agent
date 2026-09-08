package restic

import (
	"reflect"
	"strings"
	"testing"
)

func TestMaintenanceRequestsUseExplicitBoundedArguments(t *testing.T) {
	request := testRequest(t)
	request.Operation = "forget_plan"
	request.Retention = &Retention{Last: 12, Hourly: 24, Daily: 7, Weekly: 4, Monthly: 3, Yearly: 1}
	arguments, _, err := request.arguments("password", "new-password", "cache", true)
	if err != nil {
		t.Fatal(err)
	}
	wantTail := []string{
		"forget", "--dry-run", "--json", "--group-by", "",
		"--keep-last", "12", "--keep-hourly", "24", "--keep-daily", "7",
		"--keep-weekly", "4", "--keep-monthly", "3", "--keep-yearly", "1",
	}
	if !reflect.DeepEqual(arguments[len(arguments)-len(wantTail):], wantTail) {
		t.Fatalf("forget plan arguments: %v", arguments)
	}
	for _, argument := range arguments {
		if argument == "unlock" || argument == "--remove-all" {
			t.Fatalf("unsafe argument: %q", argument)
		}
	}

	request.Operation = "forget"
	request.Retention = nil
	request.SnapshotIDs = []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	arguments, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !reflect.DeepEqual(arguments[len(arguments)-3:], append([]string{"forget"}, request.SnapshotIDs...)) {
		t.Fatalf("fixed forget arguments: %v %v", arguments, err)
	}

	request.Operation = "check_data"
	request.SnapshotIDs = nil
	request.DataSubsetPart = 3
	request.DataSubsetTotal = 7
	arguments, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !reflect.DeepEqual(arguments[len(arguments)-4:], []string{"check", "--json", "--read-data-subset", "3/7"}) {
		t.Fatalf("data check arguments: %v %v", arguments, err)
	}

	request.Operation = "check_metadata"
	request.DataSubsetPart = 0
	request.DataSubsetTotal = 0
	arguments, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !reflect.DeepEqual(arguments[len(arguments)-2:], []string{"check", "--json"}) {
		t.Fatalf("metadata check arguments: %v %v", arguments, err)
	}
}
