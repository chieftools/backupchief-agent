package restic

import (
	"encoding/json"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestInteractiveRestoresUseTheWholeMachine(t *testing.T) {
	request := testRequest(t)
	request.Operation = "restore"
	request.Snapshot = strings.Repeat("a", 64)
	request.Target = t.TempDir()

	_, background, err := request.arguments("password", "", "cache", true)
	if err != nil {
		t.Fatal(err)
	}
	if effectiveEnvironment(background, "GOMAXPROCS") != "1" || effectiveEnvironment(background, "RESTIC_READ_CONCURRENCY") != "1" {
		t.Fatalf("background restore environment: %v", background)
	}

	request.Interactive = true
	_, interactive, err := request.arguments("password", "", "cache", true)
	if err != nil {
		t.Fatal(err)
	}
	if effectiveEnvironment(interactive, "GOMAXPROCS") != strconv.Itoa(runtime.NumCPU()) || effectiveEnvironment(interactive, "RESTIC_READ_CONCURRENCY") != "8" {
		t.Fatalf("interactive restore environment: %v", interactive)
	}
}

func TestHelperRequestsCannotMarkThemselvesInteractive(t *testing.T) {
	request := testRequest(t)
	request.Interactive = true
	encoded, err := json.Marshal(request)
	if err != nil || strings.Contains(strings.ToLower(string(encoded)), `"interactive"`) {
		t.Fatalf("interactive leaked into the wire format: %s %v", encoded, err)
	}

	var decoded Request
	if err = json.Unmarshal([]byte(`{"interactive":true}`), &decoded); err != nil || decoded.Interactive {
		t.Fatalf("interactive was read from a request: %+v %v", decoded, err)
	}
}

// effectiveEnvironment returns the value a child process sees: the last entry for a key wins.
func effectiveEnvironment(environment []string, key string) string {
	value := ""
	for _, entry := range environment {
		if strings.HasPrefix(entry, key+"=") {
			value = strings.TrimPrefix(entry, key+"=")
		}
	}

	return value
}
