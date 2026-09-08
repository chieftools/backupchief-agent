package agent

import (
	"strings"
	"testing"
)

func TestSelectJobUsesExactKeyOrUniqueDisplayName(t *testing.T) {
	config, _, err := DecodeConfig(standaloneConfigBody(), 0)
	if err != nil {
		t.Fatal(err)
	}

	job, err := SelectJob(config, "job_documents")
	if err != nil || job.Key != "job_documents" {
		t.Fatalf("select exact key: %+v %v", job, err)
	}
	job, err = SelectJob(config, "synthetic documents")
	if err != nil || job.Key != "job_documents" {
		t.Fatalf("select unique display name: %+v %v", job, err)
	}
	config.Jobs[0].Name = "Synthetic shared name"
	config.Jobs[1].Name = "SYNTHETIC SHARED NAME"
	if _, err := SelectJob(config, "synthetic shared name"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous display name error: %v", err)
	}
}
