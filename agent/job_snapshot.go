package agent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func encodeJobSnapshot(bootstrap Bootstrap, runID string, revision uint64, job Job) (string, error) {
	encoded, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("encode job snapshot: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeJobSnapshot(bootstrap Bootstrap, runID string, revision uint64, encoded string) (Job, error) {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return Job{}, fmt.Errorf("decode job snapshot: %w", err)
	}
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return Job{}, fmt.Errorf("decode job snapshot: %w", err)
	}
	if err := validateJob(job); err != nil {
		return Job{}, fmt.Errorf("validate job snapshot: %w", err)
	}
	return job, nil
}
