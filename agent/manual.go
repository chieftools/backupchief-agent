package agent

import (
	"fmt"
	"os"
	"strings"

	"github.com/chieftools/backupchief-agent/restic"
)

func LoadConfiguration(path string) (Config, ConfigMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, ConfigMetadata{}, fmt.Errorf("read configuration: %w", err)
	}
	config, metadata, err := DecodeConfig(data, 0)
	if err != nil {
		return Config{}, metadata, err
	}
	return config, metadata, nil
}

func SelectJob(config Config, selector string) (Job, error) {
	for _, job := range config.Jobs {
		if job.Key == selector {
			return job, nil
		}
	}
	matches := make([]Job, 0, 1)
	for _, job := range config.Jobs {
		if job.Name != "" && strings.EqualFold(job.Name, selector) {
			matches = append(matches, job)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		keys := make([]string, 0, len(matches))
		for _, job := range matches {
			keys = append(keys, job.Key)
		}
		return Job{}, fmt.Errorf("job name %q is ambiguous; use one of: %s", selector, strings.Join(keys, ", "))
	}
	return Job{}, fmt.Errorf("job %q was not found", selector)
}

func SelectJobRepository(job Job, selector string) (JobRepository, error) {
	if selector == "" || job.Repository.Key == selector {
		return job.Repository, nil
	}
	for _, repository := range job.Replicas {
		if repository.Key == selector {
			return repository, nil
		}
	}

	return JobRepository{}, fmt.Errorf("repository %q is not configured for job %q", selector, job.Key)
}

func ResticRequest(job Job, operation, host string) restic.Request {
	return ResticRequestForRepository(job, job.Repository, operation, host)
}

func ResticRequestForRepository(job Job, repository JobRepository, operation, host string) restic.Request {
	return restic.Request{
		Version: 1, Operation: operation,
		Connection: repository.Connection,
		Password:   repository.ServicePassword, Root: job.Source.Root,
		Excludes: append([]string{}, job.Source.Excludes...), Host: host,
		Tags: []string{"backupchief-job:" + job.ID}, TimeoutSeconds: 12 * 60 * 60, LockWaitSeconds: 5 * 60,
	}
}
