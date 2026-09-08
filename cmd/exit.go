package cmd

import (
	"errors"
	"fmt"
)

type exitError struct {
	code int
	err  error
}

func (err *exitError) Error() string {
	return err.err.Error()
}

func (err *exitError) Unwrap() error {
	return err.err
}

func operationError(code int, format string, arguments ...any) error {
	return &exitError{code: code, err: fmt.Errorf(format, arguments...)}
}

func ExitCode(err error) int {
	var commandError *exitError
	if errors.As(err, &commandError) {
		return commandError.code
	}
	return 2
}
