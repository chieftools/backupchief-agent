package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

const maxResticRequestBytes = 1 << 20

func newResticCommand() *cobra.Command {
	var state string
	var requestFile string
	var allowLocal bool

	command := &cobra.Command{
		Use:   "restic",
		Short: "Execute one bounded local restic request",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runResticCommand(command, state, requestFile, allowLocal)
		},
	}

	command.Flags().StringVar(&state, "state", "/var/lib/backupchief", "Private execution state directory")
	command.Flags().StringVar(&requestFile, "request-file", "", "Private request file for local service qualification")
	command.Flags().BoolVar(&allowLocal, "development-local", false, "Permit a local repository for development tests")

	return command
}

func runResticCommand(
	command *cobra.Command,
	state string,
	requestFile string,
	allowLocal bool,
) error {
	signalContext, cancelSignals := signal.NotifyContext(
		command.Context(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancelSignals()

	input := command.InOrStdin()

	if requestFile != "" {
		file, err := os.OpenFile(requestFile, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return errors.New("cannot open private request file")
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return errors.New("request file must be private")
		}

		input = file
	}

	reader := bufio.NewReaderSize(
		io.LimitReader(input, maxResticRequestBytes),
		maxResticRequestBytes,
	)
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return errors.New("cannot read restic request")
	}
	if len(line) >= maxResticRequestBytes {
		return errors.New("restic request exceeded its limit")
	}

	var request restic.Request

	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		return errors.New("invalid restic request")
	}

	executionContext, stopExecution := context.WithCancel(signalContext)
	defer stopExecution()

	if requestFile == "" {
		// PHP keeps stdin open as a lifetime pipe; worker death cancels local execution.
		go func() {
			var buffer [1]byte

			_, _ = input.Read(buffer[:])
			stopExecution()
		}()
	}

	runner := restic.Runner{
		State:      state,
		AllowLocal: allowLocal,
	}
	result := runner.Run(executionContext, request)

	return json.NewEncoder(command.OutOrStdout()).Encode(result)
}
