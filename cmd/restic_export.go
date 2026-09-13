package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

func newResticExportCommand() *cobra.Command {
	var state string
	var mode string
	var allowLocal bool

	command := &cobra.Command{
		Use:    "restic-export",
		Short:  "Stream one bounded snapshot export",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return runResticExportCommand(command, state, mode, allowLocal)
		},
	}
	command.Flags().StringVar(&state, "state", "/var/lib/backupchief", "Private execution state directory")
	command.Flags().StringVar(&mode, "mode", "", "Export mode")
	command.Flags().BoolVar(&allowLocal, "development-local", false, "Permit a local repository for development tests")

	return command
}

func runResticExportCommand(command *cobra.Command, state, mode string, allowLocal bool) error {
	if mode != "inspect" && mode != "archive" {
		return errors.New("invalid snapshot export mode")
	}

	signalContext, cancelSignals := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
	defer cancelSignals()

	reader := bufio.NewReaderSize(io.LimitReader(command.InOrStdin(), maxResticRequestBytes), maxResticRequestBytes)
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return errors.New("cannot read snapshot export request")
	}
	if len(line) >= maxResticRequestBytes {
		return errors.New("snapshot export request exceeded its limit")
	}

	var request restic.ExportRequest
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&request); err != nil {
		return errors.New("invalid snapshot export request")
	}

	runner := restic.Runner{State: state, AllowLocal: allowLocal}
	if mode == "archive" {
		return runner.StreamExport(signalContext, request, command.OutOrStdout())
	}

	inspection, err := runner.InspectExport(signalContext, request)
	if err != nil {
		return err
	}

	return json.NewEncoder(command.OutOrStdout()).Encode(inspection)
}
