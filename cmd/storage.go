package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/chieftools/backupchief-agent/storagehelper"
	"github.com/spf13/cobra"
)

const maxStorageRequestBytes = 1 << 20

func newStorageCommand() *cobra.Command {
	var state string
	command := &cobra.Command{
		Use:    "storage",
		Short:  "Execute one bounded storage request",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			reader := bufio.NewReaderSize(io.LimitReader(command.InOrStdin(), maxStorageRequestBytes), maxStorageRequestBytes)
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return errors.New("cannot read storage request")
			}
			if len(line) >= maxStorageRequestBytes {
				return errors.New("storage request exceeded its limit")
			}

			var request storagehelper.Request
			decoder := json.NewDecoder(bytes.NewReader(line))
			decoder.DisallowUnknownFields()
			if err = decoder.Decode(&request); err != nil {
				return errors.New("invalid storage request")
			}

			return json.NewEncoder(command.OutOrStdout()).Encode(storagehelper.Run(command.Context(), state, request))
		},
	}
	command.Flags().StringVar(&state, "state", "/var/lib/backupchief", "Private execution state directory")
	return command
}
