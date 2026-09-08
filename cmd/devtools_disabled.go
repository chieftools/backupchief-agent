//go:build !devtools

package cmd

import "github.com/spf13/cobra"

func addDevelopmentCommands(_ *cobra.Command, _ string) {}
