package cmd

import (
	"bytes"
	"testing"
)

func TestResticExportRejectsUnknownFields(t *testing.T) {
	command := newResticExportCommand()
	command.SetArgs([]string{"--mode", "inspect"})
	command.SetIn(bytes.NewBufferString("{\"version\":1,\"shell\":\"synthetic\"}\n"))

	if err := command.Execute(); err == nil {
		t.Fatal("accepted unknown export request field")
	}
}

func TestResticExportRequiresAnExplicitMode(t *testing.T) {
	command := newResticExportCommand()
	command.SetIn(bytes.NewBufferString("{}\n"))

	if err := command.Execute(); err == nil {
		t.Fatal("accepted missing export mode")
	}
}
