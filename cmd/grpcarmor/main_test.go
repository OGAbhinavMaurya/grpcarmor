package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestMain_Usage(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	if !strings.Contains(out, "proxy") || !strings.Contains(out, "doctor") {
		t.Errorf("expected usage to contain proxy and doctor, got: %s", out)
	}
}

func TestRunDoctor_CLI(t *testing.T) {
	// Test doctor against unreachable address (should gracefully return report without crashing)
	err := runDoctor([]string{"--target", "127.0.0.1:59996", "--timeout", "100ms"})
	if err != nil {
		t.Errorf("expected runDoctor to handle unreachable target cleanly, got: %v", err)
	}
}

func TestVersionOutput(t *testing.T) {
	if version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", version)
	}
}
