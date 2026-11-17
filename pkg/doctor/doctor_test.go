package doctor

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
)

func TestDoctor_Probe_MockServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read client preface (24 bytes)
		prefaceBuf := make([]byte, 24)
		_, _ = io.ReadFull(conn, prefaceBuf)

		// Read client SETTINGS
		_, _ = h2frame.ReadFrame(conn)

		// Send server SETTINGS with 65,535 initial window size
		serverSettings := h2frame.NewSettingsFrame(false, map[uint16]uint32{
			h2frame.SettingInitialWindowSize: 65535,
		})
		_ = serverSettings.Encode(conn)

		// Read SETTINGS ACK
		_, _ = h2frame.ReadFrame(conn)
	}()

	prober := New(Config{
		TargetAddr: ln.Addr().String(),
		Timeout:    1 * time.Second,
	})

	report, err := prober.Probe()
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	if report.Status != "VULNERABLE" {
		t.Errorf("expected VULNERABLE for 64KB default window, got %s", report.Status)
	}
	if report.InitialWindowSize != 65535 {
		t.Errorf("expected 65535 initial window, got %d", report.InitialWindowSize)
	}
}

func TestDoctor_Unreachable(t *testing.T) {
	prober := New(Config{
		TargetAddr: "127.0.0.1:59997",
		Timeout:    100 * time.Millisecond,
	})
	report, err := prober.Probe()
	if err == nil {
		t.Errorf("expected error for unreachable target")
	}
	if report.Status != "UNREACHABLE" {
		t.Errorf("expected UNREACHABLE status")
	}
}

func TestSimulateReport(t *testing.T) {
	rep := SimulateReport("localhost:50051", true, 65535)
	if rep.Status != "VULNERABLE" {
		t.Errorf("expected VULNERABLE")
	}

	repClean := SimulateReport("localhost:50051", false, 4194304)
	if repClean.Status != "PROTECTED" {
		t.Errorf("expected PROTECTED")
	}
}
