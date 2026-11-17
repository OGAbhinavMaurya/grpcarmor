package pool

import (
	"net"
	"testing"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/sentry"
)

func TestAutotuner_PreemptiveUpdate(t *testing.T) {
	at := NewAutotuner(32768, 65535)
	tracker := sentry.NewWindowTracker()

	// Initially 65,535 (above 32,768 threshold): no tune frame
	frame := at.CheckAndTune(tracker)
	if frame != nil {
		t.Errorf("expected nil frame when window is full")
	}

	// Consume 40,000 bytes (window drops to 25,535 <= 32,768): should trigger tune!
	tracker.ConsumeData(1, 40000)
	frame = at.CheckAndTune(tracker)
	if frame == nil {
		t.Fatalf("expected WINDOW_UPDATE frame injected by autotuner")
	}

	if frame.Header.StreamID != 0 {
		t.Errorf("expected connection-level window update on stream 0, got %d", frame.Header.StreamID)
	}

	injected, bytes := at.Metrics()
	if injected != 1 || bytes != 65535 {
		t.Errorf("expected metrics (1, 65535), got (%d, %d)", injected, bytes)
	}
}

func TestMultiplexer_Segregation(t *testing.T) {
	m := NewMultiplexer(Config{
		TargetAddr:      "127.0.0.1:50051",
		SubchannelCount: 4, // 2 Unary, 2 Bulk
		AutotuneWindow:  true,
	})

	// Use pipe for mock subchannel connections
	m.SetDialer(func(network, address string) (net.Conn, error) {
		c1, _ := net.Pipe()
		return c1, nil
	})

	if err := m.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	defer m.Close()

	// Assign stream 1 (Unary)
	s1 := m.AssignSubchannel(1, false)
	if s1.Role != RoleUnary {
		t.Errorf("expected stream 1 to be assigned to UNARY subchannel, got %s", s1.Role)
	}

	// Assign stream 3 (Bulk)
	s3 := m.AssignSubchannel(3, true)
	if s3.Role != RoleBulk {
		t.Errorf("expected stream 3 to be assigned to BULK subchannel, got %s", s3.Role)
	}

	metrics := m.GetMetrics()
	if metrics.UnaryStreamsRouted != 1 || metrics.BulkStreamsRouted != 1 {
		t.Errorf("unexpected routing metrics: %+v", metrics)
	}

	m.ReleaseStream(1)
	m.ReleaseStream(3)
}
