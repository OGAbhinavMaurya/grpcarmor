package sentry

import (
	"testing"
	"time"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
)

func TestWindowTracker_DeductionAndReplenish(t *testing.T) {
	wt := NewWindowTracker()

	if wt.GetConnectionWindow() != DefaultInitialWindowSize {
		t.Errorf("expected %d initial connection window, got %d", DefaultInitialWindowSize, wt.GetConnectionWindow())
	}

	// Send 10,000 bytes on stream 1
	connWin, streamWin := wt.ConsumeData(1, 10000)
	if connWin != DefaultInitialWindowSize-10000 {
		t.Errorf("expected connWin %d, got %d", DefaultInitialWindowSize-10000, connWin)
	}
	if streamWin != DefaultInitialWindowSize-10000 {
		t.Errorf("expected streamWin %d, got %d", DefaultInitialWindowSize-10000, streamWin)
	}

	// Replenish connection window with 5,000 bytes
	newConnWin := wt.ReplenishWindow(0, 5000)
	if newConnWin != DefaultInitialWindowSize-5000 {
		t.Errorf("expected newConnWin %d, got %d", DefaultInitialWindowSize-5000, newConnWin)
	}
}

func TestStarvationDetector_DrainAndRecovery(t *testing.T) {
	detector := NewStarvationDetector()

	// Register 2 streams (Stream 1 = bulk, Stream 3 = unary)
	h1 := &h2frame.Frame{Header: h2frame.FrameHeader{Type: h2frame.TypeHeaders, StreamID: 1}}
	h3 := &h2frame.Frame{Header: h2frame.FrameHeader{Type: h2frame.TypeHeaders, StreamID: 3}}
	detector.InspectOutgoingFrame(h1)
	detector.InspectOutgoingFrame(h3)

	if detector.IsStarved() {
		t.Errorf("expected not starved initially")
	}

	// Stream 1 sends 65,535 bytes (draining window to 0!)
	bulkData := make([]byte, 65535)
	d1 := h2frame.NewDataFrame(1, bulkData, false)
	detector.InspectOutgoingFrame(d1)

	if !detector.IsStarved() {
		t.Fatalf("expected detector to report starvation after draining 65535 bytes")
	}

	time.Sleep(10 * time.Millisecond)

	// Remote server sends WINDOW_UPDATE on stream 0 (connection-level) for 65,535 bytes
	wu := h2frame.NewWindowUpdateFrame(0, 65535)
	incident := detector.InspectIncomingFrame(wu)

	if incident == nil {
		t.Fatalf("expected StarvationIncident returned on recovery")
	}
	if incident.DrainingStreamID != 1 {
		t.Errorf("expected stream 1 attributed as draining stream, got %d", incident.DrainingStreamID)
	}
	if detector.IsStarved() {
		t.Errorf("expected starvation cleared after WINDOW_UPDATE")
	}
	if incident.Duration < 10*time.Millisecond {
		t.Errorf("expected incident duration >= 10ms, got %v", incident.Duration)
	}
}
