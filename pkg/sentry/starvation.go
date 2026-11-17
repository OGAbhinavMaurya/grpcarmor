package sentry

import (
	"fmt"
	"sync"
	"time"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
)

// StarvationIncident records a period where connection-level window credits hit zero,
// starving concurrent streams.
type StarvationIncident struct {
	StartTime          time.Time     `json:"start_time"`
	Duration           time.Duration `json:"duration"`
	DrainingStreamID   uint32        `json:"draining_stream_id"`
	StalledStreamCount int           `json:"stalled_stream_count"`
	MinWindowReached   int64         `json:"min_window_reached"`
	BytesDrained       int64         `json:"bytes_drained"`
}

// StarvationDetector analyzes frame streams to detect and attribute HTTP/2 flow control starvation.
type StarvationDetector struct {
	mu                      sync.RWMutex
	tracker                 *WindowTracker
	inStarvation            bool
	starvationStart         time.Time
	drainingStreamID        uint32
	drainingStreamBytes     int64
	activeStreams           map[uint32]bool
	incidents               []StarvationIncident
	totalStarvationDuration time.Duration
	eventsTotal             uint64
}

// NewStarvationDetector creates a new starvation sentry.
func NewStarvationDetector() *StarvationDetector {
	return &StarvationDetector{
		tracker:       NewWindowTracker(),
		activeStreams: make(map[uint32]bool),
	}
}

// InspectOutgoingFrame observes an outgoing frame, tracks window credits, and
// checks if the frame drained the connection window into starvation.
func (sd *StarvationDetector) InspectOutgoingFrame(f *h2frame.Frame) *StarvationIncident {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	streamID := f.Header.StreamID

	switch f.Header.Type {
	case h2frame.TypeHeaders:
		sd.activeStreams[streamID] = true
		if f.Header.Flags&h2frame.FlagEndStream != 0 {
			delete(sd.activeStreams, streamID)
			sd.tracker.CloseStream(streamID)
		}

	case h2frame.TypeData:
		payloadLen := int64(len(f.Payload))
		connWin, _ := sd.tracker.ConsumeData(streamID, payloadLen)

		if f.Header.Flags&h2frame.FlagEndStream != 0 {
			delete(sd.activeStreams, streamID)
			sd.tracker.CloseStream(streamID)
		}

		// Check if connection window is drained (<= 0)
		if connWin <= 0 {
			if !sd.inStarvation {
				sd.inStarvation = true
				sd.starvationStart = time.Now()
				sd.drainingStreamID = streamID
				sd.drainingStreamBytes = payloadLen
				sd.eventsTotal++
			} else {
				if streamID == sd.drainingStreamID {
					sd.drainingStreamBytes += payloadLen
				}
			}
		}

	case h2frame.TypeRSTStream:
		delete(sd.activeStreams, streamID)
		sd.tracker.CloseStream(streamID)
	}

	return nil
}

// InspectIncomingFrame observes an incoming frame (e.g. WINDOW_UPDATE from receiver)
// to resolve ongoing starvation.
func (sd *StarvationDetector) InspectIncomingFrame(f *h2frame.Frame) *StarvationIncident {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if f.Header.Type == h2frame.TypeWindowUpdate {
		inc, err := h2frame.ParseWindowUpdate(f)
		if err == nil {
			newWin := sd.tracker.ReplenishWindow(f.Header.StreamID, int64(inc))
			// If connection window was in starvation and is now positive
			if f.Header.StreamID == 0 && sd.inStarvation && newWin > 0 {
				duration := time.Since(sd.starvationStart)
				stalledCount := len(sd.activeStreams)
				if stalledCount > 1 {
					stalledCount-- // exclude draining stream
				}

				incident := StarvationIncident{
					StartTime:          sd.starvationStart,
					Duration:           duration,
					DrainingStreamID:   sd.drainingStreamID,
					StalledStreamCount: stalledCount,
					MinWindowReached:   sd.tracker.minConnectionWindow,
					BytesDrained:       sd.drainingStreamBytes,
				}

				sd.incidents = append(sd.incidents, incident)
				sd.totalStarvationDuration += duration
				sd.inStarvation = false

				return &incident
			}
		}
	}

	return nil
}

// IsStarved returns true if connection credits are currently exhausted.
func (sd *StarvationDetector) IsStarved() bool {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return sd.inStarvation
}

// GetTracker returns the underlying WindowTracker.
func (sd *StarvationDetector) GetTracker() *WindowTracker {
	return sd.tracker
}

// Stats returns summary metrics.
func (sd *StarvationDetector) Stats() string {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	return fmt.Sprintf("Starvation Events: %d, Total Stalled Time: %v, Current Conn Window: %d",
		sd.eventsTotal, sd.totalStarvationDuration, sd.tracker.GetConnectionWindow())
}
