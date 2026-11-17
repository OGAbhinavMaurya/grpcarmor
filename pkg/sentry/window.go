package sentry

import (
	"sync"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
)

// DefaultInitialWindowSize is the default flow control window size defined in RFC 7540 (65,535 bytes).
const DefaultInitialWindowSize int64 = 65535

// WindowTracker tracks connection-level and stream-level flow control windows for an HTTP/2 connection.
type WindowTracker struct {
	mu                  sync.RWMutex
	initialStreamWindow int64
	connectionWindow    int64
	streamWindows       map[uint32]int64
	minConnectionWindow int64
}

// NewWindowTracker initializes a new flow control window tracker.
func NewWindowTracker() *WindowTracker {
	return &WindowTracker{
		initialStreamWindow: DefaultInitialWindowSize,
		connectionWindow:    DefaultInitialWindowSize,
		streamWindows:       make(map[uint32]int64),
		minConnectionWindow: DefaultInitialWindowSize,
	}
}

// ConsumeData records transmission of a DATA frame payload, deducting credits from
// both the connection window and the target stream window.
func (wt *WindowTracker) ConsumeData(streamID uint32, length int64) (connWindow int64, streamWindow int64) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	wt.connectionWindow -= length
	if wt.connectionWindow < wt.minConnectionWindow {
		wt.minConnectionWindow = wt.connectionWindow
	}

	curr, exists := wt.streamWindows[streamID]
	if !exists {
		curr = wt.initialStreamWindow
	}
	curr -= length
	wt.streamWindows[streamID] = curr

	return wt.connectionWindow, curr
}

// ReplenishWindow processes a WINDOW_UPDATE frame, adding credits to either the
// connection window (streamID == 0) or an individual stream window (streamID > 0).
func (wt *WindowTracker) ReplenishWindow(streamID uint32, increment int64) (newWindow int64) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	if streamID == 0 {
		wt.connectionWindow += increment
		return wt.connectionWindow
	}

	curr, exists := wt.streamWindows[streamID]
	if !exists {
		curr = wt.initialStreamWindow
	}
	curr += increment
	wt.streamWindows[streamID] = curr
	return curr
}

// UpdateInitialWindow adjusts the initial stream window based on SETTINGS_INITIAL_WINDOW_SIZE.
func (wt *WindowTracker) UpdateInitialWindow(newInitial int64) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	delta := newInitial - wt.initialStreamWindow
	wt.initialStreamWindow = newInitial
	for id, win := range wt.streamWindows {
		wt.streamWindows[id] = win + delta
	}
}

// GetConnectionWindow returns the current connection-level window credit balance.
func (wt *WindowTracker) GetConnectionWindow() int64 {
	wt.mu.RLock()
	defer wt.mu.RUnlock()
	return wt.connectionWindow
}

// GetStreamWindow returns credit balance for a stream.
func (wt *WindowTracker) GetStreamWindow(streamID uint32) int64 {
	wt.mu.RLock()
	defer wt.mu.RUnlock()
	if win, exists := wt.streamWindows[streamID]; exists {
		return win
	}
	return wt.initialStreamWindow
}

// CloseStream removes tracking for a closed stream.
func (wt *WindowTracker) CloseStream(streamID uint32) {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	delete(wt.streamWindows, streamID)
}

// ProcessFrame inspects an incoming frame and updates windows appropriately.
func (wt *WindowTracker) ProcessFrame(f *h2frame.Frame) {
	switch f.Header.Type {
	case h2frame.TypeData:
		wt.ConsumeData(f.Header.StreamID, int64(len(f.Payload)))
	case h2frame.TypeWindowUpdate:
		inc, err := h2frame.ParseWindowUpdate(f)
		if err == nil {
			wt.ReplenishWindow(f.Header.StreamID, int64(inc))
		}
	}
}
