package pool

import (
	"sync/atomic"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
	"github.com/OGAbhinavMaurya/grpcarmor/pkg/sentry"
)

// DefaultAutotuneThreshold is 32 KB (50% of default 64 KB window).
const DefaultAutotuneThreshold int64 = 32768

// DefaultAutotuneIncrement is 65,535 bytes.
const DefaultAutotuneIncrement uint32 = 65535

// Autotuner proactively emits speculative WINDOW_UPDATE frames before connection
// credits fall to zero, eliminating flow-control stalls on fast links.
type Autotuner struct {
	threshold             int64
	increment             uint32
	windowUpdatesInjected uint64
	bytesGranted          uint64
}

// NewAutotuner creates a new window autotuner.
func NewAutotuner(threshold int64, increment uint32) *Autotuner {
	if threshold <= 0 {
		threshold = DefaultAutotuneThreshold
	}
	if increment == 0 {
		increment = DefaultAutotuneIncrement
	}
	return &Autotuner{
		threshold: threshold,
		increment: increment,
	}
}

// CheckAndTune checks if current connection window is below threshold.
// If so, it returns a preemptive WINDOW_UPDATE frame to inject towards the sender.
func (at *Autotuner) CheckAndTune(tracker *sentry.WindowTracker) *h2frame.Frame {
	connWin := tracker.GetConnectionWindow()
	if connWin <= at.threshold {
		atomic.AddUint64(&at.windowUpdatesInjected, 1)
		atomic.AddUint64(&at.bytesGranted, uint64(at.increment))

		// Replenish internal tracker as well
		tracker.ReplenishWindow(0, int64(at.increment))

		// Construct WINDOW_UPDATE on stream 0
		return h2frame.NewWindowUpdateFrame(0, at.increment)
	}
	return nil
}

// Metrics returns tuning stats.
func (at *Autotuner) Metrics() (injected uint64, bytes uint64) {
	return atomic.LoadUint64(&at.windowUpdatesInjected), atomic.LoadUint64(&at.bytesGranted)
}
