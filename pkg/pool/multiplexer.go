package pool

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
	"github.com/OGAbhinavMaurya/grpcarmor/pkg/sentry"
)

// SubchannelRole indicates the stream priority assigned to a subchannel.
type SubchannelRole string

const (
	RoleUnary SubchannelRole = "UNARY"
	RoleBulk  SubchannelRole = "BULK"
)

// Subchannel represents an independent physical TCP connection to the backend.
type Subchannel struct {
	ID        int
	Role      SubchannelRole
	Conn      net.Conn
	Tracker   *sentry.WindowTracker
	Detector  *sentry.StarvationDetector
	ActiveRPC int64
}

// Config configures the subchannel multiplexer pool.
type Config struct {
	TargetAddr        string
	SubchannelCount   int
	AutotuneWindow    bool
	AutotuneThreshold int64
}

// Multiplexer manages multiple subchannels and segregates unary from bulk streaming RPCs.
type Multiplexer struct {
	cfg         Config
	mu          sync.RWMutex
	subchannels []*Subchannel
	streamMap   map[uint32]*Subchannel
	autotuner   *Autotuner
	dialer      func(network, address string) (net.Conn, error)
	metrics     Metrics
}

// Metrics tracks pool operations.
type Metrics struct {
	UnaryStreamsRouted  uint64 `json:"unary_streams_routed"`
	BulkStreamsRouted   uint64 `json:"bulk_streams_routed"`
	StarvationsAverted  uint64 `json:"starvations_averted"`
	ActiveSubchannels   int    `json:"active_subchannels"`
}

// NewMultiplexer creates a new subchannel pool.
func NewMultiplexer(cfg Config) *Multiplexer {
	if cfg.SubchannelCount < 2 {
		cfg.SubchannelCount = 2
	}
	return &Multiplexer{
		cfg:       cfg,
		streamMap: make(map[uint32]*Subchannel),
		autotuner: NewAutotuner(cfg.AutotuneThreshold, DefaultAutotuneIncrement),
		dialer:    net.Dial,
	}
}

// SetDialer allows overriding the network dialer (useful for unit testing).
func (m *Multiplexer) SetDialer(d func(network, address string) (net.Conn, error)) {
	m.dialer = d
}

// Initialize dials all subchannels in the pool.
func (m *Multiplexer) Initialize() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := 0; i < m.cfg.SubchannelCount; i++ {
		conn, err := m.dialer("tcp", m.cfg.TargetAddr)
		if err != nil {
			return fmt.Errorf("failed to dial subchannel %d to %s: %w", i, m.cfg.TargetAddr, err)
		}

		// Half unary, half bulk
		role := RoleUnary
		if i%2 == 1 {
			role = RoleBulk
		}

		sub := &Subchannel{
			ID:       i,
			Role:     role,
			Conn:     conn,
			Tracker:  sentry.NewWindowTracker(),
			Detector: sentry.NewStarvationDetector(),
		}
		m.subchannels = append(m.subchannels, sub)
	}

	return nil
}

// AssignSubchannel assigns a stream to an appropriate subchannel based on whether it is bulk or unary.
func (m *Multiplexer) AssignSubchannel(streamID uint32, isBulk bool) *Subchannel {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sub, exists := m.streamMap[streamID]; exists {
		return sub
	}

	targetRole := RoleUnary
	if isBulk {
		targetRole = RoleBulk
		atomic.AddUint64(&m.metrics.BulkStreamsRouted, 1)
	} else {
		atomic.AddUint64(&m.metrics.UnaryStreamsRouted, 1)
	}

	// Find best subchannel matching targetRole with lowest active load
	var best *Subchannel
	for _, sub := range m.subchannels {
		if sub.Role == targetRole {
			if best == nil || atomic.LoadInt64(&sub.ActiveRPC) < atomic.LoadInt64(&best.ActiveRPC) {
				best = sub
			}
		}
	}

	if best == nil && len(m.subchannels) > 0 {
		best = m.subchannels[0]
	}

	if best != nil {
		atomic.AddInt64(&best.ActiveRPC, 1)
		m.streamMap[streamID] = best
	}

	return best
}

// ReleaseStream unbinds a stream on completion.
func (m *Multiplexer) ReleaseStream(streamID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sub, exists := m.streamMap[streamID]; exists {
		atomic.AddInt64(&sub.ActiveRPC, -1)
		delete(m.streamMap, streamID)
	}
}

// CheckAutotune inspects the given subchannel and returns a preemptive WINDOW_UPDATE if needed.
func (m *Multiplexer) CheckAutotune(sub *Subchannel) *h2frame.Frame {
	if !m.cfg.AutotuneWindow || sub == nil {
		return nil
	}
	f := m.autotuner.CheckAndTune(sub.Tracker)
	if f != nil {
		atomic.AddUint64(&m.metrics.StarvationsAverted, 1)
	}
	return f
}

// Close closes all subchannels in the pool.
func (m *Multiplexer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, sub := range m.subchannels {
		if sub.Conn != nil {
			_ = sub.Conn.Close()
		}
	}
	m.subchannels = nil
	return nil
}

// GetMetrics returns snapshot metrics.
func (m *Multiplexer) GetMetrics() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		UnaryStreamsRouted: atomic.LoadUint64(&m.metrics.UnaryStreamsRouted),
		BulkStreamsRouted:  atomic.LoadUint64(&m.metrics.BulkStreamsRouted),
		StarvationsAverted: atomic.LoadUint64(&m.metrics.StarvationsAverted),
		ActiveSubchannels:  len(m.subchannels),
	}
}
