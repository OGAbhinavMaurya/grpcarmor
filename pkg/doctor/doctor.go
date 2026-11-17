package doctor

import (
	"fmt"
	"net"
	"time"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
)

// Config configures the starvation doctor diagnostic prober.
type Config struct {
	TargetAddr  string
	Timeout     time.Duration
	PayloadSize int
}

// Report contains diagnostic findings from probing the gRPC target.
type Report struct {
	TargetAddr               string   `json:"target_addr"`
	Status                   string   `json:"status"` // VULNERABLE, PROTECTED, UNREACHABLE
	InitialWindowSize        int64    `json:"initial_window_size"`
	WindowDepletionRisk      bool     `json:"window_depletion_risk"`
	MaxBurstWithoutStall     int64    `json:"max_burst_without_stall"`
	Summary                  string   `json:"summary"`
	Recommendation           string   `json:"recommendation"`
	DiagnosticLog            []string `json:"diagnostic_log"`
}

// Prober tests a gRPC endpoint for HTTP/2 connection-level flow control starvation.
type Prober struct {
	cfg Config
}

// New creates a new Prober instance.
func New(cfg Config) *Prober {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 1048576 // 1MB
	}
	return &Prober{cfg: cfg}
}

// Probe connects to targetAddr, completes HTTP/2 preface, reads SETTINGS,
// and evaluates flow control window vulnerability.
func (p *Prober) Probe() (*Report, error) {
	var logs []string

	conn, err := net.DialTimeout("tcp", p.cfg.TargetAddr, p.cfg.Timeout)
	if err != nil {
		return &Report{
			TargetAddr:     p.cfg.TargetAddr,
			Status:         "UNREACHABLE",
			Summary:        fmt.Sprintf("Failed to connect to %s: %v", p.cfg.TargetAddr, err),
			Recommendation: "Verify that the gRPC server is running and accessible.",
			DiagnosticLog:  []string{fmt.Sprintf("Dial failed: %v", err)},
		}, err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(p.cfg.Timeout))

	// 1. Send HTTP/2 client connection preface
	if _, err := conn.Write([]byte(h2frame.ClientPreface)); err != nil {
		logs = append(logs, fmt.Sprintf("Failed to send client preface: %v", err))
	} else {
		logs = append(logs, "Sent HTTP/2 connection preface (PRI * HTTP/2.0...)")
	}

	// 2. Send initial SETTINGS frame
	sf := h2frame.NewSettingsFrame(false, map[uint16]uint32{
		h2frame.SettingInitialWindowSize: 65535,
	})
	if err := sf.Encode(conn); err != nil {
		logs = append(logs, fmt.Sprintf("Failed to send client SETTINGS: %v", err))
	} else {
		logs = append(logs, "Sent client SETTINGS frame (initial_window_size=65535)")
	}

	// 3. Read server response frames
	var initialWindowSize int64 = 65535
	var receivedSettings bool

	for {
		f, err := h2frame.ReadFrame(conn)
		if err != nil {
			break
		}

		if f.Header.Type == h2frame.TypeSettings {
			receivedSettings = true
			logs = append(logs, "Received server SETTINGS frame")
			// Acknowledge server settings
			ack := h2frame.NewSettingsFrame(true, nil)
			_ = ack.Encode(conn)

			// Parse settings payload
			offset := 0
			for offset+6 <= len(f.Payload) {
				id := uint16(f.Payload[offset])<<8 | uint16(f.Payload[offset+1])
				val := uint32(f.Payload[offset+2])<<24 | uint32(f.Payload[offset+3])<<16 | uint32(f.Payload[offset+4])<<8 | uint32(f.Payload[offset+5])
				if id == h2frame.SettingInitialWindowSize {
					initialWindowSize = int64(val)
					logs = append(logs, fmt.Sprintf("Server configured initial stream window: %d bytes", val))
				}
				offset += 6
			}
			break
		}
	}

	vulnerable := true
	status := "VULNERABLE"
	summary := fmt.Sprintf("Target uses standard HTTP/2 connection window (%d bytes). Bulk streams over %d KB will drain credits to 0 and starve concurrent Unary RPCs.",
		initialWindowSize, initialWindowSize/1024)
	recommendation := "Deploy 'grpcarmor proxy' with subchannel segregation and window autotuning to eliminate tail-latency spikes."

	if !receivedSettings {
		summary = "Server connected but did not respond with HTTP/2 SETTINGS frame."
	} else if initialWindowSize > 1048576 {
		vulnerable = false
		status = "PROTECTED"
		summary = fmt.Sprintf("Target has large initial window (%d bytes). Low risk of immediate flow-control starvation.", initialWindowSize)
		recommendation = "No immediate changes required."
	}

	return &Report{
		TargetAddr:           p.cfg.TargetAddr,
		Status:               status,
		InitialWindowSize:    initialWindowSize,
		WindowDepletionRisk:  vulnerable,
		MaxBurstWithoutStall: initialWindowSize,
		Summary:              summary,
		Recommendation:       recommendation,
		DiagnosticLog:        logs,
	}, nil
}

// SimulateReport produces a diagnostic report for testing without live network.
func SimulateReport(targetAddr string, vulnerable bool, windowSize int64) *Report {
	status := "PROTECTED"
	rec := "No action needed."
	summary := fmt.Sprintf("Initial window size %d bytes provides adequate buffering.", windowSize)

	if vulnerable {
		status = "VULNERABLE"
		summary = fmt.Sprintf("Target connection window (%d bytes) is vulnerable to head-of-line blocking under streaming bursts.", windowSize)
		rec = "Deploy 'grpcarmor proxy' to segregate Unary from Bulk Streaming RPCs."
	}

	return &Report{
		TargetAddr:           targetAddr,
		Status:               status,
		InitialWindowSize:    windowSize,
		WindowDepletionRisk:  vulnerable,
		MaxBurstWithoutStall: windowSize,
		Summary:              summary,
		Recommendation:       rec,
		DiagnosticLog:        []string{"Simulated flow-control diagnostic complete"},
	}
}
