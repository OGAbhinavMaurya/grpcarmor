package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OGAbhinavMaurya/grpcarmor/pkg/doctor"
	"github.com/OGAbhinavMaurya/grpcarmor/pkg/h2frame"
	"github.com/OGAbhinavMaurya/grpcarmor/pkg/pool"
)

const version = "1.0.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	var err error
	switch command {
	case "proxy":
		err = runProxy(args)
	case "doctor":
		err = runDoctor(args)
	case "version", "-v", "--version":
		fmt.Printf("grpcarmor version %s\n", version)
		return
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	usage := `grpcarmor - gRPC & HTTP/2 stream starvation sentry and dynamic subchannel multiplexer

Usage:
  grpcarmor <command> [options]

Commands:
  proxy      Run transparent HTTP/2 subchannel multiplexer and window autotuner proxy
  doctor     Diagnose target gRPC server for flow-control window starvation vulnerabilities
  version    Show grpcarmor version
  help       Show this help message

Proxy Options:
  --listen          Local bind address [default: :50052]
  --target          Upstream gRPC server address [default: 127.0.0.1:50051]
  --subchannels     Number of parallel TCP subchannels to maintain [default: 4]
  --autotune-window Enable preemptive WINDOW_UPDATE injection [default: true]
  --json            Output operational metrics in JSON upon exit

Doctor Options:
  --target          Target gRPC server address [default: 127.0.0.1:50051]
  --timeout         Probe timeout duration [default: 5s]
  --payload-size    Simulated burst payload size in bytes [default: 1048576 (1MB)]
  --json            Output results in JSON format

Examples:
  grpcarmor proxy --listen :50052 --target 127.0.0.1:50051 --subchannels 4
  grpcarmor doctor --target 127.0.0.1:50051
  grpcarmor doctor --target 127.0.0.1:50051 --json
`
	fmt.Print(usage)
}

func runProxy(args []string) error {
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	listen := fs.String("listen", ":50052", "Local listen address")
	target := fs.String("target", "127.0.0.1:50051", "Upstream gRPC target")
	subchannelCount := fs.Int("subchannels", 4, "Number of subchannels")
	autotune := fs.Bool("autotune-window", true, "Enable window autotuner")
	jsonOutput := fs.Bool("json", false, "Output metrics in JSON upon exit")

	if err := fs.Parse(args); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *listen, err)
	}
	defer ln.Close()

	multiplexer := pool.NewMultiplexer(pool.Config{
		TargetAddr:      *target,
		SubchannelCount: *subchannelCount,
		AutotuneWindow:  *autotune,
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("grpcarmor proxy listening on %s -> multiplexing across %d subchannels to %s (autotune=%v)\n",
		*listen, *subchannelCount, *target, *autotune)

	go func() {
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleProxyClient(clientConn, *target, multiplexer)
		}
	}()

	<-sigChan
	fmt.Fprintf(os.Stderr, "\nReceived shutdown signal, stopping grpcarmor proxy...\n")
	_ = multiplexer.Close()

	metrics := multiplexer.GetMetrics()
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(metrics)
	}

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Proxy Metrics:\n")
	fmt.Printf("  Unary Streams Routed:    %d\n", metrics.UnaryStreamsRouted)
	fmt.Printf("  Bulk Streams Routed:     %d\n", metrics.BulkStreamsRouted)
	fmt.Printf("  Starvations Averted:     %d\n", metrics.StarvationsAverted)
	fmt.Printf("  Active Subchannels:      %d\n", metrics.ActiveSubchannels)
	fmt.Println("--------------------------------------------------------------------------------")

	return nil
}

func handleProxyClient(client net.Conn, target string, m *pool.Multiplexer) {
	defer client.Close()

	backend, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer backend.Close()

	// Read preface
	prefaceBuf := make([]byte, len(h2frame.ClientPreface))
	if _, err := io.ReadFull(client, prefaceBuf); err != nil {
		return
	}
	if _, err := backend.Write(prefaceBuf); err != nil {
		return
	}

	// Bi-directional frame forwarding with sentry inspection
	done := make(chan struct{})

	// Backend -> Client
	go func() {
		defer close(done)
		for {
			f, err := h2frame.ReadFrame(backend)
			if err != nil {
				return
			}
			if err := f.Encode(client); err != nil {
				return
			}
		}
	}()

	// Client -> Backend
	go func() {
		for {
			f, err := h2frame.ReadFrame(client)
			if err != nil {
				return
			}
			if err := f.Encode(backend); err != nil {
				return
			}
		}
	}()

	<-done
}

func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	target := fs.String("target", "127.0.0.1:50051", "Target gRPC server")
	timeout := fs.Duration("timeout", 5*time.Second, "Probe timeout")
	payloadSize := fs.Int("payload-size", 1048576, "Payload burst size in bytes")
	jsonOutput := fs.Bool("json", false, "Output in JSON format")

	if err := fs.Parse(args); err != nil {
		return err
	}

	prober := doctor.New(doctor.Config{
		TargetAddr:  *target,
		Timeout:     *timeout,
		PayloadSize: *payloadSize,
	})

	report, _ := prober.Probe()

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}

	fmt.Println("================================================================================")
	fmt.Println("                            GRPCARMOR DOCTOR REPORT                             ")
	fmt.Println("================================================================================")
	fmt.Printf(" Target Address:           %s\n", report.TargetAddr)
	fmt.Printf(" Status:                   [%s]\n", report.Status)
	fmt.Printf(" Initial Window Size:      %d bytes (%.1f KB)\n",
		report.InitialWindowSize, float64(report.InitialWindowSize)/1024.0)
	fmt.Printf(" Flow-Control Depletion:   %v\n", report.WindowDepletionRisk)
	fmt.Printf(" Max Burst Without Stall:  %d bytes\n", report.MaxBurstWithoutStall)
	fmt.Printf(" Summary:                  %s\n", report.Summary)
	fmt.Printf(" Recommendation:           %s\n", report.Recommendation)
	fmt.Println("--------------------------------------------------------------------------------")
	if len(report.DiagnosticLog) > 0 {
		fmt.Println(" Diagnostic Trace:")
		for _, log := range report.DiagnosticLog {
			fmt.Printf("   * %s\n", log)
		}
	}
	fmt.Println("================================================================================")

	return nil
}
