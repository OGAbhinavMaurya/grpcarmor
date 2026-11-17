package h2frame

import (
	"bytes"
	"testing"
)

func TestFrame_RoundTrip(t *testing.T) {
	data := []byte("hello grpc http/2 world")
	df := NewDataFrame(1, data, true)

	var buf bytes.Buffer
	if err := df.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if decoded.Header.Type != TypeData {
		t.Errorf("expected TypeData, got %d", decoded.Header.Type)
	}
	if decoded.Header.StreamID != 1 {
		t.Errorf("expected StreamID 1, got %d", decoded.Header.StreamID)
	}
	if decoded.Header.Flags != FlagEndStream {
		t.Errorf("expected FlagEndStream, got %d", decoded.Header.Flags)
	}
	if string(decoded.Payload) != string(data) {
		t.Errorf("expected payload %q, got %q", string(data), string(decoded.Payload))
	}
}

func TestWindowUpdate_Parse(t *testing.T) {
	wf := NewWindowUpdateFrame(0, 65535)

	inc, err := ParseWindowUpdate(wf)
	if err != nil {
		t.Fatalf("ParseWindowUpdate failed: %v", err)
	}
	if inc != 65535 {
		t.Errorf("expected increment 65535, got %d", inc)
	}

	// Invalid zero increment test
	zeroWf := NewWindowUpdateFrame(0, 0)
	_, err = ParseWindowUpdate(zeroWf)
	if err == nil {
		t.Errorf("expected error for zero increment")
	}
}

func TestSettingsFrame(t *testing.T) {
	sf := NewSettingsFrame(false, map[uint16]uint32{
		SettingInitialWindowSize: 1048576,
	})

	if sf.Header.Length != 6 {
		t.Errorf("expected settings length 6, got %d", sf.Header.Length)
	}

	ack := NewSettingsFrame(true, nil)
	if ack.Header.Flags != FlagAck || ack.Header.Length != 0 {
		t.Errorf("expected ack settings")
	}
}
