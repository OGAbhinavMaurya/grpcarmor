package h2frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// HTTP/2 Setting Identifiers
const (
	SettingHeaderTableSize      uint16 = 0x1
	SettingEnablePush           uint16 = 0x2
	SettingMaxConcurrentStreams uint16 = 0x3
	SettingInitialWindowSize    uint16 = 0x4
	SettingMaxFrameSize         uint16 = 0x5
	SettingMaxHeaderListSize    uint16 = 0x6
)

// NewWindowUpdateFrame creates a WINDOW_UPDATE frame.
// If streamID is 0, it applies to the connection-level window.
func NewWindowUpdateFrame(streamID uint32, increment uint32) *Frame {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, increment&0x7FFFFFFF)
	return &Frame{
		Header: FrameHeader{
			Length:   4,
			Type:     TypeWindowUpdate,
			Flags:    0,
			StreamID: streamID,
		},
		Payload: payload,
	}
}

// ParseWindowUpdate parses the 31-bit window size increment from a WINDOW_UPDATE frame.
func ParseWindowUpdate(f *Frame) (uint32, error) {
	if f.Header.Type != TypeWindowUpdate {
		return 0, fmt.Errorf("not a WINDOW_UPDATE frame: type 0x%x", f.Header.Type)
	}
	if len(f.Payload) != 4 {
		return 0, fmt.Errorf("invalid WINDOW_UPDATE payload length: %d", len(f.Payload))
	}
	inc := binary.BigEndian.Uint32(f.Payload) & 0x7FFFFFFF
	if inc == 0 {
		return 0, errors.New("WINDOW_UPDATE increment must not be zero")
	}
	return inc, nil
}

// NewSettingsFrame creates a SETTINGS frame.
func NewSettingsFrame(ack bool, settings map[uint16]uint32) *Frame {
	var flags byte
	if ack {
		flags = FlagAck
		return &Frame{
			Header: FrameHeader{
				Length:   0,
				Type:     TypeSettings,
				Flags:    flags,
				StreamID: 0,
			},
			Payload: nil,
		}
	}

	payload := make([]byte, len(settings)*6)
	offset := 0
	for id, val := range settings {
		binary.BigEndian.PutUint16(payload[offset:offset+2], id)
		binary.BigEndian.PutUint32(payload[offset+2:offset+6], val)
		offset += 6
	}

	return &Frame{
		Header: FrameHeader{
			Length:   uint32(len(payload)),
			Type:     TypeSettings,
			Flags:    0,
			StreamID: 0,
		},
		Payload: payload,
	}
}

// NewPingFrame creates a PING frame with an 8-byte opaque payload.
func NewPingFrame(opaque [8]byte, ack bool) *Frame {
	var flags byte
	if ack {
		flags = FlagAck
	}
	payload := make([]byte, 8)
	copy(payload, opaque[:])

	return &Frame{
		Header: FrameHeader{
			Length:   8,
			Type:     TypePing,
			Flags:    flags,
			StreamID: 0,
		},
		Payload: payload,
	}
}

// NewDataFrame creates a DATA frame.
func NewDataFrame(streamID uint32, data []byte, endStream bool) *Frame {
	var flags byte
	if endStream {
		flags = FlagEndStream
	}
	return &Frame{
		Header: FrameHeader{
			Length:   uint32(len(data)),
			Type:     TypeData,
			Flags:    flags,
			StreamID: streamID,
		},
		Payload: data,
	}
}

// NewRSTStreamFrame creates a RST_STREAM frame.
func NewRSTStreamFrame(streamID uint32, errorCode uint32) *Frame {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, errorCode)
	return &Frame{
		Header: FrameHeader{
			Length:   4,
			Type:     TypeRSTStream,
			Flags:    0,
			StreamID: streamID,
		},
		Payload: payload,
	}
}
