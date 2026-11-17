package h2frame

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// HTTP/2 Frame Types
const (
	TypeData         byte = 0x0
	TypeHeaders      byte = 0x1
	TypePriority     byte = 0x2
	TypeRSTStream    byte = 0x3
	TypeSettings     byte = 0x4
	TypePushPromise  byte = 0x5
	TypePing         byte = 0x6
	TypeGoAway       byte = 0x7
	TypeWindowUpdate byte = 0x8
	TypeContinuation byte = 0x9
)

// HTTP/2 Frame Flags
const (
	FlagEndStream  byte = 0x1
	FlagEndHeaders byte = 0x4
	FlagPadded     byte = 0x8
	FlagPriority   byte = 0x20
	FlagAck        byte = 0x1
)

// ClientPreface is the 24-byte magic connection preface in HTTP/2.
const ClientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// FrameHeader represents the standard 9-byte HTTP/2 frame header.
type FrameHeader struct {
	Length   uint32 // 24-bit integer
	Type     byte
	Flags    byte
	StreamID uint32 // 31-bit integer
}

// Frame represents a complete HTTP/2 frame with its header and payload.
type Frame struct {
	Header  FrameHeader
	Payload []byte
}

// ReadFrame reads a single 9-byte header and payload from r.
func ReadFrame(r io.Reader) (*Frame, error) {
	buf := make([]byte, 9)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	length := uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])
	frameType := buf[3]
	flags := buf[4]
	streamID := binary.BigEndian.Uint32(buf[5:9]) & 0x7FFFFFFF

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read frame payload: %w", err)
		}
	}

	return &Frame{
		Header: FrameHeader{
			Length:   length,
			Type:     frameType,
			Flags:    flags,
			StreamID: streamID,
		},
		Payload: payload,
	}, nil
}

// Encode serializes the frame into w.
func (f *Frame) Encode(w io.Writer) error {
	buf := make([]byte, 9+len(f.Payload))
	buf[0] = byte(len(f.Payload) >> 16)
	buf[1] = byte(len(f.Payload) >> 8)
	buf[2] = byte(len(f.Payload))
	buf[3] = f.Header.Type
	buf[4] = f.Header.Flags
	binary.BigEndian.PutUint32(buf[5:9], f.Header.StreamID&0x7FFFFFFF)

	if len(f.Payload) > 0 {
		copy(buf[9:], f.Payload)
	}

	_, err := w.Write(buf)
	return err
}

// Bytes returns the byte representation of the complete frame.
func (f *Frame) Bytes() []byte {
	var buf bytes.Buffer
	_ = f.Encode(&buf)
	return buf.Bytes()
}
