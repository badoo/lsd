package gpbrpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/gogo/protobuf/proto"
)

const (
	GPBS_MaxMessageLength = 16 * 1024 * 1024
)

var (
	GpbsCodec  = gpbsCodec{}
	headerPool = sync.Pool{
		New: func() interface{} {
			return &buffer{}
		},
	}
)

type gpbsCodec struct {
}

type buffer struct {
	data [8]byte
}

// ServerCodec functions
func (c *gpbsCodec) ReadRequest(p Protocol, conn net.Conn) (uint32, proto.Message, int, ConnStatus, error) {
	msgid, body, readln, status, err := ReadGpbsPacket(conn)
	if err != nil {
		return 0, nil, readln, status, err
	}

	msg := p.GetRequestMsg(msgid)
	if msg == nil {
		return 0, nil, readln, ConnOK, fmt.Errorf("parse error: unknown message_id: %d", msgid)
	}
	err = proto.Unmarshal(body, msg)
	if err != nil {
		return 0, nil, readln, ConnOK, fmt.Errorf("parse error: message %s, error: %s", MapRequestIdToName(p, msgid), err)
	}
	return msgid, msg, readln, status, nil
}

// FIXME: copy-paste of the above, just message getters are different and it does not return n_bytes_read
//  since exchange is packet based - we can factor this stuff out
// OR completely remove all differences between requests and responses
func (c *gpbsCodec) ReadResponse(p Protocol, conn net.Conn) (uint32, proto.Message, ConnStatus, error) {
	msgid, body, _, status, err := ReadGpbsPacket(conn)
	if err != nil {
		return 0, nil, status, err
	}

	msg := p.GetResponseMsg(msgid)
	if msg == nil {
		return 0, nil, ConnOK, fmt.Errorf("parse error: unknown message_id: %d", msgid)
	}
	err = proto.Unmarshal(body, msg)
	if err != nil {
		return 0, nil, ConnOK, fmt.Errorf("parse error: message %s, error: %s", MapRequestIdToName(p, msgid), err)
	}
	return msgid, msg, status, nil
}

func (c *gpbsCodec) WriteRequest(p Protocol, conn net.Conn, msg proto.Message) error {
	_, err := WriteGpbsPacket(conn, p.GetRequestMsgid(msg), msg)
	return err
}

func (c *gpbsCodec) WriteResponse(p Protocol, conn net.Conn, msg proto.Message) (int, error) {
	return WriteGpbsPacket(conn, p.GetResponseMsgid(msg), msg)
}

// ---------------------------------------------------------------------------------------------------------------
// helper functions

// FIXME: little mess: ReadGpbsPacket is about []byte, WriteGpbsPacket is about proto.Message

// Reads a GPBS message from provided io.Reader.
// Returns message id, message data, bytes read from the reader, connection status, error
func ReadGpbsPacket(r io.Reader) (msgId uint32, data []byte, bytesRead int, connStatus ConnStatus, err error) {
	header := headerPool.Get().(*buffer)
	defer headerPool.Put(header)

	read_bytes, err := readAll(r, header.data[:])
	if err != nil || read_bytes != len(header.data) {
		if err != nil {
			if read_bytes == 0 {
				return 0, nil, 0, ConnEOF, fmt.Errorf("read error: %s", err)
			}
			return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: %s", err)
		}
		return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: connection unexpectedly closed")
	}

	length := binary.BigEndian.Uint32(header.data[:4])
	msgid := binary.BigEndian.Uint32(header.data[4:])

	if length < 4 {
		return 0, nil, read_bytes, ConnParseError, fmt.Errorf("parse error: invalid packet length: %d", length)
	}

	if length > GPBS_MaxMessageLength {
		return 0, nil, read_bytes, ConnParseError, fmt.Errorf("parse error: packet is too long: %d (max: %d)", length, GPBS_MaxMessageLength)
	}

	// FIXME: don't need to allocate anything if body is empty (i.e. length == 4), rewrite the code around
	body := make([]byte, length-4)

	if length > 4 {

		read_bytes, err = readAll(r, body)
		if err != nil || read_bytes != len(body) {
			if err != nil {
				return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: %s", err)
			}
			return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: connection unexpectedly closed")
		}
	}

	return msgid, body, read_bytes, ConnOK, nil
}

// Writes a GPBS message to provided io.Writer.
// Returns bytes written to the io.Writer and error if any.
func WriteGpbsPacket(w io.Writer, msgid uint32, msg proto.Message) (int, error) {
	var body []byte
	var err error

	if msg != nil {
		body, err = proto.Marshal(msg)
		if err != nil {
			return 0, fmt.Errorf("encode error: %s", err)
		}
	}

	return WriteRawGpbsPacket(w, msgid, body)
}

func WriteRawGpbsPacket(w io.Writer, msgid uint32, body []byte) (int, error) {

	t := headerPool.Get().(*buffer)
	defer headerPool.Put(t)

	if len(t.data) != 8 {
		panic("could not happen!")
	}

	binary.BigEndian.PutUint32(t.data[:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(t.data[4:], uint32(msgid))

	written_bytes1, err := writeAll(w, t.data[:])
	if err != nil || written_bytes1 != len(t.data) {
		if err != nil {
			return written_bytes1, fmt.Errorf("write error: %s", err)
		}
		return written_bytes1, fmt.Errorf("write error: connection unexpectedly closed")
	}

	written_bytes2, err := writeAll(w, body)
	if err != nil || written_bytes2 != len(body) {
		if err != nil {
			return written_bytes1 + written_bytes2, fmt.Errorf("write error: %s", err)
		}
		return written_bytes1 + written_bytes2, fmt.Errorf("write error: connection unexpectedly closed")
	}

	return written_bytes1 + written_bytes2, nil
}
