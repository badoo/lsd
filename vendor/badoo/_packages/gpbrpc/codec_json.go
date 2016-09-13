package gpbrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/gogo/protobuf/proto"
)

const (
	JSON_MaxMessageLength = 1024 * 1024 // TODO: make this configurable
)

var (
	JsonCodec = jsonCodec{}
)

type jsonCodec struct {
}

func (c *jsonCodec) ReadRequest(p Protocol, conn net.Conn) (uint32, proto.Message, int, ConnStatus, error) {
	_, line, readln, status, err := ReadJsonPacket(conn)
	if err != nil {
		return 0, nil, readln, status, err
	}

	r := bytes.IndexAny(line, " ")
	if r == -1 {
		return 0, nil, readln, ConnOK, fmt.Errorf("parse error: need message name")
	}

	message_name := "request_" + strings.ToLower(string(line[:r]))

	msgid := MapRequestNameToId(p, message_name)
	msg := p.GetRequestMsg(msgid)
	if msg == nil {
		return 0, nil, readln, ConnOK, fmt.Errorf("parse error: unknown message id = %d", msgid)
	}
	err = json.Unmarshal(line[r+1:], msg)
	if err != nil {
		return 0, nil, readln, ConnOK, fmt.Errorf("parse error: message %s, error: %s", message_name, err)
	}
	return msgid, msg, readln, status, nil
}

func (c *jsonCodec) WriteResponse(p Protocol, conn net.Conn, msg proto.Message) (int, error) {
	msg_name := JsonStripMsgname(MapResponseIdToName(p, p.GetResponseMsgid(msg)))
	return WriteJsonPacket(conn, msg_name, msg)
}

// ---------------------------------------------------------------------------------------------------------------
// helper functions

func ReadJsonPacket(conn net.Conn) (uint32, []byte, int, ConnStatus, error) {
	line := make([]byte, JSON_MaxMessageLength)

	read_bytes, err := readLine(conn, line)
	if err != nil {
		if read_bytes == 0 {
			return 0, nil, read_bytes, ConnEOF, fmt.Errorf("read error: %s", err)
		}
		if read_bytes == len(line) { // FIXME: this one is not really needed, readLine() check this cond
			return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: %s", err)
		}
		return 0, nil, read_bytes, ConnIOError, fmt.Errorf("read error: %s", err)
	}

	return 0, line[:read_bytes], read_bytes, ConnOK, nil
}

func WriteJsonPacket(conn net.Conn, msgname string, msg proto.Message) (int, error) {
	var body []byte
	var err error

	if msg != nil {
		body, err = json.Marshal(msg)
		if err != nil {
			return 0, fmt.Errorf("encode error: %s", err)
		}
	}

	packet := bytes.NewBufferString(msgname)

	packet.Write([]byte(" "))
	packet.Write(body)
	packet.Write([]byte("\r\n"))

	written_bytes, err := writeAll(conn, packet.Bytes())
	if err != nil || written_bytes != len(packet.Bytes()) {
		if err != nil {
			return written_bytes, fmt.Errorf("write error: %s", err)
		}
		return written_bytes, fmt.Errorf("write error: connection unexpectedly closed, err: %s", err)
	}

	return written_bytes, nil
}

// used by json and Client, need to move to proper spot or generate it
func JsonStripMsgname(name string) string {
	if name[:len("response_")] == "response_" {
		return name[len("response_"):]
	} else if name[:len("request_")] == "request_" {
		return name[len("request_"):]
	} else {
		panic("oops, " + name)
	}
}
