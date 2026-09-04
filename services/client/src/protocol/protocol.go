package protocol

import (
	"encoding/binary"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	HeaderSize = 5

	Connect  byte = 1
	BetBatch byte = 2
	End      byte = 3
	Ack      byte = 4
	Winners  byte = 5
	Error    byte = 6
)

func SendMsg(socket io.Writer, msgType byte, payload []byte) error {
	var header [HeaderSize]byte
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))

	if err := safe_socket.SendAll(socket, header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		return safe_socket.SendAll(socket, payload)
	}
	return nil
}

func RecvMsg(socket io.Reader) (byte, []byte, error) {
	header, err := safe_socket.RecvAll(socket, HeaderSize)
	if err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5])
	if length == 0 {
		return msgType, nil, nil
	}

	payload, err := safe_socket.RecvAll(socket, int(length))
	if err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}
