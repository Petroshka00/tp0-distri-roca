package safe_socket

import (
	"encoding/binary"
	"io"
)

const HeaderSize = 4

func SendAll(socket io.Writer, data []byte) error {
	alreadyWritten := 0
	for alreadyWritten < len(data) {
		n, err := socket.Write(data[alreadyWritten:])
		if err != nil {
			return err
		}
		alreadyWritten += n
	}
	return nil
}

func SendMsg(socket io.Writer, payload []byte) error {
	dataLength := len(payload)
	message := make([]byte, HeaderSize+dataLength)
	binary.BigEndian.PutUint32(message[:HeaderSize], uint32(dataLength))
	copy(message[HeaderSize:], payload)
	return SendAll(socket, message)
}

func RecvAll(socket io.Reader, amount int) ([]byte, error) {
	return ReadAmount(socket, amount)
}

func RecvMsg(socket io.Reader) ([]byte, error) {
	header, err := ReadAmount(socket, HeaderSize)
	if err != nil {
		return nil, err
	}

	dataLength := binary.BigEndian.Uint32(header)
	if dataLength == 0 {
		return []byte{}, nil
	}

	return ReadAmount(socket, int(dataLength))
}

func ReadAmount(socket io.Reader, amount int) ([]byte, error) {
	data := make([]byte, amount)
	readTotalData := 0

	for readTotalData < amount {
		n, err := socket.Read(data[readTotalData:])
		readTotalData += n
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}
