package safe_socket

import (
	"encoding/binary"
	"io"
)

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	alreadyWritten := 0
	for alreadyWritten < len(bytes) {
		n, err := socket.Write(bytes[alreadyWritten:])
		if err != nil {
			return err
		}
		alreadyWritten += n
	}
	return nil
}

func SendMsg(socket io.Writer, bytes []byte) error {
	dataLength := len(bytes)
	message := make([]byte, 4+dataLength)
	binary.BigEndian.PutUint32(message[0:4], uint32(dataLength))
	copy(message[4:], bytes)
	return SendAll(socket, message)
}

func RecvAll(socket io.Reader, amount int) ([]byte, error) {
	return ReadAmount(socket, amount)
}

func RecvMsg(socket io.Reader) ([]byte, error) {
	header, err := ReadAmount(socket, 4)
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
		if err != nil {
			return nil, err
		}
		readTotalData += n
	}

	return data, nil
}

