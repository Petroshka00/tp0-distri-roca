package safe_socket

import (
	"encoding/binary"
	"io"
)

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	dataLenght := len(bytes)

	message := make([]byte, 4+dataLenght)

	binary.BigEndian.PutUint32(message[0:4], uint32(dataLenght))
	copy(message[4:], bytes)

	alreadyWritten := 0
	for alreadyWritten < len(message) {
		n, err := socket.Write(message[alreadyWritten:])
		if err != nil {
			return err
		}
		alreadyWritten += n
	}
	return nil
}

func RecvAll(socket io.Reader) ([]byte, error) {
	header := make([]byte, 4)

	header, err := ReadAmount(socket, 4)
	if err != nil {
		return nil, err
	}

	dataLenght := binary.BigEndian.Uint32(header)
	if dataLenght == 0 {
		return []byte{}, nil
	}

	return ReadAmount(socket, int(dataLenght))
}

func ReadAmount(socket io.Reader, amount int) ([]byte, error) {
	data := make([]byte, amount)
	readTotalData := 0
	targetDataLenght := amount

	for readTotalData < targetDataLenght {
		n, err := socket.Read(data[readTotalData:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, err
		}
		readTotalData += n
	}

	return data, nil
}
