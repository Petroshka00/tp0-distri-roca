package safe_socket

import "io"
import "encoding/binary"
//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	dataLenght := len(bytes)

	message := make([]byte, 4 + dataLenght)

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
	
	readTotalHeader := 0
	targetHeaderLenght := 4

	for readTotalHeader < targetHeaderLenght {
		n, err := socket.Read(header[readTotalHeader:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, err
		}
		readTotalHeader += n
	}

	dataLenght := binary.BigEndian.Uint32(header)
	if dataLenght == 0 {
		return []byte{}, nil
	}

	data := make([]byte, dataLenght)
	readTotalData := 0
	targetDataLenght := int(dataLenght)

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