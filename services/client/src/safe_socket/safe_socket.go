package safe_socket

import "io"

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

func RecvAll(socket io.Reader, amount int) ([]byte, error) {
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
