package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	CONNECTION_ATTEMPTS_MAX     = 3
	CONNECTION_ATTEMPS_DELAY_MS = 200
	PROTOCOL_SUCCESS            = "SUCCESS\n"
	PROTOCOL_END                = "END"
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error

	logger.Info(action, logger.InProgress)
	address := net.JoinHostPort(host, port)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, dialErr := net.Dial("tcp", address)
		if dialErr != nil {
			err = dialErr
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		return conn, nil
	}

	return nil, err
}

func (client *Client) Close() {
	if client.conn != nil {
		client.conn.Close()
	}
}

func (client *Client) sendBatch(batch []string, messageId int) error {
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	var finalMessage []byte
	for i, line := range batch {
		if i > 0 {
			finalMessage = append(finalMessage, '\n')
		}
		finalMessage = append(finalMessage, line...)
	}

	if err := safe_socket.SendMsg(client.conn, finalMessage); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}

	responseBuffer, err := safe_socket.RecvMsg(client.conn)
	if err != nil {
		logger.Error("recv-batch-success", logger.Fail, messageArgs...)
		return err
	}
	if string(responseBuffer) != PROTOCOL_SUCCESS {
		logger.Error("recv-batch-success", logger.Fail, messageArgs...)
		return fmt.Errorf("unexpected server response: %q", string(responseBuffer))
	}

	return nil
}

func (client *Client) sendEndAndReceiveWinners(messageId int) (string, error) {
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	if err := safe_socket.SendMsg(client.conn, []byte(PROTOCOL_END)); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return "", err
	}

	responseBuffer, err := safe_socket.RecvMsg(client.conn)
	if err != nil {
		logger.Error("recv-response", logger.Fail, messageArgs...)
		return "", err
	}

	return string(responseBuffer), nil
}

func (client *Client) writeWinners(winners string) error {
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-file", logger.Fail, "file-path", client.config.OutputFile)
		return err
	}
	defer outputFile.Close()

	if len(winners) > 0 {
		if _, err := outputFile.WriteString(winners + "\n"); err != nil {
			return err
		}
	}
	return outputFile.Sync()
}

func (client *Client) Run() error {
	const mainAction = "run-client"
	defer client.conn.Close()

	file, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-file", logger.Fail, "file-path", client.config.InputFile)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	messageId := 0

	batch := make([]string, 0, client.config.BatchSize)
	for scanner.Scan() {
		clientMessage := fmt.Sprintf("%s,%s", client.config.AgencyId, scanner.Text())
		batch = append(batch, clientMessage)

		if len(batch) == client.config.BatchSize {
			if err := client.sendBatch(batch, messageId); err != nil {
				return err
			}
			batch = []string{}
			messageId++
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, "file-path", client.config.InputFile)
		return err
	}

	if len(batch) > 0 {
		if err := client.sendBatch(batch, messageId); err != nil {
			return err
		}
		messageId++
	}

	winners, err := client.sendEndAndReceiveWinners(messageId)
	if err != nil {
		return err
	}

	if err := client.writeWinners(winners); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}
