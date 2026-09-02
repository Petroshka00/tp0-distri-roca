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

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

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
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
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
	if err != nil || string(responseBuffer) != "SUCCESS\n" {
		logger.Error("recv-batch-success", logger.Fail, messageArgs...)
		return err
	}

	return nil
}

func (client *Client) sendEndAndReceiveWinners(messageId int) (string, error) {
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	if err := safe_socket.SendMsg(client.conn, []byte("END")); err != nil {
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

	if _, err := outputFile.WriteString(winners + "\n"); err != nil {
		return err
	}
	return outputFile.Sync()
}

func (client *Client) Run() error {
	const mainAction = "test-echo-server"
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
			batch = batch[:0]
		}
		messageId++
	}

	if len(batch) > 0 {
		if err := client.sendBatch(batch, messageId); err != nil {
			return err
		}
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

