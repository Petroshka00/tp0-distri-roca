package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const (
	CONNECTION_ATTEMPTS_MAX     = 3
	CONNECTION_ATTEMPS_DELAY_MS = 200
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

func (client *Client) connect() error {
	if err := protocol.SendMsg(client.conn, protocol.Connect, []byte(client.config.AgencyId)); err != nil {
		logger.Error("connect-agency", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	msgType, payload, err := protocol.RecvMsg(client.conn)
	if err != nil {
		logger.Error("connect-agency-ack", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	if msgType != protocol.Ack {
		logger.Error("connect-agency-ack", logger.Fail, "agency-id", client.config.AgencyId, "type", msgType)
		return fmt.Errorf("unexpected connect response type %d: %q", msgType, string(payload))
	}

	return nil
}

func (client *Client) sendBatch(batch []string, messageId int) error {
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	payload := strings.Join(batch, "\n")

	if err := protocol.SendMsg(client.conn, protocol.BetBatch, []byte(payload)); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}

	msgType, respPayload, err := protocol.RecvMsg(client.conn)
	if err != nil {
		logger.Error("recv-batch-success", logger.Fail, messageArgs...)
		return err
	}
	if msgType != protocol.Ack {
		logger.Error("recv-batch-success", logger.Fail, messageArgs...)
		return fmt.Errorf("unexpected server response type %d: %q", msgType, string(respPayload))
	}

	return nil
}

func (client *Client) sendEndAndReceiveWinners(messageId int) (string, error) {
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	if err := protocol.SendMsg(client.conn, protocol.End, nil); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return "", err
	}

	msgType, payload, err := protocol.RecvMsg(client.conn)
	if err != nil {
		logger.Error("recv-response", logger.Fail, messageArgs...)
		return "", err
	}
	if msgType != protocol.Winners {
		logger.Error("recv-response", logger.Fail, messageArgs...)
		return "", fmt.Errorf("unexpected response type %d: %q", msgType, string(payload))
	}

	return string(payload), nil
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

	if err := client.connect(); err != nil {
		return err
	}

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
		batch = append(batch, scanner.Text())

		if len(batch) == client.config.BatchSize {
			if err := client.sendBatch(batch, messageId); err != nil {
				return err
			}
			batch = batch[:0]
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

