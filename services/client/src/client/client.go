package client

import (
	"net"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"

	"bufio"
	"os"
	"fmt"
	"strings"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
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

func (client *Client) Run() error {
	const mainAction = "test-echo-server"
	defer client.conn.Close()
	inputFilePath := fmt.Sprintf("/input/input-%s.csv", client.config.AgencyId)
	file, err_file := os.Open(inputFilePath)
	if err_file != nil {
		logger.Error("open-file", logger.Fail, "file-path", inputFilePath)
		return err_file
	}
	defer file.Close()

	outputFilePath := fmt.Sprintf("/output/output-%s.csv", client.config.AgencyId)
	outputFile, err_output := os.Create(outputFilePath)
	if err_output != nil {
		logger.Error("create-file", logger.Fail, "file-path", outputFilePath)
		return err_output
	}
	defer outputFile.Close()
	
	scanner := bufio.NewScanner(file)
	messageId := 0
	
	batch := []string{}
	for scanner.Scan() {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		clientMessage := fmt.Sprintf("%s,%s", client.config.AgencyId, scanner.Text())

		batch = append(batch, clientMessage)

		if len(batch) == client.config.BatchSize {
			finalMessage := strings.Join(batch, "\n")
			if err := safe_socket.SendAll(client.conn, []byte(finalMessage)); err != nil {
				logger.Error("send-message", logger.Fail, messageArgs...)
				return err
			}

			responseBuffer, err := safe_socket.RecvAll(client.conn)
			if string(responseBuffer) != "SUCCESS\n" {
				logger.Error("recv-batch-success", logger.Fail, messageArgs...)
				return err
			}

			batch = []string{}				
		}
		messageId++
	}
	
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	if len(batch) > 0 {
		finalMessage := strings.Join(batch, "\n")
		if err := safe_socket.SendAll(client.conn, []byte(finalMessage)); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}

		responseBuffer, err := safe_socket.RecvAll(client.conn)
		if string(responseBuffer) != "SUCCESS\n" {
			logger.Error("recv-batch-success", logger.Fail, messageArgs...)
			return err
		}
	}

	clientMessage := "END"
	if err := safe_socket.SendAll(client.conn, []byte(clientMessage)); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}

	responseBuffer, err := safe_socket.RecvAll(client.conn)
	if err != nil {
		logger.Error("recv-response", logger.Fail, messageArgs...)
		return err
	}

	responseStr := string(responseBuffer)
	outputFile.WriteString(responseStr + "\n")
	outputFile.Sync()

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	
	return nil
}
