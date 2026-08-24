package main

import (
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSize := os.Getenv("BATCH_SIZE")
	if batchSize == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}

	batchSizeInt, err := strconv.Atoi(batchSize)
	if err != nil {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is not a valid integer")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSizeInt,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)
	var sigReceived bool

	go func() {
		<-sigChannel
		logger.Info("signal-recv", logger.Success)
		sigReceived = true
		client.Close()
	}()

	if err := client.Run(); err != nil {
		if sigReceived {
			logger.Info("signal-recv", logger.Success)
			return 0
		}
		logger.Error("client-run", logger.Fail, "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
