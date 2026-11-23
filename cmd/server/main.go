package main

/*
Node        Serf membership port    gRPC/Raft port    Data directory
---------------------------------------------------------------------
node-1      8401                    8400              data/node-1
node-2      8403                    8402              data/node-2
node-3      8405                    8404              data/node-3

Terminal 1:

SCRIBE_NODE_NAME=node-1 \
SCRIBE_BIND_ADDR=127.0.0.1:8401 \
SCRIBE_RPC_PORT=8400 \
SCRIBE_RPC_ADVERTISE_HOST=127.0.0.1 \
SCRIBE_DATA_DIR=./data/node-1 \
SCRIBE_BOOTSTRAP=true \
./bin/scribe-server

SCRIBE_NODE_NAME=node-1 \
SCRIBE_BIND_ADDR=127.0.0.1:8401 \
SCRIBE_RPC_PORT=8400 \
SCRIBE_RPC_ADVERTISE_HOST=127.0.0.1 \
SCRIBE_DATA_DIR=./data/node-1 \
SCRIBE_BOOTSTRAP=false \
SCRIBE_START_JOIN_ADDRS=127.0.0.1:8401 \
./bin/scribe-server

Terminal 2:

SCRIBE_NODE_NAME=node-2 \
SCRIBE_BIND_ADDR=127.0.0.1:8403 \
SCRIBE_RPC_PORT=8402 \
SCRIBE_RPC_ADVERTISE_HOST=127.0.0.1 \
SCRIBE_DATA_DIR=./data/node-2 \
SCRIBE_BOOTSTRAP=false \
SCRIBE_START_JOIN_ADDRS=127.0.0.1:8401 \
./bin/scribe-server

Terminal 3:

SCRIBE_NODE_NAME=node-3 \
SCRIBE_BIND_ADDR=127.0.0.1:8405 \
SCRIBE_RPC_PORT=8404 \
SCRIBE_RPC_ADVERTISE_HOST=127.0.0.1 \
SCRIBE_DATA_DIR=./data/node-3 \
SCRIBE_BOOTSTRAP=false \
SCRIBE_START_JOIN_ADDRS=127.0.0.1:8401 \
./bin/scribe-server
*/

import (
	"crypto/tls"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/IamRitwik/scribe/internal/agent"
	"github.com/IamRitwik/scribe/internal/config"
	"go.uber.org/zap"
)

func env(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	zap.ReplaceGlobals(logger)

	defer func() {
		if err := logger.Sync(); err != nil {
			log.Printf("failed to sync logger: %v", err)
		}
	}()

	// -------------------------------------------------------------------------
	// Node configuration
	// -------------------------------------------------------------------------

	nodeName := env("SCRIBE_NODE_NAME", "node-1")

	// Serf membership listener address.
	bindAddr := env(
		"SCRIBE_BIND_ADDR",
		"127.0.0.1:8401",
	)

	// Shared gRPC/Raft TCP port.
	rpcPort, err := strconv.Atoi(
		env("SCRIBE_RPC_PORT", "8400"),
	)
	if err != nil {
		log.Fatalf("invalid RPC port: %v", err)
	}

	// Address advertised to Raft peers and discovered by clients.
	//
	// If empty, Agent.Config.RPCAddr() derives the host from BindAddr.
	rpcAdvertiseHost := env(
		"SCRIBE_RPC_ADVERTISE_HOST",
		"",
	)

	dataDir := env(
		"SCRIBE_DATA_DIR",
		"./data/"+nodeName,
	)

	bootstrap, err := strconv.ParseBool(
		env("SCRIBE_BOOTSTRAP", "false"),
	)
	if err != nil {
		log.Fatalf("invalid bootstrap value: %v", err)
	}

	// -------------------------------------------------------------------------
	// Serf bootstrap/join configuration
	// -------------------------------------------------------------------------

	var startJoinAddrs []string

	joinAddrs := env(
		"SCRIBE_START_JOIN_ADDRS",
		"",
	)

	if joinAddrs != "" {
		for _, addr := range strings.Split(joinAddrs, ",") {
			addr = strings.TrimSpace(addr)

			if addr != "" {
				startJoinAddrs = append(
					startJoinAddrs,
					addr,
				)
			}
		}
	}

	// -------------------------------------------------------------------------
	// TLS configuration
	// -------------------------------------------------------------------------

	// Client-facing gRPC server mTLS configuration.
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile: config.ServerCertFile,
			KeyFile:  config.ServerKeyFile,
			CAFile:   config.CAFile,
			Server:   true,
		},
	)
	if err != nil {
		log.Fatalf(
			"failed to configure server TLS: %v",
			err,
		)
	}

	serverTLSConfig.MinVersion = tls.VersionTLS12

	// Raft peer mTLS configuration.
	//
	// Retaining the current local smoke-test configuration.
	// The peer certificates must be valid for the server names
	// used by the Raft connections.
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	if err != nil {
		log.Fatalf(
			"failed to configure peer TLS: %v",
			err,
		)
	}

	peerTLSConfig.MinVersion = tls.VersionTLS12

	// -------------------------------------------------------------------------
	// Agent configuration
	// -------------------------------------------------------------------------

	agentConfig := agent.Config{
		ServerTLSConfig: serverTLSConfig,
		PeerTLSConfig:   peerTLSConfig,

		DataDir: dataDir,

		NodeName:         nodeName,
		BindAddr:         bindAddr,
		RPCPort:          rpcPort,
		RPCAdvertiseHost: rpcAdvertiseHost,

		Bootstrap:      bootstrap,
		StartJoinAddrs: startJoinAddrs,

		ACLModelFile:  config.ACLModelFile,
		ACLPolicyFile: config.ACLPolicyFile,
	}

	scribeAgent, err := agent.New(agentConfig)
	if err != nil {
		log.Fatalf(
			"failed to start Scribe agent: %v",
			err,
		)
	}

	// -------------------------------------------------------------------------
	// Startup information
	// -------------------------------------------------------------------------

	rpcAddr, err := scribeAgent.Config.RPCAddr()
	if err != nil {
		log.Fatalf(
			"failed to get RPC advertise address: %v",
			err,
		)
	}

	log.Printf(
		"Scribe agent started: node=%s serf=%s rpc=%s bootstrap=%t data=%s",
		nodeName,
		bindAddr,
		rpcAddr,
		bootstrap,
		dataDir,
	)

	// -------------------------------------------------------------------------
	// Graceful shutdown
	// -------------------------------------------------------------------------

	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-signals

	signal.Stop(signals)

	log.Printf(
		"shutting down Scribe node %s...",
		nodeName,
	)

	if err := scribeAgent.Shutdown(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			log.Printf(
				"error shutting down agent: %v",
				err,
			)
		}
	}

	log.Printf(
		"Scribe node %s stopped",
		nodeName,
	)
}
