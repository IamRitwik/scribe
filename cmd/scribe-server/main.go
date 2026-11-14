package main

import (
	"crypto/tls"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/IamRitwik/scribe/internal/auth"
	"github.com/IamRitwik/scribe/internal/config"
	scribeLog "github.com/IamRitwik/scribe/internal/log"
	server "github.com/IamRitwik/scribe/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	logConfig := scribeLog.Config{}
	logConfig.Segment.MaxStoreBytes = 1024
	logConfig.Segment.MaxIndexBytes = 1024
	logConfig.Segment.InitialOffset = 0

	dataDir := "./data"

	commitLog, err := scribeLog.NewLog(dataDir, logConfig)
	if err != nil {
		log.Fatalf("failed to create commit log: %v", err)
	}

	defer func() {
		if err := commitLog.Close(); err != nil {
			log.Printf("failed to close commit log: %v", err)
		}
	}()

	// Initialize application authorization.
	authorizer, err := auth.New(
		config.ACLModelFile,
		config.ACLPolicyFile,
	)
	if err != nil {
		log.Fatalf("failed to initialize authorizer: %v", err)
	}

	// Configure server-side mutual TLS.
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	if err != nil {
		log.Fatalf("failed to configure TLS: %v", err)
	}

	tlsConfig.MinVersion = tls.VersionTLS12 // TLS 1.2 minimum

	// Inject the log into gRPC server
	grpcServer, err := server.NewGRPCServer(
		&server.Config{
			CommitLog:  commitLog,
			Authorizer: authorizer,
		},
		grpc.Creds(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		log.Fatalf("failed to create gRPC server: %v", err)
	}

	var PORT string = ":8400"
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		<-signals
		log.Println("shutdown signal received...")
		log.Println("gracefully stopping gRPC server...")

		grpcServer.GracefulStop()
	}()

	log.Println("Scribe listening with mutual TLS on", PORT)

	err = grpcServer.Serve(lis)
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Printf("gRPC server stopped with error: %v", err)
	}

	log.Println("gRPC server stopped!")
}
