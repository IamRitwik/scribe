package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
		log.Fatalf("failed to register client metric views: %v", err)
	}
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "localhost",
	})
	if err != nil {
		log.Fatalf("failed to configure client TLS: %v", err)
	}

	tlsConfig.MinVersion = tls.VersionTLS12

	conn, err := grpc.NewClient(
		"localhost:8400",
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig),
		),
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}),
	)
	if err != nil {
		log.Fatalf("failed to create gRPC client: %v", err)
	}
	defer conn.Close()

	client := api.NewLogClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	produceRes, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Hello Scribe!"),
			},
		},
	)
	if err != nil {
		log.Fatalf("produce failed: %v", err)
	}

	fmt.Println("Produced record at offset:", produceRes.Offset)

	consumeRes, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produceRes.Offset,
		},
	)
	if err != nil {
		log.Fatalf("consume failed: %v", err)
	}

	fmt.Println("Consumed record:", string(consumeRes.Record.Value))
	fmt.Println("Consumed offset:", consumeRes.Record.Offset)
}
