package main

/*
Node	Serf membership port	gRPC/Raft port	Data directory
--------------------------------------------------------------
node-1			8401				8400		data/node-1
node-2			8403				8402		data/node-2
node-3			8405				8404		data/node-3

*/

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"
	"github.com/IamRitwik/scribe/internal/loadbalance"

	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func verifyFollower(
	addr string,
	tlsConfig *tls.Config,
	offset uint64,
) error {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := api.NewLogClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	res, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: offset},
	)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Follower %s has offset %d: %q\n",
		addr,
		res.Record.Offset,
		string(res.Record.Value),
	)

	return nil
}

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

	// Node 1 is only our bootstrap address.
	// The custom resolver will discover the other cluster members.
	target := fmt.Sprintf("%s://%s", loadbalance.Name, "localhost:8400")

	conn, err := grpc.NewClient(
		target,
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

	// 1. Discover the cluster.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	servers, err := client.GetServers(
		ctx,
		&api.GetServersRequest{},
		grpc.WaitForReady(true),
	)
	cancel()

	if err != nil {
		log.Fatalf("GetServers failed: %v", err)
	}

	fmt.Println("\nDiscovered Scribe cluster:")

	for _, server := range servers.Servers {
		role := "FOLLOWER"
		if server.IsLeader {
			role = "LEADER"
		}

		fmt.Printf(
			"Node: %-10s Address: %-20s Role: %s\n",
			server.Id,
			server.RpcAddr,
			role,
		)
	}

	// 2. Produce a record.
	ctx, cancel = context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	produceRes, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Hello Scribe! Three-node Raft cluster."),
			},
		},
		grpc.WaitForReady(true),
	)
	cancel()

	if err != nil {
		log.Fatalf("Produce failed: %v", err)
	}

	fmt.Printf(
		"\nProduced record successfully. Offset: %d\n",
		produceRes.Offset,
	)

	// 3. Consume the same record several times.
	// The picker should distribute Consume requests across followers.
	fmt.Println("\nConsuming record:")

	for i := 1; i <= 6; i++ {
		ctx, cancel = context.WithTimeout(
			context.Background(),
			15*time.Second,
		)

		consumeRes, err := client.Consume(
			ctx,
			&api.ConsumeRequest{
				Offset: produceRes.Offset,
			},
			grpc.WaitForReady(true),
		)
		cancel()

		if err != nil {
			log.Fatalf("Consume attempt %d failed: %v", i, err)
		}

		fmt.Printf(
			"Read %d: offset=%d value=%q\n",
			i,
			consumeRes.Record.Offset,
			string(consumeRes.Record.Value),
		)
	}

	fmt.Println("\nScribe cluster smoke test completed!")

	for _, addr := range []string{
		"localhost:8402",
		"localhost:8404",
	} {
		if err := verifyFollower(addr, tlsConfig, produceRes.Offset); err != nil {
			log.Printf("Follower verification failed for %s: %v", addr, err)
		}
	}
}
