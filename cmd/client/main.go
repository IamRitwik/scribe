package main

/*
Three-node Scribe cluster:

node-1    127.0.0.1:8400    Leader
node-2    127.0.0.1:8402    Follower
node-3    127.0.0.1:8404    Follower

Test flow:

1. Connect to the cluster through the bootstrap address.
2. Discover all Raft members.
3. Identify the leader and followers.
4. Produce a record.
5. Read the record directly from the leader.
6. Poll each follower independently until the same record is available.
7. Verify that every node returns the expected offset and value.
*/

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"
	"github.com/IamRitwik/scribe/internal/loadbalance"

	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	bootstrapAddress = "localhost:8404"

	replicationTimeout = 15 * time.Second
	requestTimeout     = 2 * time.Second

	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 1 * time.Second
)

// Node represents a discovered Scribe server.
type Node struct {
	ID       string
	Address  string
	IsLeader bool
}

// newNodeClient creates a direct connection to a particular Scribe node.
//
// Unlike the cluster connection, this connection does not use our
// custom resolver or load balancer.
func newNodeClient(
	addr string,
	tlsConfig *tls.Config,
) (*grpc.ClientConn, api.LogClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig.Clone()),
		),
		grpc.WithStatsHandler(
			&ocgrpc.ClientHandler{},
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"create connection to %s: %w",
			addr,
			err,
		)
	}

	return conn, api.NewLogClient(conn), nil
}

// discoverCluster obtains the Raft membership through the bootstrap address.
//
// The bootstrap address is only an entry point. It does not have to be
// the leader.
func discoverCluster(
	client api.LogClient,
) ([]Node, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	response, err := client.GetServers(
		ctx,
		&api.GetServersRequest{},
		grpc.WaitForReady(true),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"discover cluster: %w",
			err,
		)
	}

	var nodes []Node

	for _, server := range response.Servers {
		nodes = append(nodes, Node{
			ID:       server.Id,
			Address:  server.RpcAddr,
			IsLeader: server.IsLeader,
		})
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf(
			"cluster discovery returned no servers",
		)
	}

	return nodes, nil
}

// printCluster displays the discovered Raft membership.
func printCluster(nodes []Node) {
	fmt.Println("\nDiscovered Scribe cluster:")

	for _, node := range nodes {
		role := "FOLLOWER"

		if node.IsLeader {
			role = "LEADER"
		}

		fmt.Printf(
			"Node: %-10s Address: %-22s Role: %s\n",
			node.ID,
			node.Address,
			role,
		)
	}
}

// findLeader identifies the current leader from the discovered membership.
func findLeader(nodes []Node) (Node, error) {
	var leader Node
	leaderFound := false

	for _, node := range nodes {
		if !node.IsLeader {
			continue
		}

		if leaderFound {
			return Node{}, fmt.Errorf(
				"multiple leaders discovered",
			)
		}

		leader = node
		leaderFound = true
	}

	if !leaderFound {
		return Node{}, fmt.Errorf(
			"no leader found in discovered cluster",
		)
	}

	return leader, nil
}

// findFollowers returns all discovered followers.
func findFollowers(nodes []Node) []Node {
	var followers []Node

	for _, node := range nodes {
		if !node.IsLeader {
			followers = append(followers, node)
		}
	}

	return followers
}

// isRetryableConsumeError identifies errors that may resolve after
// a follower catches up with the leader.
func isRetryableConsumeError(err error) bool {
	if err == nil {
		return false
	}

	switch status.Code(err) {
	case codes.NotFound,
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted:
		return true
	}

	// Compatibility with Scribe's current application error:
	//
	// rpc error: code = Code(404) desc = offset out of range
	//
	// We should eventually map this error consistently to
	// gRPC codes.NotFound in the server.
	return strings.Contains(
		strings.ToLower(err.Error()),
		"offset out of range",
	)
}

// consumeFromNode reads an offset directly from one specific node.
func consumeFromNode(
	ctx context.Context,
	client api.LogClient,
	offset uint64,
) (*api.ConsumeResponse, error) {
	requestCtx, cancel := context.WithTimeout(
		ctx,
		requestTimeout,
	)
	defer cancel()

	response, err := client.Consume(
		requestCtx,
		&api.ConsumeRequest{
			Offset: offset,
		},
		grpc.WaitForReady(true),
	)
	if err != nil {
		return nil, err
	}

	if response.Record == nil {
		return nil, fmt.Errorf(
			"consume returned an empty record",
		)
	}

	return response, nil
}

// verifyRecord checks that the returned record matches the expected
// offset and value.
func verifyRecord(
	response *api.ConsumeResponse,
	expectedOffset uint64,
	expectedValue []byte,
) error {
	if response.Record == nil {
		return fmt.Errorf("response contains no record")
	}

	if response.Record.Offset != expectedOffset {
		return fmt.Errorf(
			"unexpected offset: got %d, want %d",
			response.Record.Offset,
			expectedOffset,
		)
	}

	if !bytes.Equal(
		response.Record.Value,
		expectedValue,
	) {
		return fmt.Errorf(
			"record value does not match the produced value",
		)
	}

	return nil
}

// readFromLeader verifies that the leader can immediately read the record
// returned by its successful Produce operation.
func readFromLeader(
	node Node,
	client api.LogClient,
	offset uint64,
	expectedValue []byte,
) error {
	fmt.Printf(
		"\nReading offset %d directly from leader %s (%s)...\n",
		offset,
		node.ID,
		node.Address,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := consumeFromNode(
		ctx,
		client,
		offset,
	)
	if err != nil {
		return fmt.Errorf(
			"leader %s could not read offset %d: %w",
			node.ID,
			offset,
			err,
		)
	}

	if err := verifyRecord(
		response,
		offset,
		expectedValue,
	); err != nil {
		return fmt.Errorf(
			"leader record verification failed: %w",
			err,
		)
	}

	fmt.Printf(
		"Leader %s successfully read offset %d: %q\n",
		node.ID,
		offset,
		string(response.Record.Value),
	)

	return nil
}

// waitForRecord repeatedly consumes the requested offset from one follower.
//
// It stops when:
//   - The expected record becomes available.
//   - A non-retryable error occurs.
//   - The overall context deadline expires.
func waitForRecord(
	ctx context.Context,
	node Node,
	client api.LogClient,
	offset uint64,
	expectedValue []byte,
) error {
	backoff := initialBackoff
	attempt := 0

	fmt.Printf(
		"\nWaiting for follower %s (%s) to apply offset %d...\n",
		node.ID,
		node.Address,
		offset,
	)

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf(
				"follower %s did not apply offset %d: %w",
				node.ID,
				offset,
				err,
			)
		}

		attempt++

		response, err := consumeFromNode(
			ctx,
			client,
			offset,
		)

		if err == nil {
			if err := verifyRecord(
				response,
				offset,
				expectedValue,
			); err != nil {
				return fmt.Errorf(
					"follower %s returned an unexpected record: %w",
					node.ID,
					err,
				)
			}

			fmt.Printf(
				"Follower %s successfully applied offset %d: %q (attempt %d)\n",
				node.ID,
				offset,
				string(response.Record.Value),
				attempt,
			)

			return nil
		}

		if !isRetryableConsumeError(err) {
			return fmt.Errorf(
				"non-retryable error from follower %s: %w",
				node.ID,
				err,
			)
		}

		fmt.Printf(
			"Follower %s is not ready yet (attempt %d): %v\n",
			node.ID,
			attempt,
			err,
		)

		timer := time.NewTimer(backoff)

		select {
		case <-ctx.Done():
			timer.Stop()

			return fmt.Errorf(
				"timed out waiting for follower %s: %w",
				node.ID,
				ctx.Err(),
			)

		case <-timer.C:
		}

		backoff *= 2

		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// verifyFollowers polls all followers concurrently.
//
// Each follower gets its own deadline, so one slow follower does not
// consume the time budget of another follower.
func verifyFollowers(
	followers []Node,
	tlsConfig *tls.Config,
	offset uint64,
	expectedValue []byte,
) error {
	if len(followers) == 0 {
		return fmt.Errorf("no followers discovered")
	}

	var wg sync.WaitGroup

	errCh := make(chan error, len(followers))

	for _, follower := range followers {
		follower := follower

		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, client, err := newNodeClient(
				follower.Address,
				tlsConfig,
			)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(
				context.Background(),
				replicationTimeout,
			)
			defer cancel()

			if err := waitForRecord(
				ctx,
				follower,
				client,
				offset,
				expectedValue,
			); err != nil {
				errCh <- err
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	var failures []string

	for err := range errCh {
		failures = append(failures, err.Error())
	}

	if len(failures) > 0 {
		return fmt.Errorf(
			"follower replication verification failed:\n%s",
			strings.Join(failures, "\n"),
		)
	}

	return nil
}

func main() {
	// -------------------------------------------------------------------------
	// Metrics
	// -------------------------------------------------------------------------

	if err := view.Register(
		ocgrpc.DefaultClientViews...,
	); err != nil {
		log.Fatalf(
			"failed to register client metric views: %v",
			err,
		)
	}

	// -------------------------------------------------------------------------
	// Client TLS
	// -------------------------------------------------------------------------

	tlsConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "localhost",
		},
	)
	if err != nil {
		log.Fatalf(
			"failed to configure client TLS: %v",
			err,
		)
	}

	tlsConfig.MinVersion = tls.VersionTLS12

	// -------------------------------------------------------------------------
	// Connect through the cluster resolver.
	//
	// This connection is used for cluster discovery and Produce.
	// The resolver routes Produce to the current leader.
	// -------------------------------------------------------------------------

	target := fmt.Sprintf(
		"%s://%s",
		loadbalance.Name,
		bootstrapAddress,
	)

	clusterConn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig.Clone()),
		),
		grpc.WithStatsHandler(
			&ocgrpc.ClientHandler{},
		),
	)
	if err != nil {
		log.Fatalf(
			"failed to create cluster client: %v",
			err,
		)
	}
	defer clusterConn.Close()

	clusterClient := api.NewLogClient(clusterConn)

	// -------------------------------------------------------------------------
	// 1. Discover cluster membership.
	// -------------------------------------------------------------------------

	nodes, err := discoverCluster(clusterClient)
	if err != nil {
		log.Fatalf("cluster discovery failed: %v", err)
	}

	printCluster(nodes)

	leader, err := findLeader(nodes)
	if err != nil {
		log.Fatalf("leader discovery failed: %v", err)
	}

	followers := findFollowers(nodes)

	fmt.Printf(
		"\nLeader identified: %s (%s)\n",
		leader.ID,
		leader.Address,
	)

	fmt.Printf(
		"Followers identified: %d\n",
		len(followers),
	)

	// -------------------------------------------------------------------------
	// 2. Produce a record through the cluster client.
	// -------------------------------------------------------------------------

	expectedValue := []byte(
		"Hello Scribe! Three-node Raft cluster.",
	)

	fmt.Println("\nProducing a record...")

	produceCtx, produceCancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	produceResponse, err := clusterClient.Produce(
		produceCtx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: expectedValue,
			},
		},
		grpc.WaitForReady(true),
	)

	produceCancel()

	if err != nil {
		log.Fatalf("Produce failed: %v", err)
	}

	offset := produceResponse.Offset

	fmt.Printf(
		"Produced record successfully. Offset: %d\n",
		offset,
	)

	// -------------------------------------------------------------------------
	// 3. Connect directly to the discovered leader.
	// -------------------------------------------------------------------------

	leaderConn, leaderClient, err := newNodeClient(
		leader.Address,
		tlsConfig,
	)
	if err != nil {
		log.Fatalf(
			"failed to connect to leader: %v",
			err,
		)
	}
	defer leaderConn.Close()

	// -------------------------------------------------------------------------
	// 4. Read the record directly from the leader.
	// -------------------------------------------------------------------------

	if err := readFromLeader(
		leader,
		leaderClient,
		offset,
		expectedValue,
	); err != nil {
		log.Fatalf("leader verification failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// 5. Poll each follower independently.
	// -------------------------------------------------------------------------

	fmt.Println(
		"\nLeader verification succeeded. Now checking follower application logs...",
	)

	if err := verifyFollowers(
		followers,
		tlsConfig,
		offset,
		expectedValue,
	); err != nil {
		log.Fatalf("%v", err)
	}

	// -------------------------------------------------------------------------
	// 6. Final result.
	// -------------------------------------------------------------------------

	fmt.Println("\n========================================")
	fmt.Println("SCRIBE REPLICATION VERIFICATION PASSED")
	fmt.Println("========================================")

	fmt.Printf("Produced offset: %d\n", offset)
	fmt.Printf("Leader: %s\n", leader.ID)

	for _, follower := range followers {
		fmt.Printf(
			"Verified follower: %s (%s)\n",
			follower.ID,
			follower.Address,
		)
	}

	fmt.Println("\nAll discovered followers have applied the record.")
}
