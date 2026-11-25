package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var defaultBootstrapAddresses = []string{
	"localhost:8400",
	"localhost:8402",
	"localhost:8404",
}

type Options struct {
	Bootstrap  []string
	TLSCert    string
	TLSKey     string
	TLSCA      string
	TLSServer  string
	RPCTimeout time.Duration
}

type Node struct {
	ID       string
	Address  string
	IsLeader bool
}

type NodeHealth struct {
	Node      Node
	Reachable bool
	Err       error
}

type Client struct {
	opts      Options
	tlsConfig *tls.Config
	in        *bufio.Reader
}

func loadOptions() (Options, error) {
	flags := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	flags.StringSlice("bootstrap", defaultBootstrapAddresses,
		"comma-separated bootstrap RPC addresses")
	flags.String("tls-cert", config.RootClientCertFile, "client TLS certificate")
	flags.String("tls-key", config.RootClientKeyFile, "client TLS private key")
	flags.String("tls-ca", config.CAFile, "CA certificate used to verify Scribe nodes")
	flags.String("tls-server-name", "localhost", "TLS server name to verify")
	flags.Duration("rpc-timeout", 3*time.Second, "timeout for an individual RPC")

	if err := flags.Parse(os.Args[1:]); err != nil {
		return Options{}, err
	}

	v := viper.New()
	v.SetEnvPrefix("SCRIBE")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	if err := v.BindPFlags(flags); err != nil {
		return Options{}, fmt.Errorf("bind CLI flags: %w", err)
	}

	opts := Options{
		Bootstrap:  v.GetStringSlice("bootstrap"),
		TLSCert:    v.GetString("tls-cert"),
		TLSKey:     v.GetString("tls-key"),
		TLSCA:      v.GetString("tls-ca"),
		TLSServer:  v.GetString("tls-server-name"),
		RPCTimeout: v.GetDuration("rpc-timeout"),
	}
	if len(opts.Bootstrap) == 0 {
		return Options{}, errors.New("at least one --bootstrap address is required")
	}
	if opts.RPCTimeout <= 0 {
		return Options{}, errors.New("--rpc-timeout must be greater than zero")
	}

	// Trim accidental spaces in comma-separated bootstrap values.
	for i := range opts.Bootstrap {
		opts.Bootstrap[i] = strings.TrimSpace(opts.Bootstrap[i])
		if opts.Bootstrap[i] == "" {
			return Options{}, errors.New("bootstrap addresses cannot be empty")
		}
	}
	return opts, nil
}

func newClient(opts Options) (*Client, error) {
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      opts.TLSCert,
		KeyFile:       opts.TLSKey,
		CAFile:        opts.TLSCA,
		ServerAddress: opts.TLSServer,
	})
	if err != nil {
		return nil, fmt.Errorf("configure client TLS: %w", err)
	}
	tlsConfig.MinVersion = tls.VersionTLS12

	return &Client{
		opts:      opts,
		tlsConfig: tlsConfig,
		in:        bufio.NewReader(os.Stdin),
	}, nil
}

// newNodeClient deliberately creates a direct connection to one node.
// This client does not depend on Scribe's custom resolver or picker.
func (c *Client) newNodeClient(addr string) (*grpc.ClientConn, api.LogClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig.Clone())),
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create connection to %s: %w", addr, err)
	}
	return conn, api.NewLogClient(conn), nil
}

// discoverCluster tries every configured seed in order. A seed is only an
// entry point; it does not need to be the Raft leader.
func (c *Client) discoverCluster() ([]Node, string, error) {
	var failures []string

	for _, seed := range c.opts.Bootstrap {
		fmt.Printf("Trying bootstrap seed %s...\n", seed)

		conn, rpcClient, err := c.newNodeClient(seed)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", seed, err))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.opts.RPCTimeout)
		response, err := rpcClient.GetServers(ctx, &api.GetServersRequest{})
		cancel()
		_ = conn.Close()

		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", seed, err))
			continue
		}
		if response == nil || len(response.Servers) == 0 {
			failures = append(failures, fmt.Sprintf("%s: discovery returned no servers", seed))
			continue
		}

		nodes := make([]Node, 0, len(response.Servers))
		seen := make(map[string]struct{}, len(response.Servers))
		for _, server := range response.Servers {
			if server == nil || server.RpcAddr == "" {
				continue
			}
			if _, ok := seen[server.RpcAddr]; ok {
				continue
			}
			seen[server.RpcAddr] = struct{}{}
			nodes = append(nodes, Node{
				ID:       server.Id,
				Address:  server.RpcAddr,
				IsLeader: server.IsLeader,
			})
		}
		if len(nodes) == 0 {
			failures = append(failures, fmt.Sprintf("%s: discovery returned no usable server addresses", seed))
			continue
		}

		fmt.Printf("Discovered %d Raft member(s) through %s.\n", len(nodes), seed)
		return nodes, seed, nil
	}

	return nil, "", fmt.Errorf("all bootstrap seeds failed:\n%s", strings.Join(failures, "\n"))
}

func findLeader(nodes []Node) (Node, error) {
	var leader Node
	found := false
	for _, node := range nodes {
		if !node.IsLeader {
			continue
		}
		if found {
			return Node{}, errors.New("discovery reported more than one leader")
		}
		leader, found = node, true
	}
	if !found {
		return Node{}, errors.New("no leader is currently reported; retry discovery after the Raft election")
	}
	return leader, nil
}

func printCluster(nodes []Node) {
	fmt.Println("\nDiscovered Raft configuration:")
	for _, node := range nodes {
		role := "FOLLOWER"
		if node.IsLeader {
			role = "LEADER"
		}
		fmt.Printf("  %-12s %-24s %s\n", node.ID, node.Address, role)
	}
}

func (c *Client) checkNode(node Node) NodeHealth {
	conn, rpcClient, err := c.newNodeClient(node.Address)
	if err != nil {
		return NodeHealth{Node: node, Err: err}
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.RPCTimeout)
	defer cancel()
	_, err = rpcClient.GetServers(ctx, &api.GetServersRequest{})
	return NodeHealth{Node: node, Reachable: err == nil, Err: err}
}

func (c *Client) showClusterStatus() error {
	nodes, seed, err := c.discoverCluster()
	if err != nil {
		return err
	}
	fmt.Printf("\nDiscovery source: %s\n", seed)
	printCluster(nodes)

	results := make([]NodeHealth, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		i, node := i, node
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.checkNode(node)
		}()
	}
	wg.Wait()

	reachable := 0
	fmt.Println("\nRPC reachability:")
	for _, result := range results {
		state := "UNREACHABLE"
		if result.Reachable {
			state = "REACHABLE"
			reachable++
		}
		fmt.Printf("  %-12s %-24s %-12s", result.Node.ID, result.Node.Address, state)
		if result.Err != nil {
			fmt.Printf(" (%v)", result.Err)
		}
		fmt.Println()
	}

	// This is only a rough client-side estimate. It assumes all configured
	// Raft servers are voters and does not prove that Raft has quorum.
	required := len(nodes)/2 + 1
	fmt.Printf("\nReachable RPC endpoints: %d/%d\n", reachable, len(nodes))
	fmt.Printf("Estimated majority (assuming all members are voters): %d/%d\n", required, len(nodes))
	if reachable >= required {
		fmt.Println("The reachable-node count meets the estimated majority.")
	} else {
		fmt.Println("The reachable-node count is below the estimated majority.")
	}
	fmt.Println("Note: RPC reachability is not proof of Raft quorum or replication health.")
	return nil
}

func (c *Client) produce() error {
	nodes, _, err := c.discoverCluster()
	if err != nil {
		return err
	}
	leader, err := findLeader(nodes)
	if err != nil {
		return err
	}

	fmt.Printf("Current leader: %s (%s)\n", leader.ID, leader.Address)
	fmt.Print("Record value (one line): ")
	value, err := c.readLine()
	if err != nil {
		return err
	}
	if value == "" {
		return errors.New("record value cannot be empty")
	}

	conn, rpcClient, err := c.newNodeClient(leader.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	response, err := rpcClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte(value)},
	})
	if err != nil {
		// Do not retry automatically. A timeout can occur after the server
		// committed the record but before the response reached this client.
		return fmt.Errorf(
			"produce through leader %s failed: %w\n"+
				"Do not blindly retry: the record may have committed. Rediscover and consume the expected offset before retrying",
			leader.ID, err,
		)
	}

	fmt.Printf("Produced successfully. Offset: %d\n", response.Offset)
	return nil
}

func isOffsetUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.NotFound {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "offset out of range")
}

func isNodeUnavailable(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func (c *Client) consume() error {
	nodes, _, err := c.discoverCluster()
	if err != nil {
		return err
	}

	fmt.Print("Offset to consume: ")
	rawOffset, err := c.readLine()
	if err != nil {
		return err
	}
	offset, err := strconv.ParseUint(strings.TrimSpace(rawOffset), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", rawOffset, err)
	}

	// Prefer followers for reads, then fall back to the leader. The service
	// currently exposes local reads, so a follower may not have applied the
	// requested offset yet.
	ordered := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		if !node.IsLeader {
			ordered = append(ordered, node)
		}
	}
	for _, node := range nodes {
		if node.IsLeader {
			ordered = append(ordered, node)
		}
	}

	var failures []string
	for _, node := range ordered {
		conn, rpcClient, dialErr := c.newNodeClient(node.Address)
		if dialErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", node.ID, dialErr))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.opts.RPCTimeout)
		response, callErr := rpcClient.Consume(ctx, &api.ConsumeRequest{Offset: offset})
		cancel()
		_ = conn.Close()

		if callErr == nil {
			if response == nil || response.Record == nil {
				failures = append(failures, fmt.Sprintf("%s: empty consume response", node.ID))
				continue
			}
			if response.Record.Offset != offset {
				return fmt.Errorf("%s returned offset %d; requested %d", node.ID, response.Record.Offset, offset)
			}
			fmt.Printf("\nConsumed from %s (%s)\n", node.ID, node.Address)
			fmt.Printf("Offset: %d\n", response.Record.Offset)
			fmt.Printf("Value:  %s\n", string(response.Record.Value))
			return nil
		}

		failures = append(failures, fmt.Sprintf("%s: %v", node.ID, callErr))
		if !isOffsetUnavailable(callErr) && !isNodeUnavailable(callErr) {
			// Authentication, malformed requests, and other application errors
			// are not made better by trying every other node.
			return fmt.Errorf("consume from %s failed: %w", node.ID, callErr)
		}
	}

	return fmt.Errorf("could not read offset %d from any discovered node:\n%s",
		offset, strings.Join(failures, "\n"))
}

func (c *Client) readLine() (string, error) {
	line, err := c.in.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line = strings.TrimSpace(line)
	if errors.Is(err, io.EOF) && line == "" {
		return "", io.EOF
	}
	return line, nil
}

func (c *Client) run() error {
	fmt.Println("Scribe interactive client")
	fmt.Println("Bootstrap seeds:", strings.Join(c.opts.Bootstrap, ", "))

	for {
		fmt.Println("\nChoose an operation:")
		fmt.Println("  1. Discover cluster and show status")
		fmt.Println("  2. Produce a record")
		fmt.Println("  3. Consume a record by offset")
		fmt.Println("  4. Exit")
		fmt.Print("> ")

		choice, err := c.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("\nInput closed; exiting.")
				return nil
			}
			return err
		}

		switch choice {
		case "1":
			if err := c.showClusterStatus(); err != nil {
				fmt.Printf("Cluster status failed: %v\n", err)
			}
		case "2":
			if err := c.produce(); err != nil {
				fmt.Printf("Produce failed: %v\n", err)
			}
		case "3":
			if err := c.consume(); err != nil {
				fmt.Printf("Consume failed: %v\n", err)
			}
		case "4", "q", "quit", "exit":
			fmt.Println("Goodbye.")
			return nil
		case "":
			continue
		default:
			fmt.Println("Unknown choice; enter 1, 2, 3, or 4.")
		}
	}
}

func main() {
	if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
		fmt.Fprintf(os.Stderr, "register client metric views: %v\n", err)
		os.Exit(1)
	}

	opts, err := loadOptions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	client, err := newClient(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize client: %v\n", err)
		os.Exit(1)
	}
	if err := client.run(); err != nil {
		fmt.Fprintf(os.Stderr, "client stopped: %v\n", err)
		os.Exit(1)
	}
}
