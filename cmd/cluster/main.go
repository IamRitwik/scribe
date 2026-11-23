package main

/*
go build -o bin/scribe-client ./cmd/cluster

./bin/scribe-client \
  -addrs "127.0.0.1:8400,127.0.0.1:8402,127.0.0.1:8404"
*/

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	tlsConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "localhost",
		},
	)
	if err != nil {
		log.Fatalf("failed to configure client TLS: %v", err)
	}

	tlsConfig.MinVersion = tls.VersionTLS12

	addrsFlag := flag.String(
		"addrs",
		"127.0.0.1:8400",
		"comma-separated Scribe RPC addresses to use as bootstrap seeds",
	)

	timeout := flag.Duration(
		"timeout",
		3*time.Second,
		"connection and RPC timeout per node",
	)

	flag.Parse()

	seeds := parseAddresses(*addrsFlag)
	if len(seeds) == 0 {
		log.Fatal("provide at least one address using -addrs")
	}

	// Addresses successfully contacted.
	reachable := make(map[string]bool)

	// Latest known node information, indexed by node ID.
	nodes := make(map[string]*api.Server)

	// Prevent querying the same address more than once.
	queried := make(map[string]bool)

	var discoveredAddrs []string

	// Step 1: Query all configured bootstrap seeds.
	for _, addr := range seeds {
		queried[addr] = true

		servers, err := getServers(addr, *timeout, tlsConfig)
		if err != nil {
			log.Printf("could not query %s: %v", addr, err)
			continue
		}

		reachable[addr] = true
		mergeServers(nodes, servers)

		discoveredAddrs = append(
			discoveredAddrs,
			serverAddresses(servers)...,
		)
	}

	// Step 2: Query addresses discovered through GetServers.
	for _, addr := range uniqueAddresses(discoveredAddrs) {
		if queried[addr] {
			continue
		}

		queried[addr] = true

		servers, err := getServers(addr, *timeout, tlsConfig)
		if err != nil {
			log.Printf(
				"could not query discovered node %s: %v",
				addr,
				err,
			)
			continue
		}

		reachable[addr] = true
		mergeServers(nodes, servers)
	}

	if len(nodes) == 0 {
		log.Fatal("could not retrieve cluster information from any Scribe node")
	}

	printCluster(nodes, reachable)
}

func getServers(
	addr string,
	timeout time.Duration,
	tlsConfig *tls.Config,
) ([]*api.Server, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig.Clone()),
		),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	client := api.NewLogClient(conn)

	res, err := client.GetServers(
		ctx,
		&api.GetServersRequest{},
	)
	if err != nil {
		return nil, fmt.Errorf("GetServers RPC: %w", err)
	}

	return res.Servers, nil
}

func mergeServers(
	nodes map[string]*api.Server,
	servers []*api.Server,
) {
	for _, server := range servers {
		if server == nil {
			continue
		}

		// The latest queried observer's view wins.
		nodes[server.Id] = server
	}
}

func serverAddresses(servers []*api.Server) []string {
	var addrs []string

	for _, server := range servers {
		if server == nil || server.RpcAddr == "" {
			continue
		}

		addrs = append(addrs, server.RpcAddr)
	}

	return addrs
}

func parseAddresses(value string) []string {
	var addrs []string

	for _, addr := range strings.Split(value, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}

	return uniqueAddresses(addrs)
}

func uniqueAddresses(addrs []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, addr := range addrs {
		if addr == "" || seen[addr] {
			continue
		}

		seen[addr] = true
		result = append(result, addr)
	}

	return result
}

func printCluster(
	nodes map[string]*api.Server,
	reachable map[string]bool,
) {
	var servers []*api.Server

	for _, server := range nodes {
		if server != nil {
			servers = append(servers, server)
		}
	}

	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Id < servers[j].Id
	})

	// First render the table into memory so we can calculate
	// the separator width dynamically.
	var table strings.Builder

	w := tabwriter.NewWriter(
		&table,
		0,
		0,
		3,
		' ',
		0,
	)

	fmt.Fprintln(
		w,
		"NODE\tRAFT MEMBER\tRAFT ROLE\tLEADER\tSERF STATUS\tSERF ONLINE\tRPC ADDRESS\tRPC REACHABLE",
	)

	var raftCount int
	var serfCount int
	var leader string

	for _, server := range servers {
		raftMember := "no"
		if server.IsRaftMember {
			raftMember = "yes"
			raftCount++
		}

		leaderStatus := "no"
		if server.IsLeader {
			leaderStatus = "YES"
			leader = server.Id
		}

		serfOnline := "no"
		if server.IsOnline {
			serfOnline = "yes"
			serfCount++
		}

		rpcReachable := "unknown"

		if server.RpcAddr != "" {
			if reachable[server.RpcAddr] {
				rpcReachable = "yes"
			} else {
				rpcReachable = "no"
			}
		}

		raftRole := server.RaftRole
		if raftRole == "" && !server.IsRaftMember {
			raftRole = "not_member"
		}

		fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			server.Id,
			raftMember,
			raftRole,
			leaderStatus,
			server.SerfStatus,
			serfOnline,
			server.RpcAddr,
			rpcReachable,
		)
	}

	if err := w.Flush(); err != nil {
		log.Printf("could not render cluster table: %v", err)
		return
	}

	renderedTable := table.String()

	// Calculate the visible table width.
	tableWidth := 0
	for _, line := range strings.Split(strings.TrimRight(renderedTable, "\n"), "\n") {
		if len(line) > tableWidth {
			tableWidth = len(line)
		}
	}

	separator := strings.Repeat("-", tableWidth)
	border := strings.Repeat("=", tableWidth)

	// Print the final report.
	fmt.Println()
	fmt.Println("SCRIBE CLUSTER")
	fmt.Println(border)
	fmt.Print(renderedTable)
	fmt.Println(separator)

	// Use a separate tabwriter for the summary.
	var summary strings.Builder

	sw := tabwriter.NewWriter(
		&summary,
		0,
		0,
		3,
		' ',
		0,
	)

	fmt.Fprintf(sw, "Total nodes in combined view:\t%d\n", len(servers))
	fmt.Fprintf(sw, "Raft members:\t%d\n", raftCount)
	fmt.Fprintf(sw, "Serf-alive members:\t%d\n", serfCount)

	if leader == "" {
		leader = "unknown"
	}

	fmt.Fprintf(sw, "Current leader:\t%s\n", leader)

	if err := sw.Flush(); err != nil {
		log.Printf("could not render cluster summary: %v", err)
		return
	}

	fmt.Print(summary.String())
	fmt.Println(border)
	fmt.Println()
}
