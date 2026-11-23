package agent

/*
Node 1 starts
	 |
Serf discovers Node 0
	 |
Serf emits member-join event
	 |
eventHandler() processes event
	 |
m.log.Join(nodeID, rpcAddr)
	 |
Raft AddVoter()
	 |
Node becomes a voting Raft member
*/

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/auth"
	"github.com/IamRitwik/scribe/internal/discovery"
	"github.com/IamRitwik/scribe/internal/log"
	"github.com/IamRitwik/scribe/internal/server"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
- cmux classifies a TCP connection once, when it arrives.
- TLS secures that connection.
- Raft's transport handles Raft RPC messages over it.
- gRPC uses its own connection and HTTP/2 protocol.
- Both services share the same TCP listening port, not the same application protocol or connection.
*/
type Agent struct {
	Config Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config

	DataDir          string
	BindAddr         string
	RPCPort          int
	RPCAdvertiseHost string

	NodeName       string
	StartJoinAddrs []string
	ACLModelFile   string
	ACLPolicyFile  string
	Bootstrap      bool
}

func (c Config) RPCAddr() (string, error) {
	host := c.RPCAdvertiseHost

	if host == "" {
		var err error

		host, _, err = net.SplitHostPort(c.BindAddr)
		if err != nil {
			return "", fmt.Errorf(
				"extract RPC advertise host from bind address: %w",
				err,
			)
		}
	}

	return net.JoinHostPort(
		host,
		strconv.Itoa(c.RPCPort),
	), nil
}

// setup zap logger
// setup the commit log
// create the gRPC server
// setup membership, create gRPC client connection to its own node
// replicator receives a gRPC client pointing to its own server
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		err := fn()
		if err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

// create a listener on our RPC address
// that will accept both Raft and gRPC connections
// then creates mux with the listener
// mux accepts connections on that listener and match connections based on configured rules
func (a *Agent) setupMux() error {
	addr, err := net.ResolveTCPAddr("tcp", a.Config.BindAddr)
	if err != nil {
		return err
	}

	rpcAddr := net.JoinHostPort(
		addr.IP.String(),
		strconv.Itoa(a.Config.RPCPort),
	)

	fmt.Printf(
		"[MUX] node=%s bind_addr=%s rpc_port=%d listening_on=%s\n",
		a.Config.NodeName,
		a.Config.BindAddr,
		a.Config.RPCPort,
		rpcAddr,
	)

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", rpcAddr, err)
	}

	a.mux = cmux.New(ln)
	return nil
}

/*func (a *Agent) setupMux() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", a.Config.RPCPort))
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}*/

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// configure mux that matches Raft connection
// we identify raft connection by reading 1 byte and
// checking that the byte matches the byte we set up our ongoing Raft connections to write in StreamLayer
func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)

		_, err := io.ReadFull(reader, b)
		if err != nil {
			return false
		}

		return b[0] == byte(log.RaftRPC)
	})

	// Calculate the address this node should advertise to Raft peers.
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return fmt.Errorf("get RPC advertise address: %w", err)
	}

	advertiseAddr, err := net.ResolveTCPAddr("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("resolve RPC advertise address: %w", err)
	}

	logConfig := log.Config{}

	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
		advertiseAddr,
	)

	logConfig.Raft.BindAddr = rpcAddr
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}

	// Do not wait for a Raft leader here.
	//
	// A node may be restarting while other Raft voters are
	// temporarily unavailable.
	//
	// Allow the Agent to finish initializing Serf, gRPC,
	// and the shared TCP listener.

	/*if a.Config.Bootstrap {
		if err := a.log.WaitForLeader(3 * time.Second); err != nil {
			return fmt.Errorf("wait for initial Raft leader: %w", err)
		}
	}*/

	return nil
}

// we want Raft and gRPC server to share the same TCP port
// we need a way to distinguish which incoming TCP connection belongs to Raft and which one to gRPC
func (a *Agent) setupServer() error {
	authorizer, err := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	if err != nil {
		return err
	}
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	var firstErr error

	recordError := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// 1. Stop accepting new gRPC work.
	if a.server != nil {
		a.server.GracefulStop()
	}

	// 2. Broadcast Serf departure and stop membership processing.
	// Wait for in-flight membership handlers before closing Raft.
	if a.membership != nil {
		recordError(a.membership.Shutdown())
	}

	// 3. Shut down Raft and close its persistent stores.
	if a.log != nil {
		recordError(a.log.Close())
	}

	// 4. Close the multiplexed listener.
	if a.mux != nil {
		a.mux.Close()
	}

	return firstErr
}

func (a *Agent) serve() error {
	err := a.mux.Serve()
	if err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

func (a *Agent) GetServers() ([]*api.Server, error) {
	// 1. Obtain the current Raft configuration.
	servers, err := a.log.GetServers()
	if err != nil {
		return nil, err
	}

	// 2. Obtain the current Serf membership view.
	members := a.membership.Members()

	// 3. Index Serf members by node name.
	serfMembers := make(map[string]serf.Member, len(members))
	for _, member := range members {
		serfMembers[member.Name] = member
	}

	// 4. Track nodes already present in Raft.
	raftMembers := make(map[string]bool, len(servers))

	for _, server := range servers {
		raftMembers[server.Id] = true
		server.IsRaftMember = true

		member, ok := serfMembers[server.Id]
		if !ok {
			server.IsOnline = false
			server.SerfStatus = "not_observed"
			continue
		}

		server.SerfStatus = serfStatus(member.Status)
		server.IsOnline = member.Status == serf.StatusAlive

		// Prefer the RPC address advertised by Serf when available.
		if rpcAddr := member.Tags["rpc_addr"]; rpcAddr != "" {
			server.RpcAddr = rpcAddr
		}
	}

	// 5. Include Serf members not yet admitted to Raft.
	for _, member := range members {
		if raftMembers[member.Name] {
			continue
		}

		servers = append(servers, &api.Server{
			Id:           member.Name,
			RpcAddr:      member.Tags["rpc_addr"],
			IsLeader:     false,
			IsRaftMember: false,
			IsOnline:     member.Status == serf.StatusAlive,
			SerfStatus:   serfStatus(member.Status),
			RaftRole:     "not_member",
		})
	}

	return servers, nil
}

func serfStatus(status serf.MemberStatus) string {
	switch status {
	case serf.StatusAlive:
		return "alive"
	case serf.StatusLeaving:
		return "leaving"
	case serf.StatusLeft:
		return "left"
	case serf.StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}
