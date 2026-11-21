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
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/IamRitwik/scribe/internal/auth"
	"github.com/IamRitwik/scribe/internal/discovery"
	"github.com/IamRitwik/scribe/internal/log"
	"github.com/IamRitwik/scribe/internal/server"
	"github.com/hashicorp/raft"
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
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
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
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	// actual TCP listener
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	// multiplexer that accepts connections from ln and distributes them
	a.mux = cmux.New(ln)
	return nil
}

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
		_, err := reader.Read(b)
		if err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
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
		GetServerer: a.log,
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
	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) serve() error {
	err := a.mux.Serve()
	if err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
