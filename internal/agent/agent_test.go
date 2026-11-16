package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/agent"
	"github.com/IamRitwik/scribe/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// freePorts allocates two available TCP ports.
// The listeners are closed before returning the ports.
func freePorts(t *testing.T) (int, int) {
	t.Helper()

	serfListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	rpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	serfPort := serfListener.Addr().(*net.TCPAddr).Port
	rpcPort := rpcListener.Addr().(*net.TCPAddr).Port

	require.NoError(t, serfListener.Close())
	require.NoError(t, rpcListener.Close())

	return serfPort, rpcPort
}

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// Start three agents.
	var agents []*agent.Agent

	// Register cleanup before starting agents, so partially
	// initialized test clusters are cleaned up too.
	t.Cleanup(func() {
		for _, a := range agents {
			require.NoError(t, a.Shutdown())
			require.NoError(t, os.RemoveAll(a.Config.DataDir))
		}
	})

	for i := 0; i < 3; i++ {
		serfPort, rpcPort := freePorts(t)

		bindAddr := net.JoinHostPort(
			"127.0.0.1",
			strconv.Itoa(serfPort),
		)

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = []string{
				agents[0].Config.BindAddr,
			}
		}

		a, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}

	// Give the Serf membership events a little time to propagate.
	// The actual replication assertion below is still condition-based.
	time.Sleep(500 * time.Millisecond)

	// Produce a record on the first agent.
	leaderClient := client(t, agents[0], peerTLSConfig)

	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// Verify that the record can be consumed from the first agent.
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), consumeResponse.Record.Value)

	// Wait until the record has been replicated to the second agent.
	followerClient := client(t, agents[1], peerTLSConfig)

	require.Eventually(t, func() bool {
		response, err := followerClient.Consume(
			context.Background(),
			&api.ConsumeRequest{
				Offset: produceResponse.Offset,
			},
		)
		if err != nil {
			return false
		}

		return string(response.Record.Value) == "foo"
	}, 10*time.Second, 100*time.Millisecond,
		"record was not replicated to follower",
	)
}

func client(
	t *testing.T,
	a *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	t.Helper()

	tlsCreds := credentials.NewTLS(tlsConfig)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	rpcAddr, err := a.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	return api.NewLogClient(conn)
}
