package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/config"

	"github.com/IamRitwik/scribe/internal/loadbalance"
	"github.com/stretchr/testify/require"
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
	var agents []*Agent

	// Register cleanup before starting agents.
	t.Cleanup(func() {
		for i := len(agents) - 1; i >= 0; i-- {
			require.NoError(t, agents[i].Shutdown())
		}
	})

	for i := 0; i < 3; i++ {
		serfPort, rpcPort := freePorts(t)

		bindAddr := net.JoinHostPort(
			"127.0.0.1",
			strconv.Itoa(serfPort),
		)

		var startJoinAddrs []string

		if i != 0 {
			startJoinAddrs = []string{
				agents[0].Config.BindAddr,
			}
		}

		a, err := New(Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         t.TempDir(),
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,

			// Only Node 0 bootstraps the initial Raft cluster.
			Bootstrap: i == 0,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}

	// The resolver queries GetServers once when the client connects, so wait
	// until Raft has a leader and all three nodes have joined the cluster.
	require.Eventually(
		t,
		func() bool {
			servers, err := agents[0].log.GetServers()
			if err != nil || len(servers) != 3 {
				return false
			}
			for _, server := range servers {
				if server.IsLeader {
					return true
				}
			}
			return false
		},
		15*time.Second,
		100*time.Millisecond,
		"cluster did not elect a leader and gain all 3 members",
	)

	// Connect to Node 0, which bootstrapped the Raft cluster.
	// Connect through the Scribe resolver and picker.
	// Node 0 is only the bootstrap node.
	clusterClient := loadBalancedClient(
		t,
		agents[0],
		peerTLSConfig,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	// Produce should be routed to the Raft leader.
	produceResponse, err := clusterClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// Verify that the record eventually appears on both followers.
	// We use direct connections here so that we independently verify
	// replication to each specific follower.
	for i := 1; i < 3; i++ {
		followerClient := client(t, agents[i], peerTLSConfig)

		require.Eventually(
			t,
			func() bool {
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
			},
			10*time.Second,
			100*time.Millisecond,
			fmt.Sprintf(
				"record was not replicated to node %d",
				i,
			),
		)
	}

	// Now consume through the load-balanced client.
	// The picker should distribute these calls across followers.
	for i := 0; i < 6; i++ {
		response, err := clusterClient.Consume(
			ctx,
			&api.ConsumeRequest{
				Offset: produceResponse.Offset,
			},
		)

		require.NoError(t, err)
		require.Equal(
			t,
			[]byte("foo"),
			response.Record.Value,
		)
	}
}

func client(
	t *testing.T,
	a *Agent,
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

func loadBalancedClient(
	t *testing.T,
	a *Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	t.Helper()

	tlsCreds := credentials.NewTLS(tlsConfig)

	rpcAddr, err := a.Config.RPCAddr()
	require.NoError(t, err)

	target := fmt.Sprintf(
		"%s://%s",
		loadbalance.Name,
		rpcAddr,
	)

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(tlsCreds),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	return api.NewLogClient(conn)
}

func TestRPCAddr(t *testing.T) {
	tests := []struct {
		name          string
		bindAddr      string
		advertiseHost string
		want          string
	}{
		{
			name:     "derive advertise host from bind address",
			bindAddr: "127.0.0.1:8401",
			want:     "127.0.0.1:8400",
		},
		{
			name:          "explicit advertise host",
			bindAddr:      "0.0.0.0:8401",
			advertiseHost: "192.168.10.11",
			want:          "192.168.10.11:8400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BindAddr:         tt.bindAddr,
				RPCPort:          8400,
				RPCAdvertiseHost: tt.advertiseHost,
			}

			got, err := config.RPCAddr()

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
