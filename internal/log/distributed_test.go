package log_test

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func newTestDistributedLog(
	t *testing.T,
	id string,
	bootstrap bool,
) (*log.DistributedLog, string) {
	t.Helper()

	dataDir := t.TempDir()

	// Let the OS allocate an available TCP port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	config := log.Config{}
	config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
	config.Raft.LocalID = raft.ServerID(id)
	config.Raft.Bootstrap = bootstrap

	// Keep election timings short for tests.
	config.Raft.HeartbeatTimeout = 50 * time.Millisecond
	config.Raft.ElectionTimeout = 50 * time.Millisecond
	config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	config.Raft.CommitTimeout = 5 * time.Millisecond

	l, err := log.NewDistributedLog(dataDir, config)
	require.NoError(t, err)

	// Close the distributed log when the test finishes.
	t.Cleanup(func() {
		_ = l.Close()
	})

	return l, ln.Addr().String()
}

func TestMultipleNodes(t *testing.T) {
	const nodeCount = 3

	logs := make([]*log.DistributedLog, 0, nodeCount)
	addresses := make([]string, 0, nodeCount)

	// Start the first node as the bootstrap node.
	leader, addr := newTestDistributedLog(t, "0", true)
	logs = append(logs, leader)
	addresses = append(addresses, addr)

	require.NoError(t, leader.WaitForLeader(3*time.Second))

	// Start followers and join them to the leader.
	for i := 1; i < nodeCount; i++ {
		id := fmt.Sprintf("%d", i)

		follower, addr := newTestDistributedLog(t, id, false)

		err := leader.Join(id, addr)
		require.NoError(t, err)

		logs = append(logs, follower)
		addresses = append(addresses, addr)
	}

	// Verify that GetServers returns all cluster members.
	servers, err := leader.GetServers()
	require.NoError(t, err)
	require.Len(t, servers, nodeCount)

	for _, server := range servers {
		switch server.Id {
		case "0":
			require.Equal(t, addresses[0], server.RpcAddr)
			require.True(t, server.IsLeader)

		case "1":
			require.Equal(t, addresses[1], server.RpcAddr)
			require.False(t, server.IsLeader)

		case "2":
			require.Equal(t, addresses[2], server.RpcAddr)
			require.False(t, server.IsLeader)

		default:
			t.Fatalf("unexpected server ID: %s", server.Id)
		}
	}

	// Test replication from the leader to every follower.
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {
		off, err := leader.Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for _, node := range logs {
				got, err := node.Read(off)
				if err != nil {
					return false
				}

				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}

				if got.Offset != off {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	}

	// Remove node 1 from the Raft cluster.
	err = leader.Leave("1")
	require.NoError(t, err)

	// Verify that the cluster configuration reflects the removal.
	servers, err = leader.GetServers()
	require.NoError(t, err)
	require.Len(t, servers, 2)

	for _, server := range servers {
		require.NotEqual(t, "1", server.Id)

		if server.Id == "0" {
			require.True(t, server.IsLeader)
		}

		if server.Id == "2" {
			require.False(t, server.IsLeader)
		}
	}

	// Append another record after the membership change.
	off, err := leader.Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)

	// Node 2 should receive the new record.
	require.Eventually(t, func() bool {
		got, err := logs[2].Read(off)
		return err == nil &&
			reflect.DeepEqual(got.Value, []byte("third")) &&
			got.Offset == off
	}, 5*time.Second, 50*time.Millisecond)

	// Node 1 should not receive the new record.
	require.Eventually(t, func() bool {
		_, err := logs[1].Read(off)
		return reflect.TypeOf(err) == reflect.TypeOf(api.ErrOffsetOutOfRange{})
	}, 2*time.Second, 50*time.Millisecond)
}
