// agent/cluster.go

package agent

import (
	"fmt"

	"github.com/hashicorp/raft"
)

type ClusterNode struct {
	NodeID      string
	RaftMember  bool
	RaftState   string
	RaftAddress string
	SerfMember  bool
	SerfStatus  string
	RPCAddress  string
}

func (a *Agent) Cluster() ([]ClusterNode, error) {
	// 1. Get the Raft configuration.
	raftServers, err := a.log.GetServers()
	if err != nil {
		return nil, fmt.Errorf("get raft servers: %w", err)
	}

	// 2. Get the Serf membership snapshot.
	serfMembers := a.membership.Members()

	// 3. Create a map keyed by node ID.
	nodes := make(map[string]*ClusterNode)

	// 4. Populate Raft information.
	for _, server := range raftServers {
		node := &ClusterNode{
			NodeID:      string(server.Id),
			RaftMember:  true,
			RaftAddress: string(server.RpcAddr),
			RaftState:   "FOLLOWER",
		}

		if server.IsLeader {
			node.RaftState = "LEADER"
		}

		nodes[node.NodeID] = node
	}

	// 5. Merge Serf information.
	for _, member := range serfMembers {
		node, ok := nodes[member.Name]
		if !ok {
			// Serf member that is not yet a Raft voter.
			node = &ClusterNode{
				NodeID:     member.Name,
				RaftMember: false,
				RaftState:  "N/A",
			}
			nodes[member.Name] = node
		}

		node.SerfMember = true
		node.SerfStatus = member.Status.String()
		node.RPCAddress = member.Tags["rpc_addr"]
	}

	// 6. Convert map to a slice.
	result := make([]ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, *node)
	}

	return result, nil
}

// Keep this assertion only if your API needs raft imported elsewhere.
var _ = raft.Voter
