package loadbalance

/*
Component			Who provides it?	Responsibility
Resolver interface	gRPC				Contract for discovering backend addresses
Your Resolver		Scribe				Discovers Scribe cluster members
Picker interface	gRPC				Contract for selecting a connection
Your Picker	Scribe	Decides 			which node handles an RPC
Balancer framework	gRPC				Manages connections and invokes your picker
*/

import (
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*Picker)(nil)

// Picker is the gRPC picker builder.
// It creates a new immutable picker snapshot whenever gRPC calls Build.
type Picker struct{}

// picker is one immutable snapshot of the currently ready SubConns.
// Only current changes, and it is updated atomically.
type picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

var _ balancer.Picker = (*picker)(nil)

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	result := &picker{}

	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := false

		if attrs := scInfo.Address.Attributes; attrs != nil {
			value := attrs.Value("is_leader")
			isLeader, _ = value.(bool)
		}

		if isLeader {
			result.leader = sc
			continue
		}

		result.followers = append(result.followers, sc)
	}

	return result
}

/*
Routing policy:
- Produce requests go to the Raft leader.
- Consume requests are distributed across ready followers.
*/
func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	switch {
	case strings.Contains(info.FullMethodName, "Produce"):
		if p.leader == nil {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{SubConn: p.leader}, nil

	case strings.Contains(info.FullMethodName, "Consume"):
		follower := p.nextFollower()
		if follower == nil {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{SubConn: follower}, nil

	default:
		if p.leader == nil {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{SubConn: p.leader}, nil
	}
}

// nextFollower selects a ready follower using round-robin selection.
func (p *picker) nextFollower() balancer.SubConn {
	n := uint64(len(p.followers))
	if n == 0 {
		return nil
	}

	// Subtract one so the first call selects followers[0].
	cur := atomic.AddUint64(&p.current, 1) - 1
	return p.followers[cur%n]
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}
