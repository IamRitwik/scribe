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
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*Picker)(nil)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()
	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.
			Address.
			Attributes.
			Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers
	return p
}

/*
- Produce requests must go to the leader.
- Consume requests should be distributed across followers.
*/

var _ balancer.Picker = (*Picker)(nil)

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if p.leader == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	switch {
	case strings.Contains(info.FullMethodName, "Produce"):
		return balancer.PickResult{
			SubConn: p.leader,
		}, nil

	case strings.Contains(info.FullMethodName, "Consume"):
		return balancer.PickResult{
			SubConn: p.leader,
		}, nil

	default:
		return balancer.PickResult{
			SubConn: p.leader,
		}, nil
	}
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}
