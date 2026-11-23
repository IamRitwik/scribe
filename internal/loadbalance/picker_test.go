package loadbalance_test

import (
	"testing"

	"github.com/IamRitwik/scribe/internal/loadbalance"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	builder := &loadbalance.Picker{}
	picker := builder.Build(base.PickerBuildInfo{})

	for _, method := range []string{
		"/log.vX.Log/Produce",
		"/log.vX.Log/Consume",
	} {
		result, err := picker.Pick(balancer.PickInfo{
			FullMethodName: method,
		})

		require.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
		require.Nil(t, result.SubConn)
	}
}

func TestPickerProducesToLeader(t *testing.T) {
	picker, subConns := setupTest()

	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	}

	for i := 0; i < 5; i++ {
		result, err := picker.Pick(info)

		require.NoError(t, err)
		require.Same(t, subConns[0], result.SubConn)
	}
}

func TestPickerConsumesFromFollowers(t *testing.T) {
	picker, subConns := setupTest()

	info := balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	}

	var firstFollower balancer.SubConn

	for i := 0; i < 6; i++ {
		result, err := picker.Pick(info)

		require.NoError(t, err)
		require.NotNil(t, result.SubConn)

		// A Consume request must never go to the leader.
		require.NotSame(t, subConns[0], result.SubConn)

		// There are exactly two followers.
		require.True(
			t,
			result.SubConn == subConns[1] ||
				result.SubConn == subConns[2],
		)

		if i == 0 {
			firstFollower = result.SubConn
		} else {
			// With two followers, round-robin must alternate.
			if i%2 == 1 {
				require.NotSame(t, firstFollower, result.SubConn)
			} else {
				require.Same(t, firstFollower, result.SubConn)
			}
		}
	}
}

func TestPickerConsumeWithoutFollowers(t *testing.T) {
	leader := &subConn{}
	addr := resolver.Address{
		Attributes: attributes.New("is_leader", true),
	}

	builder := &loadbalance.Picker{}
	picker := builder.Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			leader: {Address: addr},
		},
	})

	result, err := picker.Pick(balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	})

	require.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
	require.Nil(t, result.SubConn)
}

func TestPickerBuildReturnsIndependentSnapshots(t *testing.T) {
	builder := &loadbalance.Picker{}

	firstLeader := &subConn{}
	firstFollower := &subConn{}

	first := builder.Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			firstLeader: {
				Address: resolver.Address{
					Attributes: attributes.New("is_leader", true),
				},
			},
			firstFollower: {
				Address: resolver.Address{
					Attributes: attributes.New("is_leader", false),
				},
			},
		},
	})

	secondLeader := &subConn{}
	second := builder.Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			secondLeader: {
				Address: resolver.Address{
					Attributes: attributes.New("is_leader", true),
				},
			},
		},
	})

	// Building a new picker must not mutate the previous picker.
	result, err := first.Pick(balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Consume",
	})

	require.NoError(t, err)
	require.Same(t, firstFollower, result.SubConn)

	result, err = second.Pick(balancer.PickInfo{
		FullMethodName: "/log.vX.Log/Produce",
	})

	require.NoError(t, err)
	require.Same(t, secondLeader, result.SubConn)
}

func setupTest() (balancer.Picker, []*subConn) {
	var subConns []*subConn

	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}

	for i := 0; i < 3; i++ {
		sc := &subConn{}

		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}

		sc.UpdateAddresses([]resolver.Address{addr})

		buildInfo.ReadySCs[sc] = base.SubConnInfo{
			Address: addr,
		}

		subConns = append(subConns, sc)
	}

	builder := &loadbalance.Picker{}
	picker := builder.Build(buildInfo)

	return picker, subConns
}

type subConn struct {
	balancer.SubConn
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}
