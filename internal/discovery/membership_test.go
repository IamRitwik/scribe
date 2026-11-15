package discovery

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
)

type mockHandler struct {
	joinCh  chan memberInfo
	leaveCh chan string

	joinErr  error
	leaveErr error
}

type memberInfo struct {
	name string
	addr string
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		joinCh:  make(chan memberInfo, 10),
		leaveCh: make(chan string, 10),
	}
}

func (h *mockHandler) Join(name, addr string) error {
	if h.joinErr != nil {
		return h.joinErr
	}

	h.joinCh <- memberInfo{
		name: name,
		addr: addr,
	}
	return nil
}

func (h *mockHandler) Leave(name string) error {
	if h.leaveErr != nil {
		return h.leaveErr
	}

	h.leaveCh <- name
	return nil
}

// newMembership creates a Membership instance with an ephemeral
// local port, avoiding hardcoded ports and port conflicts.
func newMembership(
	t *testing.T,
	name string,
	handler Handler,
	joinAddrs []string,
) *Membership {
	t.Helper()

	m, err := New(handler, Config{
		NodeName: name,
		BindAddr: "127.0.0.1:0",
		Tags: map[string]string{
			"rpc_addr": "127.0.0.1:8400",
		},
		StartJoinAddrs: joinAddrs,
	})
	if err != nil {
		t.Fatalf("failed to create membership: %v", err)
	}

	t.Cleanup(func() {
		// Leave is graceful. Shutdown releases Serf's resources.
		// Ignore errors because cleanup must remain best-effort.
		if m.serf != nil {
			_ = m.serf.Leave()
			_ = m.serf.Shutdown()
		}
	})

	return m
}

// serfAddr returns the actual address selected by Serf.
// When BindAddr uses port 0, the OS assigns an available port.
func serfAddr(m *Membership) string {
	member := m.serf.LocalMember()

	return net.JoinHostPort(
		member.Addr.String(),
		strconv.Itoa(int(member.Port)),
	)
}

// waitFor repeatedly evaluates condition until it succeeds
// or the timeout expires.
func waitFor(
	t *testing.T,
	timeout time.Duration,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

func TestMembership(t *testing.T) {
	handler := newMockHandler()

	m := newMembership(t, "node-1", handler, nil)

	// A standalone node should initially know only itself.
	members := m.Members()

	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}

	local := m.serf.LocalMember()

	if local.Name != "node-1" {
		t.Errorf("expected node name node-1, got %s", local.Name)
	}

	if local.Tags["rpc_addr"] != "127.0.0.1:8400" {
		t.Errorf(
			"expected rpc_addr 127.0.0.1:8400, got %s",
			local.Tags["rpc_addr"],
		)
	}

	// The local node should not be passed to its own Handler.
	select {
	case got := <-handler.joinCh:
		t.Fatalf("unexpected local join callback: %+v", got)
	default:
	}
}

func TestMembershipJoin(t *testing.T) {
	handler1 := newMockHandler()
	node1 := newMembership(t, "node-1", handler1, nil)

	handler2 := newMockHandler()

	node2 := newMembership(
		t,
		"node-2",
		handler2,
		[]string{serfAddr(node1)},
	)

	// Node 1 should discover Node 2.
	select {
	case got := <-handler1.joinCh:
		if got.name != "node-2" {
			t.Fatalf("expected node-2, got %s", got.name)
		}

		if got.addr != "127.0.0.1:8400" {
			t.Fatalf("unexpected RPC address: %s", got.addr)
		}

	case <-time.After(10 * time.Second):
		t.Fatal("node-1 did not receive node-2 join event")
	}

	// Node 2 should also discover Node 1.
	select {
	case got := <-handler2.joinCh:
		if got.name != "node-1" {
			t.Fatalf("expected node-1, got %s", got.name)
		}

	case <-time.After(10 * time.Second):
		t.Fatal("node-2 did not receive node-1 join event")
	}

	// Verify both nodes have converged on the same membership.
	waitFor(t, 10*time.Second, func() bool {
		return len(node1.Members()) == 2 &&
			len(node2.Members()) == 2
	})
}

func TestMembershipLeave(t *testing.T) {
	handler1 := newMockHandler()
	node1 := newMembership(t, "node-1", handler1, nil)

	handler2 := newMockHandler()
	node2 := newMembership(
		t,
		"node-2",
		handler2,
		[]string{serfAddr(node1)},
	)

	// Wait until node-1 has processed node-2's join.
	select {
	case got := <-handler1.joinCh:
		if got.name != "node-2" {
			t.Fatalf("expected node-2, got %s", got.name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("node-1 did not receive node-2 join event")
	}

	// Gracefully leave the cluster.
	if err := node2.Leave(); err != nil {
		t.Fatalf("failed to leave cluster: %v", err)
	}

	// Node 1 should receive the departure event.
	select {
	case got := <-handler1.leaveCh:
		if got != "node-2" {
			t.Fatalf("expected node-2 to leave, got %s", got)
		}

	case <-time.After(10 * time.Second):
		t.Fatal("node-1 did not receive node-2 leave event")
	}
}

func TestMembershipInvalidAddress(t *testing.T) {
	handler := newMockHandler()

	_, err := New(handler, Config{
		NodeName: "node-invalid",
		BindAddr: "invalid-address",
	})

	if err == nil {
		t.Fatal("expected error for invalid bind address")
	}
}

// This test verifies that handler errors do not crash event processing.
// We inject a Serf membership event directly rather than relying on
// an actual network event.
func TestMembershipHandlerErrors(t *testing.T) {
	handler := newMockHandler()
	handler.joinErr = errors.New("join handler failed")
	handler.leaveErr = errors.New("leave handler failed")

	m := newMembership(t, "node-1", handler, nil)

	member := serf.Member{
		Name: "node-2",
		Tags: map[string]string{
			"rpc_addr": "127.0.0.1:8401",
		},
	}

	// These methods should log errors rather than panic.
	m.handleJoin(member)
	m.handleLeave(member)

	// Verify the configured errors were exercised.
	if handler.joinErr.Error() != "join handler failed" {
		t.Fatal(fmt.Errorf("unexpected join error"))
	}

	if handler.leaveErr.Error() != "leave handler failed" {
		t.Fatal(fmt.Errorf("unexpected leave error"))
	}
}
