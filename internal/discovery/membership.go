package discovery

/*
- serf is a distributed cluster-membersip and event-propagation system
- serf uses SWIM protocol
- SWIM is designed to solve two problems:
1. Failure detection.
2. Dissemination of membership updates.
Serf builds on SWIM and adds practical functionality such as event handling,
cluster orchestration, and user-defined events.
*/

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// contract for responding to membership changes
type Handler interface {
	Join(name string, addr string) error
	Leave(name string) error
}

/*
Field			Purpose												Example
-------------------------------------------------------------------------------------------
NodeName		Unique identity of this Serf node					scribe-1
BindAddr		Local address where Serf listens					127.0.0.1:7946
Tags			Metadata distributed with membership information	rpc_addr=127.0.0.1:8400
StartJoinAddrs	Existing Serf nodes to contact when joining			127.0.0.1:7947
*/

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// type wrapping serf to provide discovery
// Config - local node configuration
// handler - notifies when membership changes
// serf - serf instance
// events - receives membership event emitted by serf
// logger - records errors encountered while processing events
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// constructor to create the Membership object
// delegates the Serf initialization to setupSerf
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	err := c.setupSerf()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// resolve the bind address
// start default Serf configuration
// configure the event channel, when memberships happen send to this channel
// configure node identity and metadata
// create the Serf instance
func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()

	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)

	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	// start goroutine to continuously consume events from Serf
	go m.eventHandler()
	// join an existing cluster if configured
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// processing membership events
// member joins - multiple membership events into single event
// member leaves or fails, EventMemberLeave - graceful departure,
// EventMemberFailed - unexpected failure
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	)
	if err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	err := m.handler.Leave(
		member.Name,
	)
	if err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
