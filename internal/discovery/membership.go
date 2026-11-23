package discovery

import (
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Handler interface {
	Join(name string, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Membership struct {
	Config

	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger

	stopCh           chan struct{}
	eventHandlerDone chan struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

func New(handler Handler, config Config) (*Membership, error) {
	m := &Membership{
		Config:           config,
		handler:          handler,
		logger:           zap.L().Named("membership"),
		stopCh:           make(chan struct{}),
		eventHandlerDone: make(chan struct{}),
	}

	if err := m.setupSerf(); err != nil {
		return nil, err
	}

	return m, nil
}

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

	go m.eventHandler()

	if len(m.StartJoinAddrs) > 0 {
		if _, err = m.serf.Join(m.StartJoinAddrs, true); err != nil {
			_ = m.Shutdown()
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	defer close(m.eventHandlerDone)

	for {
		select {
		case <-m.stopCh:
			return

		case e, ok := <-m.events:
			if !ok {
				return
			}

			switch e.EventType() {
			case serf.EventMemberJoin:
				for _, member := range e.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					m.handleJoin(member)
				}

			case serf.EventMemberLeave:
				for _, member := range e.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					//m.handleFail(member)
					m.logger.Info(
						"member departed",
						zap.String("name", member.Name),
						zap.String("event", string(e.EventType())),
					)
				}
			case serf.EventMemberFailed:
				for _, member := range e.(serf.MemberEvent).Members {
					if m.isLocal(member) {
						continue
					}
					//m.handleFail(member)
					m.logger.Info(
						"member departed",
						zap.String("name", member.Name),
						zap.String("event", string(e.EventType())),
					)
				}
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

func (m *Membership) handleFail(member serf.Member) {
	err := m.handler.Leave(member.Name)
	if err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	const (
		maxRetries = 10
		retryDelay = time.Second
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := m.handler.Leave(member.Name)
		if err == nil {
			m.logger.Info("successfully removed departed member from Raft",
				zap.String("name", member.Name),
			)
			return
		}

		if err != raft.ErrNotLeader {
			m.logError(err, "failed to leave", member)
			return
		}

		if attempt == maxRetries {
			break
		}

		timer := time.NewTimer(retryDelay)

		select {
		case <-timer.C:
		case <-m.stopCh:
			timer.Stop()
			return
		}
	}

	m.logger.Warn("exhausted retries removing departed member",
		zap.String("name", member.Name),
	)
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave broadcasts a graceful departure but keeps the event handler running.
// Use Shutdown when the Membership instance itself is being stopped.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// Shutdown stops membership processing before the caller shuts down Raft.
//
// It is safe to call multiple times.
func (m *Membership) Shutdown() error {
	m.shutdownOnce.Do(func() {
		// First broadcast the graceful departure while Serf is still running.
		leaveErr := m.serf.Leave()

		// Stop Serf's network activity and background goroutines.
		serfShutdownErr := m.serf.Shutdown()

		// Stop our event handler and wait for any in-flight handler.Join/Leave
		// call to finish before the Agent closes the Raft instance.
		close(m.stopCh)
		<-m.eventHandlerDone

		if leaveErr != nil {
			m.shutdownErr = leaveErr
		} else if serfShutdownErr != nil {
			m.shutdownErr = serfShutdownErr
		}
	})

	return m.shutdownErr
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error

	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}

	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
