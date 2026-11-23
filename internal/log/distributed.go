package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config    Config
	log       *Log
	raft      *raft.Raft
	raftStore *raftboltdb.BoltStore
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}

	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		// Clean up resources if Raft initialization fails.
		if l.raft != nil {
			_ = l.raft.Shutdown().Error()
		}
		if l.log != nil {
			_ = l.log.Close()
		}
		if l.raftStore != nil {
			_ = l.raftStore.Close()
		}
		return nil, err
	}

	return l, nil
}

// setupLog initializes Scribe's application log.
//
// This log stores application records, not Raft commands.
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft initializes HashiCorp Raft and its persistent storage.
func (l *DistributedLog) setupRaft(dataDir string) error {
	raftDir := filepath.Join(dataDir, "raft")

	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return err
	}

	// The FSM applies committed Raft commands to Scribe's application log.
	fsm := &fsm{
		log: l.log,
	}

	// BoltDB implements both raft.LogStore and raft.StableStore.
	//
	// It stores:
	//   - Raft log entries
	//   - Raft indexes
	//   - Raft terms
	//   - Raft stable metadata
	storePath := filepath.Join(raftDir, "raft.db")

	raftStore, err := raftboltdb.NewBoltStore(storePath)
	if err != nil {
		return fmt.Errorf("create raft BoltDB store: %w", err)
	}

	l.raftStore = raftStore

	// Create the snapshot store.
	snapshotStore, err := raft.NewFileSnapshotStore(
		raftDir,
		1,
		os.Stderr,
	)
	if err != nil {
		return fmt.Errorf("create raft snapshot store: %w", err)
	}

	// Check for existing Raft state BEFORE constructing the Raft instance.
	hasState, err := raft.HasExistingState(
		raftStore,
		raftStore,
		snapshotStore,
	)
	if err != nil {
		return fmt.Errorf("check existing raft state: %w", err)
	}

	// IMPORTANT:
	//
	// The application log is derived from the committed Raft log and
	// snapshots. Rebuild it during startup before Raft replays committed
	// entries into the FSM.
	//
	// If we leave the previous application log untouched, replaying
	// committed entries can append duplicates.
	if err := l.log.Reset(); err != nil {
		return fmt.Errorf("reset application log before FSM recovery: %w", err)
	}

	// Create Raft network transport.
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		5,
		10*time.Second,
		os.Stderr,
	)

	// Start with HashiCorp's default configuration.
	raftConfig := raft.DefaultConfig()

	// Assign this node's unique ID.
	raftConfig.LocalID = l.config.Raft.LocalID

	// Apply configured timeouts.
	if l.config.Raft.HeartbeatTimeout != 0 {
		raftConfig.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}

	if l.config.Raft.ElectionTimeout != 0 {
		raftConfig.ElectionTimeout = l.config.Raft.ElectionTimeout
	}

	if l.config.Raft.LeaderLeaseTimeout != 0 {
		raftConfig.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}

	if l.config.Raft.CommitTimeout != 0 {
		raftConfig.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// Construct the Raft instance.
	l.raft, err = raft.NewRaft(
		raftConfig,
		fsm,
		raftStore,     // LogStore
		raftStore,     // StableStore
		snapshotStore, // SnapshotStore
		transport,
	)
	if err != nil {
		return fmt.Errorf("create raft instance: %w", err)
	}

	// Bootstrap only if:
	//   1. This node is configured as the bootstrap node.
	//   2. No previous Raft state exists.
	if l.config.Raft.Bootstrap && !hasState {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID: raftConfig.LocalID,
					// Address: transport.LocalAddr(),
					Address: raft.ServerAddress(l.config.Raft.BindAddr),
				},
			},
		}

		if err := l.raft.BootstrapCluster(configuration).Error(); err != nil {
			return fmt.Errorf("bootstrap raft cluster: %w", err)
		}
	}

	return nil
}

// Append submits a record to Raft.
//
// The leader replicates and commits the command before the FSM appends
// the record to the application log.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{
			Record: record,
		},
	)
	if err != nil {
		return 0, err
	}

	response, ok := res.(*api.ProduceResponse)
	if !ok {
		return 0, fmt.Errorf("unexpected FSM response type: %T", res)
	}

	return response.Offset, nil
}

// apply serializes a command and submits it to Raft.
func (l *DistributedLog) apply(
	reqType RequestType,
	req proto.Message,
) (interface{}, error) {
	var buf bytes.Buffer

	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	if _, err := buf.Write(data); err != nil {
		return nil, err
	}

	future := l.raft.Apply(buf.Bytes(), 10*time.Second)

	if err := future.Error(); err != nil {
		return nil, err
	}

	response := future.Response()

	if err, ok := response.(error); ok {
		return nil, err
	}

	return response, nil
}

// Read reads a record from the application log.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// Join adds a node to the Raft cluster as a voter.
func (l *DistributedLog) Join(id string, addr string) error {
	configFuture := l.raft.GetConfiguration()

	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == serverID && server.Address == serverAddr {
			return nil
		}

		// If either the ID or address already exists, remove the
		// existing server before adding the new identity.
		if server.ID == serverID || server.Address == serverAddr {
			removeFuture := l.raft.RemoveServer(server.ID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	return addFuture.Error()
}

// Leave removes a node from the Raft cluster.
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader waits until this node knows the current leader.
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for raft leader")

		case <-ticker.C:
			if l.raft.Leader() != "" {
				return nil
			}
		}
	}
}

// Close shuts down Raft and closes persistent storage.
func (l *DistributedLog) Close() error {
	var firstErr error

	// Stop Raft before closing the stores it uses.
	if l.raft != nil {
		if err := l.raft.Shutdown().Error(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if l.log != nil {
		if err := l.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// raftStore implements both LogStore and StableStore.
	// Close it exactly once.
	if l.raftStore != nil {
		if err := l.raftStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// GetServers returns the current Raft cluster membership.
func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	raftRole := "unknown"

	if err := future.Error(); err != nil {
		return nil, err
	}

	var servers []*api.Server

	for _, server := range future.Configuration().Servers {
		switch server.Suffrage {
		case raft.Voter:
			raftRole = "voter"
		case raft.Nonvoter:
			raftRole = "nonvoter"
		}
		servers = append(servers, &api.Server{
			Id:           string(server.ID),
			RpcAddr:      string(server.Address),
			IsLeader:     l.raft.Leader() == server.Address,
			IsRaftMember: true,
			RaftRole:     raftRole,
		})
	}

	return servers, nil
}

// -----------------------------------------------------------------------------
// FSM
// -----------------------------------------------------------------------------

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

// RequestType identifies the command stored in a Raft log entry.
type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply is called by Raft when a committed entry is ready for application.
func (l *fsm) Apply(record *raft.Log) interface{} {
	if record == nil || len(record.Data) == 0 {
		return fmt.Errorf("invalid empty raft log entry")
	}

	buf := record.Data
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])

	default:
		return fmt.Errorf("unknown request type: %d", reqType)
	}
}

// applyAppend decodes a ProduceRequest and appends its record.
func (l *fsm) applyAppend(data []byte) interface{} {
	var req api.ProduceRequest

	if err := proto.Unmarshal(data, &req); err != nil {
		return err
	}

	if req.Record == nil {
		return fmt.Errorf("produce request contains no record")
	}

	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{
		Offset: offset,
	}
}

// Snapshot asks the FSM to provide a snapshot of its current state.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		reader: f.log.Reader(),
	}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist writes the FSM snapshot to the Raft snapshot sink.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore rebuilds the application log from a Raft snapshot.
func (f *fsm) Restore(r io.ReadCloser) error {
	defer r.Close()

	header := make([]byte, lenWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		_, err := io.ReadFull(r, header)

		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("read snapshot record length: %w", err)
		}

		size := int64(enc.Uint64(header))

		if size < 0 {
			return fmt.Errorf("invalid snapshot record size: %d", size)
		}

		if _, err := io.CopyN(&buf, r, size); err != nil {
			return fmt.Errorf("read snapshot record: %w", err)
		}

		record := &api.Record{}

		if err := proto.Unmarshal(buf.Bytes(), record); err != nil {
			return fmt.Errorf("unmarshal snapshot record: %w", err)
		}

		if i == 0 {
			// The snapshot may start at a non-zero application offset.
			f.log.Config.Segment.InitialOffset = record.Offset

			if err := f.log.Reset(); err != nil {
				return fmt.Errorf("reset application log during restore: %w", err)
			}
		}

		if _, err := f.log.Append(record); err != nil {
			return fmt.Errorf("append snapshot record: %w", err)
		}

		buf.Reset()
	}

	return nil
}

// -----------------------------------------------------------------------------
// Raft Stream Layer
// -----------------------------------------------------------------------------

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
	advertiseAddr   net.Addr
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig *tls.Config,
	peerTLSConfig *tls.Config,
	advertiseAddr ...net.Addr,
) *StreamLayer {
	s := &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}

	if len(advertiseAddr) > 0 {
		s.advertiseAddr = advertiseAddr[0]
	}

	return s
}

// RaftRPC is the first byte used by cmux to identify Raft connections.
const RaftRPC = 1

// Dial connects to another Raft node.
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: timeout,
	}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// Send the Raft marker before the TLS handshake.
	n, err := conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if n != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to write raft connection marker")
	}

	if s.peerTLSConfig != nil {
		return tls.Client(conn, s.peerTLSConfig), nil
	}

	return conn, nil
}

// Accept accepts an incoming Raft connection.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	// Read the Raft connection marker.
	marker := make([]byte, 1)

	if _, err := io.ReadFull(conn, marker); err != nil {
		_ = conn.Close()
		return nil, err
	}

	if marker[0] != byte(RaftRPC) {
		_ = conn.Close()
		return nil, fmt.Errorf("not a raft rpc connection")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	if s.advertiseAddr != nil {
		return s.advertiseAddr
	}

	return s.ln.Addr()
}
