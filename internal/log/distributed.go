package log

/*
- Application Log: Log
- Raft's replicated log: raft.Log
- Raft's log storage: LogStore
- Application's FSM: fsm
- FSM's snapshot
- Raft's stable store
- raft's snapshot store
- StreamLayer: networking layer connecting all raft nodes

Raft log: /data/raft/log
    Index 1 → Append("Hello")
    Index 2 → Append("World")

Application log: /data/log
    Offset 0 → Hello
    Offset 1 → World

data/log			Actual application records
data/raft/log		Raft's replicated commands
data/raft/stable	Persistent Raft metadata
data/raft/snapshots	Snapshots of application FSM state
*/

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
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{config: config}
	err := l.setupLog(dataDir)
	if err != nil {
		return nil, err
	}
	err = l.setupRaft(dataDir)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// Application Log: data/log
// stores actual scribe records
// used by DistributedLog.Read()
// contains scribe's data
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return err
	}
	l.log, err = NewLog(logDir, l.config)
	return err
}

// A raft instance:
// Finite State Machine (FSM) that applies commands provided to raft
// Log store to store log entries (commands, terms, indexes)
// Stable store where raft stores the cluster's configuration, servers in the cluster etc etc
// Snapshot store where raft stores compact snapshots of its FSM state
// Transport raft uses to connect to server's peers
func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return err
	}
	// Raft nees to store itws own consensus log
	// Raft's LogStore uses scribe's Log
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	// Raft log store: data/raft/lofg
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// StableStore - Persistent Raft metadata
	// BoltDB-backed persistent store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}
	retain := 1
	// Snapshots of FSM state
	// Raft takes snapshot of the FSM's current state
	// For scribe FSM state is the application log /data/log
	// snapshot contains that log data
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID // unique ID for this server
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr()},
			},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// we are not directly doing l.log.Append(record)
// we want raft to coordinate first
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

/*
Lifecycle of a Raft log entry
1. Submitted - Client submits a command to the leader through raft.Apply().
2. Persisted locally - Leader writes the Raft entry to its local LogStore.
3. Replicated - Leader sends the entry to followers. Followers persist it in their own LogStores.
4. Committed - Majority achieved, The leader establishes that the entry is committed
according to Raft's rules.
5. Applied - Raft invokes FSM.Apply() to execute the command.
*/
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	// please replicate this command
	// commit it according to raft's consensus rules
	// apply it to the FSM
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	// Raft returns a Future
	// A Future represnts an operation whose result will become available later
	// Retrieves the value returned by FSM's Apply
	res := future.Response()
	err, ok := res.(error)
	if ok {
		return nil, err
	}
	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// We ask Raft for current cluster membership
// Then we check whether the server is already present
func (l *DistributedLog) Join(id string, addr string) error {
	configFuture := l.raft.GetConfiguration()
	err := configFuture.Error()
	if err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			err = removeFuture.Error()
			if err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	err = addFuture.Error()
	if err != nil {
		return err
	}
	return nil
}

func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

// Finite State Machine
var _ raft.FSM = (*fsm)(nil)

// FSM operates on scribe Log
type fsm struct {
	log *Log
}

/*
- First byte identifies the command
+--------------------+--------------------------+
| Request Type       | Protobuf Request         |
+--------------------+--------------------------+
| 0                  | ProduceRequest bytes     |
+--------------------+--------------------------+
*/
type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Raft calls Apply when a Raft log entry has been committed
// and is ready to be applied to the state machine
// Raft decides and FSM executes the Raft log command
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Raft asks FSM to give a snapshot of current state
// FSM returns a reader that reads the complete application log
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist the snapshot using the file snapshot store
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Snapshot reader contains records in length-prefixed records
// [record length][record bytes]
// rebuild the application log from the snapshot
// restore must replace the existing state, that's why reset the log
// For Scribe, because we want to preserve the complete application log,
// our snapshot contains all the application records that have been retained in that log.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

// logStore embeds scribe's Log implementation
// also we add methods required by raft
type logStore struct {
	*Log
}

var _ raft.LogStore = (*logStore)(nil)

// creates the Raft log store
// stores raft log entries
// implements raft.LogStore
// stores command data, Raft terms, entry types
// used internally by Raft
func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	off, err := l.LowestOffset()
	if err == io.EOF {
		return 0, nil
	}
	return off, err
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	if err == io.EOF {
		return 0, nil
	}
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

// convert from raft.Log to api.Record
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// START: identify_raft
	// identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	// END: identify_raft
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
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
	return s.ln.Addr()
}
