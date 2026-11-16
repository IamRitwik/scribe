package log

/*
Node A — Commit Log, Contains record at offset 0
1. ConsumeStream RPC, Initiated by Node B's Replicator
Node A — gRPC Server, Handles the streaming request

2. Stream records, Server streams records to Node B
Node B — Replicator, Receives records through stream.Recv()

3. Produce RPC, Calls LocalServer.Produce()
Node B — gRPC Server, Handles the local Produce request

4. Append record
Node B — Commit Log
Record is persisted in Node B's local log

An important observation: replication uses two different gRPC calls.
- ConsumeStream() goes from Node B to Node A.
- Produce() goes from Node B to Node B itself.
*/

import (
	"context"
	"sync"

	api "github.com/IamRitwik/scribe/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

// prevent concurrent modification
// initialize internal state
// check whether replication is closed
// prevent duplicate replication
// create a per server cancellation channel
// the channel allows to stop replication with an individual server
// start asynchronous replication
func (r *Replicator) Join(name string, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	_, ok := r.servers[name]
	if ok {
		// already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

// establish gRPC connection to remote node
// Node B Replicator  -------- gRPC --------> Node A
// create a logClient, now Node B can call Node A's log service
// start consuming records from offset 0
// receive records from gRPC server stream
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	// this context controls the lifetime of the gRPC stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *api.Record)
	recvError := make(chan error, 1)

	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				select {
				case recvError <- err:
				case <-ctx.Done():
				}
				r.logError(err, "failed to receive", addr)
				return
			}
			// Do not remain blocked sending a record if
			// the main replication goroutine exits.
			select {
			case records <- recv.Record:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	_, ok := r.servers[name]
	if !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
