package server

/*
- How the gRPC server is constructed and registered.
- How grpcServer implements the generated LogServer interface.
- How the CommitLog interface achieves dependency inversion.
- How Go interfaces work with your existing Log and Segment types.
- How streaming RPCs work differently from ordinary RPCs.
*/

import (
	"context"
	"io"

	api "github.com/IamRitwik/scribe/api/v1"
	"google.golang.org/grpc"
)

// Config provides dependency to gRPC server
type Config struct {
	CommitLog CommitLog
}

// compile-time interface implementation check
// grpcServer implements LogServer interface
var _ api.LogServer = (*grpcServer)(nil)

// struct embedding and field promotion for grpcServer
type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Dependency inversion with interfae
// implicit interface implementation
// *Log implements CommitLog interface
/*
myLog, err := log.NewLog(config)
grpcServer, err := server.NewGRPCServer(
	&server.Config{
		CommitLog: myLog,
	}
)
*/
// CommitLog interface = Dynamic Type: *log.Log, Dynamic Value: myLog
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

// gRPC server receives the request
// Producer() receives the request (req.Record)
// gRPC server delegates the operation
// gRPC server constructs the response and sends to client
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			err = stream.Send(res)
			if err != nil {
				return err
			}
			req.Offset++
		}
	}
}

/*
1. Producer calls client.Produce() with a record.
2. gRPC transports and decodes the request.
3. grpcServer.Produce() is invoked.
4. CommitLog.Append() dispatches to Log.Append().
5. Log assigns an offset and appends the record.
6. gRPC server returns ProduceResponse.
7. Producer receives the assigned offset.
*/

// grpc.NewServer() creates the server.
// RegisterLogServer() registers your service.
// net.Listen() creates the network listener.
// Serve() actually starts accepting connections.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
