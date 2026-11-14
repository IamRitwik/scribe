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
	"crypto/tls"
	"io"
	"log"

	api "github.com/IamRitwik/scribe/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Config provides dependency to gRPC server
type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject string, object string, action string) error
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
func (s *grpcServer) Produce(
	ctx context.Context,
	req *api.ProduceRequest,
) (*api.ProduceResponse, error) {
	log.Println("gRPC Produce request received")
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		log.Printf("Append failed: %v", err)
		return nil, err
	}

	log.Printf("Record successfully appended at offset %d", offset)

	return &api.ProduceResponse{
		Offset: offset,
	}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
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

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"could not find peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.Error(
			codes.Unauthenticated,
			"invalid transport credentials",
		)
	}
	log.Printf("TLS version: %s", tls.VersionName(tlsInfo.State.Version))

	if len(tlsInfo.State.VerifiedChains) == 0 ||
		len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx, status.Error(
			codes.Unauthenticated,
			"no verified certificate chain",
		)
	}
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

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
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
