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

	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}
	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				// START_HIGHLIGHT
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				// END_HIGHLIGHT
				grpc_auth.StreamServerInterceptor(authenticate),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			// START_HIGHLIGHT
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
			// END_HIGHLIGHT
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
		// START_HIGHLIGHT
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
		// END_HIGHLIGHT
	)
	gsrv := grpc.NewServer(grpcOpts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
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
	logger := ctxzap.Extract(ctx)

	err := s.Authorizer.Authorize(subject(ctx), "*", "produce")
	if err != nil {
		logger.Warn("produce request unauthorized", zap.Error(err))
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		logger.Error("failed to append record", zap.Error(err))
		return nil, err
	}

	logger.Info(
		"record appended",
		zap.Uint64("offset", offset),
		zap.Int("value_bytes", len(req.Record.Value)),
	)

	return &api.ProduceResponse{
		Offset: offset,
	}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	logger := ctxzap.Extract(ctx)

	err := s.Authorizer.Authorize(subject(ctx), "*", "consume")
	if err != nil {
		logger.Warn("consume request unauthorized", zap.Error(err))
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		logger.Error(
			"failed to read record",
			zap.Uint64("offset", req.Offset),
			zap.Error(err),
		)
		return nil, err
	}
	logger.Info(
		"record consumed",
		zap.Uint64("offset", record.Offset),
	)
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
	logger := ctxzap.Extract(ctx)
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
	logger.Info("TLS version", zap.String("tls_version", tls.VersionName(tlsInfo.State.Version)))

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
