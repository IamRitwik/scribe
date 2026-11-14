package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/IamRitwik/scribe/internal/auth"
	"github.com/IamRitwik/scribe/internal/config"
	scribeLog "github.com/IamRitwik/scribe/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type testEnv struct {
	rootClient      api.LogClient
	nobodyClient    api.LogClient
	noCertClient    api.LogClient
	wrongCAClient   api.LogClient
	wrongNameClient api.LogClient

	cfg *Config
}

func TestServer(t *testing.T) {
	tests := map[string]func(*testing.T, *testEnv){
		"produce and consume succeeds":        testProduceConsume,
		"produce and consume stream succeeds": testProduceConsumeStream,
		"consume past log boundary fails":     testConsumePastBoundary,
		"unauthorized client is denied":       testUnauthorized,
		"client without certificate fails":    testMissingClientCertificate,
		"client with wrong CA fails":          testUntrustedServerCA,
		"client with wrong hostname fails":    testWrongServerName,
	}

	for name, testFn := range tests {
		t.Run(name, func(t *testing.T) {
			env, teardown := setupTest(t)
			defer teardown()

			testFn(t, env)
		})
	}
}

func setupTest(t *testing.T) (*testEnv, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "scribe-server-test")
	require.NoError(t, err)

	clog, err := scribeLog.NewLog(dir, scribeLog.Config{})
	require.NoError(t, err)

	authorizer, err := auth.New(
		config.ACLModelFile,
		config.ACLPolicyFile,
	)
	require.NoError(t, err)

	cfg := &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)

	serverTLSConfig.MinVersion = tls.VersionTLS12

	grpcServer, err := NewGRPCServer(
		cfg,
		grpc.Creds(credentials.NewTLS(serverTLSConfig)),
	)
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(l)
	}()

	var connections []*grpc.ClientConn

	newClient := func(tlsConfig *tls.Config) api.LogClient {
		t.Helper()

		tlsConfig.MinVersion = tls.VersionTLS12

		conn, err := grpc.Dial(
			l.Addr().String(),
			grpc.WithTransportCredentials(
				credentials.NewTLS(tlsConfig),
			),
		)
		require.NoError(t, err)

		connections = append(connections, conn)
		return api.NewLogClient(conn)
	}

	// Normal authorized client.
	rootTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	rootClient := newClient(rootTLSConfig)

	// Valid certificate, but not authorized by Casbin.
	nobodyTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.NobodyClientCertFile,
		KeyFile:       config.NobodyClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	nobodyClient := newClient(nobodyTLSConfig)

	// Client trusts the server CA but has no client certificate.
	noCertTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	noCertClient := newClient(noCertTLSConfig)

	// Client presents a valid client certificate but trusts no server CA.
	clientCert, err := tls.LoadX509KeyPair(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	require.NoError(t, err)

	wrongCATLSConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      x509.NewCertPool(),
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS12,
	}

	wrongCAClient := newClient(wrongCATLSConfig)

	// Client trusts the correct CA but expects the wrong server identity.
	wrongNameTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "wrong.example.com",
	})
	require.NoError(t, err)

	wrongNameClient := newClient(wrongNameTLSConfig)

	env := &testEnv{
		rootClient:      rootClient,
		nobodyClient:    nobodyClient,
		noCertClient:    noCertClient,
		wrongCAClient:   wrongCAClient,
		wrongNameClient: wrongNameClient,
		cfg:             cfg,
	}

	teardown := func() {
		grpcServer.Stop()

		for _, conn := range connections {
			_ = conn.Close()
		}

		_ = l.Close()

		_ = clog.Close()

		_ = os.RemoveAll(dir)
	}

	return env, teardown
}

func testProduceConsume(t *testing.T, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := env.rootClient.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	require.NoError(t, err)
	require.NotNil(t, produce)

	consume, err := env.rootClient.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset},
	)
	require.NoError(t, err)
	require.NotNil(t, consume)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, produce.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	produce, err := env.rootClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})
	require.NoError(t, err)

	consume, err := env.rootClient.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})

	require.Nil(t, consume)
	require.Equal(
		t,
		status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err()),
		status.Code(err),
	)
}

func testProduceConsumeStream(t *testing.T, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	records := []*api.Record{
		{Value: []byte("first message")},
		{Value: []byte("second message")},
	}

	// Bidirectional ProduceStream.
	produceStream, err := env.rootClient.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err = produceStream.Send(&api.ProduceRequest{
			Record: record,
		})
		require.NoError(t, err)

		res, err := produceStream.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(offset), res.Offset)
	}

	require.NoError(t, produceStream.CloseSend())

	// Server-streaming ConsumeStream.
	consumeStream, err := env.rootClient.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	for offset, record := range records {
		res, err := consumeStream.Recv()
		require.NoError(t, err)

		want := &api.Record{
			Value:  record.Value,
			Offset: uint64(offset),
		}

		require.Equal(t, want, res.Record)
	}
}

func testUnauthorized(t *testing.T, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TLS succeeds: nobody has a valid certificate signed by our CA.
	// Casbin rejects the identity at the application authorization layer.
	produce, err := env.nobodyClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("unauthorized")},
	})

	require.Nil(t, produce)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	consume, err := env.nobodyClient.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})

	require.Nil(t, consume)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func assertRPCFails(t *testing.T, client api.LogClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("should fail")},
	})

	require.Error(t, err)
}

func testMissingClientCertificate(t *testing.T, env *testEnv) {
	assertRPCFails(t, env.noCertClient)
}

func testUntrustedServerCA(t *testing.T, env *testEnv) {
	assertRPCFails(t, env.wrongCAClient)
}

func testWrongServerName(t *testing.T, env *testEnv) {
	assertRPCFails(t, env.wrongNameClient)
}
