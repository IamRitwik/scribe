package main

/*
./bin/scribe-server --help

# Terminal 1
./bin/scribe-server --config-file "${HOME}/.scribe/node-1.yaml"

# Terminal 2
./bin/scribe-server --config-file "${HOME}/.scribe/node-2.yaml"

# Terminal 3
./bin/scribe-server --config-file "${HOME}/.scribe/node-3.yaml"

CLI flags
    |
    v
Environment variables (SCRIBE_*)
    |
    v
YAML configuration
    |
    v
CLI defaults
*/

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/IamRitwik/scribe/internal/agent"
	"github.com/IamRitwik/scribe/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type cli struct {
	v      *viper.Viper
	logger *zap.Logger
}

func main() {
	if err := newCLI().execute(); err != nil {
		log.Fatalf("scribe-server: %v", err)
	}
}

func newCLI() *cli {
	return &cli{
		v: viper.New(),
	}
}

func (c *cli) execute() error {
	cmd := &cobra.Command{
		Use:          "scribe-server",
		Short:        "Start a Scribe distributed log node",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		PreRunE:      c.loadConfig,
		RunE:         c.run,
	}

	c.addFlags(cmd)

	if err := c.v.BindPFlags(cmd.Flags()); err != nil {
		return fmt.Errorf("bind CLI flags: %w", err)
	}

	c.v.SetEnvPrefix("SCRIBE")
	c.v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	c.v.AutomaticEnv()

	return cmd.Execute()
}

func (c *cli) addFlags(cmd *cobra.Command) {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("get home directory: %v", err)
	}

	configDir := filepath.Join(home, ".scribe")

	cmd.Flags().String(
		"config-file",
		filepath.Join(configDir, "config.yaml"),
		"Path to the Scribe configuration file.",
	)

	cmd.Flags().String(
		"data-dir",
		filepath.Join(".", "data", "scribe"),
		"Directory containing this node's persistent data.",
	)

	cmd.Flags().String("node-name", "", "Unique node identity.")

	cmd.Flags().String(
		"bind-addr",
		"127.0.0.1:8401",
		"Serf membership bind address.",
	)

	cmd.Flags().Int(
		"rpc-port",
		8400,
		"Shared gRPC and Raft TCP port.",
	)

	cmd.Flags().String(
		"rpc-advertise-host",
		"",
		"Host advertised to clients and Raft peers. Defaults to the host derived from bind-addr.",
	)

	cmd.Flags().StringSlice(
		"start-join-addrs",
		nil,
		"Serf addresses to join. Supply multiple addresses separated by commas.",
	)

	cmd.Flags().Bool(
		"bootstrap",
		false,
		"Bootstrap a brand-new Raft cluster. Use true only for its initial node.",
	)

	cmd.Flags().String("acl-model-file", "", "Path to the ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to the ACL policy.")

	cmd.Flags().String("server-tls-cert-file", "", "Server TLS certificate.")
	cmd.Flags().String("server-tls-key-file", "", "Server TLS private key.")
	cmd.Flags().String("server-tls-ca-file", "", "CA certificate used to verify clients.")

	cmd.Flags().String("peer-tls-cert-file", "", "Raft peer TLS certificate.")
	cmd.Flags().String("peer-tls-key-file", "", "Raft peer TLS private key.")
	cmd.Flags().String("peer-tls-ca-file", "", "CA certificate used to verify Raft peers.")
	cmd.Flags().String(
		"peer-tls-server-address",
		"127.0.0.1",
		"Server name/IP used when verifying the Raft peer certificate.",
	)
}

func (c *cli) loadConfig(cmd *cobra.Command, args []string) error {
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}

	configFile = expandPath(configFile)

	c.v.SetConfigFile(configFile)

	if err := c.v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) {
			return fmt.Errorf(
				"configuration file %q not found; create it or pass --config-file",
				configFile,
			)
		}
		return fmt.Errorf("read configuration file: %w", err)
	}

	return nil
}

func expandPath(value string) string {
	value = os.ExpandEnv(value)

	if value == "~" {
		home, err := os.UserHomeDir()
		if err == nil {
			return home
		}
	}

	if strings.HasPrefix(value, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			value = filepath.Join(home, value[2:])
		}
	}

	return filepath.Clean(value)
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	c.logger = logger
	defer func() {
		_ = logger.Sync()
	}()

	zap.ReplaceGlobals(logger)

	agentConfig, err := c.buildAgentConfig()
	if err != nil {
		return fmt.Errorf("build agent configuration: %w", err)
	}

	if err := validateConfig(agentConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	rpcAddr, err := agentConfig.RPCAddr()
	if err != nil {
		return fmt.Errorf("resolve RPC address: %w", err)
	}

	logger.Info(
		"starting Scribe node",
		zap.String("node", agentConfig.NodeName),
		zap.String("serf_bind", agentConfig.BindAddr),
		zap.String("rpc_addr", rpcAddr),
		zap.String("data_dir", agentConfig.DataDir),
		zap.Bool("bootstrap", agentConfig.Bootstrap),
		zap.Strings("start_join_addrs", agentConfig.StartJoinAddrs),
	)

	scribeAgent, err := agent.New(agentConfig)
	if err != nil {
		return fmt.Errorf("start Scribe agent: %w", err)
	}

	logger.Info(
		"Scribe node started",
		zap.String("node", agentConfig.NodeName),
		zap.String("rpc_addr", rpcAddr),
	)

	return waitForShutdown(scribeAgent, logger)
}

func (c *cli) buildAgentConfig() (agent.Config, error) {
	fmt.Printf(
		"\nCONFIG FILE: %s\n"+
			"node-name: %q\n"+
			"bind-addr: %q\n"+
			"rpc-port: %d\n"+
			"rpc-advertise-host: %q\n"+
			"bootstrap: %v\n\n",
		c.v.ConfigFileUsed(),
		c.v.GetString("node-name"),
		c.v.GetString("bind-addr"),
		c.v.GetInt("rpc-port"),
		c.v.GetString("rpc-advertise-host"),
		c.v.GetBool("bootstrap"),
	)
	var cfg agent.Config

	// Node configuration.
	cfg.NodeName = c.v.GetString("node-name")
	cfg.DataDir = expandPath(c.v.GetString("data-dir"))
	cfg.BindAddr = c.v.GetString("bind-addr")
	cfg.RPCPort = c.v.GetInt("rpc-port")
	cfg.RPCAdvertiseHost = c.v.GetString("rpc-advertise-host")
	cfg.StartJoinAddrs = c.v.GetStringSlice("start-join-addrs")
	cfg.Bootstrap = c.v.GetBool("bootstrap")

	// ACL configuration.
	cfg.ACLModelFile = expandPath(c.v.GetString("acl-model-file"))
	cfg.ACLPolicyFile = expandPath(c.v.GetString("acl-policy-file"))

	// Server-facing gRPC TLS configuration.
	serverTLS, err := c.buildTLSConfig(
		"server-tls-cert-file",
		"server-tls-key-file",
		"server-tls-ca-file",
		true,
		"",
	)
	if err != nil {
		return cfg, fmt.Errorf("server TLS: %w", err)
	}
	cfg.ServerTLSConfig = serverTLS

	// Raft peer TLS configuration.
	peerTLS, err := c.buildTLSConfig(
		"peer-tls-cert-file",
		"peer-tls-key-file",
		"peer-tls-ca-file",
		false,
		c.v.GetString("peer-tls-server-address"),
	)
	if err != nil {
		return cfg, fmt.Errorf("peer TLS: %w", err)
	}
	cfg.PeerTLSConfig = peerTLS

	return cfg, nil
}

func (c *cli) buildTLSConfig(
	certKey string,
	keyKey string,
	caKey string,
	server bool,
	serverAddress string,
) (*tls.Config, error) {
	certFile := expandPath(c.v.GetString(certKey))
	keyFile := expandPath(c.v.GetString(keyKey))
	caFile := expandPath(c.v.GetString(caKey))

	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf(
			"certificate, private key, and CA paths are all required",
		)
	}

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      certFile,
		KeyFile:       keyFile,
		CAFile:        caFile,
		Server:        server,
		ServerAddress: serverAddress,
	})
	if err != nil {
		return nil, err
	}

	tlsConfig.MinVersion = tls.VersionTLS12

	return tlsConfig, nil
}

func validateConfig(cfg agent.Config) error {
	if strings.TrimSpace(cfg.NodeName) == "" {
		return errors.New("node-name must not be empty")
	}

	if strings.TrimSpace(cfg.DataDir) == "" {
		return errors.New("data-dir must not be empty")
	}

	if strings.TrimSpace(cfg.BindAddr) == "" {
		return errors.New("bind-addr must not be empty")
	}

	if cfg.RPCPort < 1 || cfg.RPCPort > 65535 {
		return fmt.Errorf("rpc-port must be between 1 and 65535")
	}

	/*if cfg.Bootstrap && len(cfg.StartJoinAddrs) > 0 {
		return errors.New(
			"bootstrap=true cannot be combined with start-join-addrs; " +
				"the initial node bootstraps locally, other nodes join it",
		)
	}*/

	if cfg.ServerTLSConfig == nil {
		return errors.New("server TLS configuration is required")
	}

	if cfg.PeerTLSConfig == nil {
		return errors.New("peer TLS configuration is required")
	}

	return nil
}

func waitForShutdown(a *agent.Agent, logger *zap.Logger) error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-ctx.Done()

	logger.Info("shutdown signal received")

	if err := a.Shutdown(); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	logger.Info("Scribe node stopped")

	return nil
}
