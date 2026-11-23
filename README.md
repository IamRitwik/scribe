

# ⚡ Scribe

<h3 align="center">A Distributed Commit Log Built with Go</h3>

<p align="center">
  <strong>Reliable. Replicated. Distributed.</strong>
  <br />
  Building the foundations of reliable distributed systems, one node at a time.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white" alt="Go" />
  <img src="https://img.shields.io/badge/gRPC-RPC-244c5a?logo=google&logoColor=white" alt="gRPC" />
  <img src="https://img.shields.io/badge/Consensus-Raft-blue" alt="Raft" />
  <img src="https://img.shields.io/badge/Discovery-Serf-orange" alt="Serf" />
  <img src="https://img.shields.io/badge/Container-Docker-2496ED?logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Orchestration-Kubernetes-326CE5?logo=kubernetes&logoColor=white" alt="Kubernetes" />
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License" />
</p>

<p align="center">
  <a href="#-architecture">Architecture</a> •
  <a href="#-getting-started">Getting Started</a> •
  <a href="#-running-a-scribe-cluster">Cluster Setup</a> •
  <a href="#-roadmap">Roadmap</a>
</p>

---
### A Distributed Commit Log Built with Go

Scribe is a distributed commit-log service written in Go, inspired by Travis Jeffrey's book [*Distributed Services with Go*](https://pragprog.com/titles/tjgo/distributed-services-with-go/).

**Reliable. Replicated. Distributed.**

The project explores how to build a distributed system from first principles: service discovery, cluster membership, consensus, replicated state, durable storage, secure communication, and client-side load balancing.

At its core, Scribe uses a **Raft-based replicated state machine** to coordinate a cluster of nodes and maintain a consistent, replicated commit log.

The project explores how to build distributed systems from first principles:

- 🔍 **Service Discovery** — Discover nodes and maintain cluster membership with Serf.
- 🗳️ **Consensus** — Coordinate distributed state using the Raft consensus algorithm.
- 🔄 **Replication** — Replicate log entries across cluster members.
- 💾 **Durable Storage** — Persist records using segmented append-only logs and indexing.
- ⚖️ **Load Balancing** — Distribute client requests across available service instances.
- 🔐 **Security** — Protect communication using TLS, mTLS, and authorization policies.
- 📊 **Observability** — Instrument services with structured logging, metrics, and tracing.

---

## 📚 Table of Contents

- [⚡ Architecture](#-architecture)
- [⚙️ How Scribe Works](#️-how-scribe-works)
- [🧩 Core Components](#-core-components)
- [🛠️ Technology Stack](#️-technology-stack)
- [📁 Repository Structure](#-repository-structure)
- [📋 Prerequisites](#-prerequisites)
- [🚀 Getting Started](#-getting-started)
- [🖥️ Running a Scribe Cluster](#️-running-a-scribe-cluster)
- [🔍 Cluster Discovery and Inspection](#-cluster-discovery-and-inspection)
- [⚙️ Configuration Reference](#️-configuration-reference)
- [🔐 TLS and Security](#-tls-and-security)
- [🧪 Testing](#-testing)
- [🐳 Docker](#-docker)
- [☸️ Kubernetes Deployment](#️-kubernetes-deployment)
- [🔨 Development Commands](#-development-commands)
- [🗺️ Roadmap](#️-roadmap)
- [🙏 Acknowledgements](#-acknowledgements)
- [📄 License](#-license)

---

## Architecture

Scribe is built around a replicated state machine backed by a durable, segmented commit log.

The architecture separates cluster membership, consensus, storage, and client communication into distinct components.

```text
                       PRODUCERS / CLIENTS
                               |
                               | gRPC
                               |
                     Client-side Load Balancing
                               |
                  +------------+------------+
                  |            |            |
                  v            v            v
              SCRIBE-1      SCRIBE-2      SCRIBE-3
                  |            |            |
          +-------+------------+------------+-------+
          |                    |                    |
          v                    v                    v
       SERF                 SERF                 SERF
     Membership           Membership           Membership
          |                    |                    |
          +--------------------+--------------------+
                               |
                       Cluster Membership
                               |
                               v
                         RAFT CONSENSUS
                               |
                      Leader Election
                               |
                       Log Replication
                               |
                       Commit Decisions
                               |
                  +------------+------------+
                  |            |            |
                  v            v            v
               Storage      Storage      Storage
                  |            |            |
                  v            v            v
               Segments     Segments     Segments
                  |            |            |
                  v            v            v
                Index        Index        Index
```

### Design principles

**Separation of concerns**

Membership, consensus, storage, authentication, and client communication are implemented as separate components.

**Consistency through consensus**

Raft coordinates replicated state and establishes which log entries are committed.

**Durability through local storage**

Each node maintains persistent log data using segmented storage and indexing.

**Resilience through replication**

The cluster maintains replicated state so that it can tolerate certain node failures, subject to Raft quorum requirements.

**Secure communication**

TLS and mutual TLS protect communication between clients, servers, and Raft peers.

**Operational visibility**

The cluster diagnostic command provides a consolidated view of node membership, Raft roles, leadership, and RPC reachability.

---

## How Scribe Works

### 1. Cluster discovery

Scribe uses Serf for cluster membership and failure detection.

When a node starts, it initializes its membership agent and either bootstraps locally or joins an existing cluster through configured Serf addresses.

Membership events allow nodes to maintain information about participating members.

### 2. Consensus and leader election

Raft coordinates the replicated state machine.

The cluster elects a leader, which coordinates Raft log replication. Followers replicate entries and participate in consensus according to Raft's quorum rules.

### 3. Replicated log

The log is the fundamental data structure of Scribe.

Records are appended to the log and persisted in segments. The index supports efficient record lookup without requiring a full sequential scan.

### 4. Client communication

Clients interact with Scribe through gRPC.

The project includes client-side service discovery and load-balancing components to help clients select available service instances.

### 5. Recovery

Persistent storage allows a node to recover its local log state after restart.

Raft replication and recovery coordinate the replicated state of the cluster. Local storage recovery and distributed consensus recovery are related, but distinct responsibilities.

---

## Core Components

| Component | Responsibility |
|---|---|
| Agent | Coordinates node lifecycle and cluster integration |
| Discovery | Serf-based membership and discovery |
| Raft integration | Consensus, leader election, and replication |
| Distributed log | Replicated log operations |
| Segment | Append-only record storage |
| Index | Efficient record lookup |
| Store | Manages log segments and storage lifecycle |
| Replicator | Supports log replication |
| Server | Exposes service APIs |
| Authentication | TLS and certificate-based authentication |
| Authorization | Policy-based access control |
| Load balancing | Client-side node selection and resolution |

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Go | Core implementation |
| gRPC | RPC communication |
| Protocol Buffers | API and message definitions |
| Raft | Distributed consensus |
| Serf | Cluster membership and failure detection |
| TLS / mTLS | Secure communication |
| Casbin | Authorization |
| Zap | Structured logging |
| OpenCensus | Current metrics and tracing instrumentation |
| Viper | Configuration management |
| Cobra | Command-line interface |
| Docker | Containerization |
| Kubernetes | Container orchestration |
| Helm | Kubernetes deployment management |

---

## Repository Structure

```text
scribe/
│
├── api/
│   └── v1/                       # Protocol Buffer API definitions
│
├── cmd/
│   ├── client/                   # Client command
│   ├── cluster/                  # Cluster discovery and inspection CLI
│   ├── scribe/                   # Main Scribe executable
│   ├── scribe-server/            # Distributed node startup command
│   ├── server/                   # Server command
│   └── main.go
│
├── deploy/
│   └── scribe/                   # Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── service.yaml
│           ├── statefulset.yaml
│           └── _helpers.tpl
│
├── docs/                         # Architecture and technical documentation
│
├── internal/
│   ├── agent/                    # Agent and cluster lifecycle
│   │   ├── agent.go
│   │   └── cluster.go
│   │
│   ├── auth/                     # Authorization
│   │   └── authorizer.go
│   │
│   ├── config/                   # Configuration and TLS
│   │   ├── files.go
│   │   └── tls.go
│   │
│   ├── discovery/                # Serf membership
│   │   └── membership.go
│   │
│   ├── loadbalance/              # Client-side load balancing
│   │   ├── picker.go
│   │   └── resolver.go
│   │
│   ├── log/                      # Distributed log and storage
│   │   ├── config.go
│   │   ├── distributed.go
│   │   ├── replicator.go
│   │   ├── log.go
│   │   ├── segment.go
│   │   ├── index.go
│   │   └── store.go
│   │
│   └── server/                   # Server implementation
│       ├── server.go
│       ├── http.go
│       └── log.go
│
├── test/                         # Test and authorization configuration
│   ├── ca-config.json
│   ├── ca-csr.json
│   ├── client-csr.json
│   ├── server-csr.json
│   ├── model.conf
│   └── policy.csv
│
├── data/                         # Local runtime data
├── data-backup/                  # Local data backups
│
├── Dockerfile
├── Makefile
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

The `internal/` directory contains the core implementation. Go prevents packages outside the module's allowed import scope from importing these internal packages.

---

## Prerequisites

Install the following tools:

| Tool | Purpose |
|---|---|
| Go | Build and run Scribe |
| Protocol Buffers compiler (`protoc`) | Compile API definitions |
| `protoc-gen-go` | Generate Go message types |
| `protoc-gen-go-grpc` | Generate gRPC service code |
| CFSSL | Generate development certificates |
| `cfssljson` | Write generated certificates and keys |
| Docker | Build container images |
| kubectl | Interact with Kubernetes |
| Helm | Deploy Scribe |

The Dockerfile currently uses `golang:1.27-alpine` as its build image. Use a compatible local Go version and keep the development, CI, and container build environments aligned with `go.mod`.

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/IamRitwik/scribe.git
cd scribe
```

### 2. Download dependencies

```bash
go mod download
```

### 3. Initialize the configuration directory

```bash
make init
```

This creates:

```text
~/.scribe/
```

Scribe uses this directory for local configuration and generated development certificates.

### 4. Generate development certificates

```bash
make gencert
```

This generates the CA, server, and client certificates, including additional client identities used by authorization tests.

The generated files are moved into:

```text
~/.scribe/
```

### 5. Run the test suite

```bash
make test
```

This runs all Go tests with the race detector enabled.

---

## Running a Scribe Cluster

The `scribe-server` executable starts a Scribe node.

It loads configuration from YAML, applies environment variables and CLI settings, initializes TLS, creates the agent, and waits for a shutdown signal.

### 1. Build the server

```bash
mkdir -p bin

go build -o bin/scribe-server ./cmd/scribe-server
```

### 2. Prepare node configurations

Create three configuration files:

```text
~/.scribe/
├── node-1.yaml
├── node-2.yaml
└── node-3.yaml
```

Each node requires a unique identity and persistent data directory.

The following is an illustrative configuration for node 1. Adjust certificate paths and advertised addresses to match your environment.

**`~/.scribe/node-1.yaml`**

```yaml
node-name: node-1

data-dir: ~/.scribe/data/node-1

bind-addr: 127.0.0.1:8401

rpc-port: 8400

rpc-advertise-host: 127.0.0.1

start-join-addrs: []

bootstrap: true

acl-model-file: ~/.scribe/model.conf
acl-policy-file: ~/.scribe/policy.csv

server-tls-cert-file: ~/.scribe/server.pem
server-tls-key-file: ~/.scribe/server-key.pem
server-tls-ca-file: ~/.scribe/ca.pem

peer-tls-cert-file: ~/.scribe/server.pem
peer-tls-key-file: ~/.scribe/server-key.pem
peer-tls-ca-file: ~/.scribe/ca.pem

peer-tls-server-address: 127.0.0.1
```

**Important:** This is a configuration template, not a verified ready-to-run three-node configuration. The actual node addresses, certificate identities, and peer TLS verification names must match your environment. In particular, use distinct ports and advertise addresses for each local node.

For a three-node local cluster, a typical port allocation is:

| Node | Serf bind address | RPC / Raft port |
|---|---|---:|
| node-1 | `127.0.0.1:8401` | `8400` |
| node-2 | `127.0.0.1:8403` | `8402` |
| node-3 | `127.0.0.1:8405` | `8404` |

Node 1 bootstraps the new Raft cluster. Nodes 2 and 3 join the existing cluster through Serf.

### 3. Start the initial node

In terminal 1:

```bash
./bin/scribe-server \
  --config-file "$HOME/.scribe/node-1.yaml"
```

The initial node should have:

```yaml
bootstrap: true
```

### 4. Start the remaining nodes

In terminal 2:

```bash
./bin/scribe-server \
  --config-file "$HOME/.scribe/node-2.yaml"
```

In terminal 3:

```bash
./bin/scribe-server \
  --config-file "$HOME/.scribe/node-3.yaml"
```

The joining nodes should use:

```yaml
bootstrap: false

start-join-addrs:
  - "127.0.0.1:8401"
```

Each node must have its own unique `node-name`, `data-dir`, Serf bind address, and RPC port.

### 5. Graceful shutdown

Scribe handles `SIGINT` and `SIGTERM`.

For example, pressing `Ctrl+C` sends an interrupt signal. The server then invokes:

```go
a.Shutdown()
```

This allows the agent to perform its graceful shutdown procedure.

---

## Cluster Discovery and Inspection

Scribe includes a cluster diagnostic command that queries nodes through the gRPC `GetServers` API.

It connects to known bootstrap addresses, discovers additional advertised RPC addresses, queries those nodes, and merges their reported cluster views.

### Build the cluster diagnostic CLI

```bash
go build -o bin/scribe-client ./cmd/cluster
```

### Inspect the cluster

```bash
./bin/scribe-client \
  -addrs "127.0.0.1:8400,127.0.0.1:8402,127.0.0.1:8404"
```

You can also configure the per-node timeout:

```bash
./bin/scribe-client \
  -addrs "127.0.0.1:8400,127.0.0.1:8402,127.0.0.1:8404" \
  -timeout 5s
```

### Example report

The diagnostic command reports information in a table similar to:

```text
SCRIBE CLUSTER
====================================================================

NODE     RAFT MEMBER   RAFT ROLE   LEADER   SERF STATUS   SERF ONLINE   RPC ADDRESS       RPC REACHABLE
node-1   yes           leader      YES      alive         yes           127.0.0.1:8400    yes
node-2   yes           follower    no       alive         yes           127.0.0.1:8402    yes
node-3   yes           follower    no       alive         yes           127.0.0.1:8404    yes

--------------------------------------------------------------------
Total nodes in combined view:  3
Raft members:                  3
Serf-alive members:            3
Current leader:                node-1
====================================================================
```

The values above are illustrative, not live cluster output.

### What the report tells you

| Field | Meaning |
|---|---|
| NODE | Node identity |
| RAFT MEMBER | Whether the node is a Raft member |
| RAFT ROLE | Reported Raft role |
| LEADER | Whether the node reports itself as leader |
| SERF STATUS | Membership status |
| SERF ONLINE | Whether the node is reported online |
| RPC ADDRESS | Advertised RPC address |
| RPC REACHABLE | Whether the diagnostic CLI successfully contacted that address |

**Important:** This report is a combined view from queried nodes, not an atomic cluster snapshot. During failures or membership changes, different nodes may temporarily report different views. RPC reachability also does not imply Raft health or successful participation in quorum.

---

## Configuration Reference

The `scribe-server` command supports CLI flags, environment variables, and YAML configuration.

The environment variable prefix is `SCRIBE_`, and hyphens in configuration names are replaced with underscores.

For example:

```bash
SCRIBE_NODE_NAME=node-1
SCRIBE_RPC_PORT=8400
SCRIBE_BOOTSTRAP=true
```

### Available configuration settings

| Setting | Description | Default |
|---|---|---|
| `config-file` | YAML configuration path | `~/.scribe/config.yaml` |
| `data-dir` | Persistent data directory | `./data/scribe` |
| `node-name` | Unique node identity | Empty |
| `bind-addr` | Serf membership bind address | `127.0.0.1:8401` |
| `rpc-port` | Shared gRPC and Raft TCP port | `8400` |
| `rpc-advertise-host` | Advertised host for RPC and Raft | Derived from bind address |
| `start-join-addrs` | Existing Serf members to join | Empty |
| `bootstrap` | Bootstrap a new Raft cluster | `false` |
| `acl-model-file` | Casbin model path | Empty |
| `acl-policy-file` | Casbin policy path | Empty |
| `server-tls-cert-file` | Server certificate | Empty |
| `server-tls-key-file` | Server private key | Empty |
| `server-tls-ca-file` | CA used to verify clients | Empty |
| `peer-tls-cert-file` | Raft peer certificate | Empty |
| `peer-tls-key-file` | Raft peer private key | Empty |
| `peer-tls-ca-file` | CA used to verify Raft peers | Empty |
| `peer-tls-server-address` | Expected Raft peer certificate name | `127.0.0.1` |

The server requires a YAML configuration file to be readable. The individual settings can then be overridden through environment variables or CLI flags.

---

## TLS and Security

Scribe uses TLS to encrypt communication and authenticate peers.

The configuration separates two communication paths.

### Client-facing server TLS

The server-facing gRPC listener uses:

- Server certificate
- Server private key
- CA certificate used to verify client certificates

### Raft peer TLS

The Raft transport uses a separate TLS configuration:

- Peer certificate
- Peer private key
- CA certificate used to verify Raft peers
- Expected peer server name or address

Both configurations require certificate, private key, and CA paths.

The server enforces a minimum TLS version of TLS 1.2.

### Authorization

Scribe uses Casbin for authorization.

The authorization model and policy are maintained in:

```text
test/model.conf
test/policy.csv
```

The Makefile copies these into:

```text
~/.scribe/model.conf
~/.scribe/policy.csv
```

The development certificate and authorization configuration should not be treated as production credentials or production security policy.

**Never commit private keys, production certificates, or secrets to Git.**

---

## Testing

Scribe uses Go's standard testing framework.

Run the full test suite:

```bash
make test
```

The Makefile executes:

```bash
go test -v -race ./...
```

This enables:

- Verbose test output
- Race-condition detection
- Tests across all Go packages

The Makefile also ensures the Casbin model and policy files exist before running the tests.

Individual packages can be tested directly:

```bash
go test -v -race ./internal/log
go test -v -race ./internal/agent
go test -v -race ./internal/discovery
go test -v -race ./internal/server
go test -v -race ./internal/loadbalance
```

---

## Docker

Scribe includes a multi-stage Dockerfile.

The build stage:

1. Uses the official Go Alpine image.
2. Copies the source code.
3. Builds the main Scribe executable from `cmd/scribe`.
4. Installs the gRPC health probe.

The runtime stage uses `scratch`, resulting in a minimal container image.

### Build the image

```bash
make build-docker
```

The default image is:

```text
github.com/iamritwik/scribe:0.0.1
```

Override the tag:

```bash
make build-docker TAG=0.0.2
```

The Docker image entrypoint is:

```text
/bin/scribe
```

The Dockerfile currently builds `cmd/scribe`, whereas the local cluster startup procedure documented above uses `cmd/scribe-server`. These are distinct entrypoints; the Docker image should not be assumed to launch the same CLI without checking `cmd/scribe/main.go`.

---

## Kubernetes Deployment

Scribe includes a Helm chart:

```text
deploy/scribe/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── service.yaml
    ├── statefulset.yaml
    └── _helpers.tpl
```

The chart's default values are:

| Setting | Default |
|---|---|
| Image repository | `github.com/iamritwik/scribe` |
| Image tag | `0.0.1` |
| Image pull policy | `IfNotPresent` |
| Serf port | `8401` |
| RPC port | `8400` |
| Replicas | `3` |
| Storage | `1Gi` |

### Deploy Scribe

```bash
make deploy
```

The deployment target runs three operations:

1. `secret` — creates or updates the Kubernetes Secret containing certificates and authorization configuration.
2. `load-image` — loads the locally built Docker image into the local Kubernetes cluster.
3. `helm upgrade --install` — installs or upgrades the Scribe release.

The underlying Helm command is:

```bash
helm upgrade --install scribe ./deploy/scribe
```

### Customize the image tag

```bash
make build-docker TAG=0.0.2
make deploy TAG=0.0.2
```

The Kubernetes deployment assumes that Docker, kubectl, Helm, the expected local Kubernetes cluster, and the required certificates are available.

---

## Development Commands

The Makefile provides the following targets:

| Command | Description |
|---|---|
| `make init` | Creates `~/.scribe/` |
| `make gencert` | Generates development CA, server, and client certificates |
| `make test` | Runs all Go tests with the race detector |
| `make compile` | Regenerates Go and gRPC code from Protocol Buffer definitions |
| `make build-docker` | Builds the Docker image |
| `make secret` | Creates or updates the Kubernetes configuration Secret |
| `make load-image` | Imports the Docker image into the local Kubernetes cluster |
| `make deploy` | Creates deployment prerequisites and installs/upgrades the Helm release |

Regenerate Protocol Buffer code:

```bash
make compile
```

Run all tests:

```bash
make test
```

Build the Docker image:

```bash
make build-docker
```

Deploy:

```bash
make deploy
```

---

## Roadmap

The long-term goal is to evolve Scribe into a reliable distributed event-processing platform.

- [x] Append-only log storage
- [x] Segmented storage
- [x] Log indexing
- [x] gRPC service API
- [x] TLS and mTLS
- [x] Serf-based membership and discovery
- [x] Raft integration
- [x] Client-side load balancing
- [x] Authorization
- [ ] Comprehensive failure and recovery testing
- [ ] Production-grade operational hardening

---

## Acknowledgements

Scribe is inspired by:

**[*Distributed Services with Go*](https://pragprog.com/titles/tjgo/distributed-services-with-go/)** by Travis Jeffrey.

The book provides an excellent practical foundation for understanding how distributed services are designed and implemented in Go.

Scribe is an independent learning and engineering project that builds on these concepts.

---

## License

Scribe is released under the MIT License. See [LICENSE](LICENSE) for details.