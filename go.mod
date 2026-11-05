module github.com/IamRitwik/scribe

go 1.25.4

require (
	github.com/casbin/casbin/v2 v2.130.0 // NOT v1 — see earlier fix
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/go-metrics v0.7.0 // for the hashicorpmetrics build tag
	github.com/hashicorp/raft v1.3.9
	github.com/hashicorp/raft-boltdb/v2 v2.2.2 // NOT the plain raft-boltdb, NOT the book's replace-directive fork
	github.com/soheilhy/cmux v0.1.5
	github.com/tysonmote/gommap v0.0.2 // NOT tysontate — that path is stale
)
