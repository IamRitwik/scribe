# START: begin
CONFIG_PATH=${HOME}/.scribe/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
# END: begin

# START: client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		test/client-csr.json | cfssljson -bare client
# END: client

# START: multi_client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client
# END: multi_client

# START: begin
	mv *.pem *.csr ${CONFIG_PATH}

# END: begin
# START: auth
$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

# START: begin
.PHONY: test
# END: auth
test:
# END: begin
# START: auth
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
#: START: begin
	go test -v -race ./...
# END: auth

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

# END: begin

TAG ?= 0.0.1

build-docker:
	docker build -t github.com/iamritwik/scribe:$(TAG) .

.PHONY: secret
secret: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv
	kubectl create secret generic scribe-config \
		--from-file=ca.pem=$(CONFIG_PATH)/ca.pem \
		--from-file=server.pem=$(CONFIG_PATH)/server.pem \
		--from-file=server-key.pem=$(CONFIG_PATH)/server-key.pem \
		--from-file=root-client.pem=$(CONFIG_PATH)/root-client.pem \
		--from-file=root-client-key.pem=$(CONFIG_PATH)/root-client-key.pem \
		--from-file=model.conf=$(CONFIG_PATH)/model.conf \
		--from-file=policy.csv=$(CONFIG_PATH)/policy.csv \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: load-image
load-image:
	docker save github.com/iamritwik/scribe:$(TAG) | docker exec -i desktop-control-plane ctr --namespace=k8s.io images import -

.PHONY: deploy
deploy: secret load-image
	helm upgrade --install scribe ./deploy/scribe
