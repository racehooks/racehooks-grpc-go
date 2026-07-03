.PHONY: tools generate lint vet fmt test cover build all

# Pin codegen tool versions for reproducible stubs.
BUF_VERSION            ?= v1.71.0
PROTOC_GEN_GO_VERSION  ?= latest
PROTOC_GEN_GRPC_VERSION?= latest

tools: ## Install codegen tools (buf + protoc plugins) into $GOBIN
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GRPC_VERSION)

generate: ## Regenerate Go stubs from the proto (committed under internal/)
	buf generate proto

lint: vet fmt ## buf lint + go vet + gofmt check
	buf lint proto

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run: gofmt -w ." && exit 1)

test: ## Run tests with the race detector
	go test -race -count=1 ./...

cover: ## Test coverage summary
	go test -cover -count=1 ./...

build:
	go build ./...

all: lint test build
