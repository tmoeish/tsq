MODULE=$(shell go list -m)
BINARY_NAME=$(shell basename $(MODULE))
OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u '+%Y-%m-%d %H:%M:%S')

LDFLAGS=-ldflags "-X '$(MODULE)/pkg/tsq.Version=$(VERSION)' -X '$(MODULE)/pkg/tsq.BuildTime=$(BUILD_TIME)'"

# Allow turning off function inlining and variable registerization
ifeq ($(DISABLE_OPTIMIZATION),true)
	GO_GCFLAGS=-gcflags "-N -l"
	VERSION:="$(VERSION)-noopt"
endif

GO_TAGS=$(if $(BUILDTAGS),-tags "$(BUILDTAGS)",)



##@ General

all: clean fmt vet lint test build update-sample ## Build and run all

PROJECT_DIR = $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
LOCALBIN = ${PROJECT_DIR}/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

LINT_BIN = $(LOCALBIN)/golangci-lint
$(LINT_BIN): $(LOCALBIN)
	$(call go-get-tool,$(LINT_BIN),github.com/golangci/golangci-lint/cmd/golangci-lint@master)

.PHONY: lint
lint: $(LINT_BIN) ## Run golangci-lint
	@$(LINT_BIN) run

.PHONY: mod-tidy
mod-tidy: ## Tidy dependencies
	@go mod tidy

.PHONY: fmt
fmt: mod-tidy $(LINT_BIN) ## Format code
	@go fmt ./...
	@$(LINT_BIN) run --disable-all -E gofumpt,gci,tagalign,wsl --fix --no-config

.PHONY: vet
vet: ## Run go vet
	@go vet ./...

.PHONY: build
build: ## Run go build
	@GOOS=$(OS) GOARCH=$(ARCH) go build -v -trimpath $(GO_GCFLAGS) $(GO_LDFLAGS) $(GO_TAGS) -o ./bin/$(BINARY_NAME) ./cmd/tsq

.PHONY: run
test: ## Run tests
	@go test -v ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: clean
clean: ## Clean build artifacts
	@rm -f bin/$(BINARY_NAME)
	@rm -f coverage.out

.PHONY: install
install: build ## Install to GOPATH/bin
	@cp bin/$(BINARY_NAME) $$(go env GOPATH)/bin/

.PHONY: update-examples
update-examples: build ## Update examples code
	@./bin/$(BINARY_NAME) gen -v $(MODULE)/examples/database
	@go build -o bin/examples ./examples/


help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target.env>\033[0m\n"} /^[a-zA-Z_0-9\-\\.% ]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

define go-get-tool
@[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
GOBIN=$(LOCALBIN) go install $(2) ;\
rm -rf $$TMP_DIR ;\
}
endef
