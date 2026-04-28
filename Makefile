GO ?= $(shell command -v go 2>/dev/null || echo /usr/local/go/bin/go)
MODULE=$(shell $(GO) list -m)
BINARY_NAME=$(shell basename $(MODULE))
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u '+%Y-%m-%d %H:%M:%S')
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

LDFLAGS=-ldflags "-X '$(MODULE).Version=$(VERSION)' -X '$(MODULE).BuildTime=$(BUILD_TIME)' -X '$(MODULE).GitCommit=$(GIT_COMMIT)' -X '$(MODULE).GitBranch=$(GIT_BRANCH)'"

# Allow turning off function inlining and variable registerization
ifeq ($(DISABLE_OPTIMIZATION),true)
	GO_GCFLAGS=-gcflags "-N -l"
	VERSION:="$(VERSION)-noopt"
endif

GO_TAGS=$(if $(BUILDTAGS),-tags "$(BUILDTAGS)",)



##@ General

all: clean fmt vet lint test build examples ## Build and run all

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

.PHONY: mod-download
mod-download: ## Download dependencies
	@$(GO) mod download

.PHONY: mod-tidy
mod-tidy: ## Tidy dependencies
	@$(GO) mod tidy

.PHONY: fmt
fmt: mod-tidy $(LINT_BIN) ## Format code
	@$(GO) fmt ./...
	@$(LINT_BIN) run --disable-all -E gofumpt,gci,tagalign,wsl --fix --no-config

.PHONY: vet
vet: ## Run go vet
	@$(GO) vet ./...

.PHONY: build
build: ## Run go build
	@GOOS=$(OS) GOARCH=$(ARCH) $(GO) build -v -trimpath $(GO_GCFLAGS) $(LDFLAGS) $(GO_TAGS) -o ./bin/$(BINARY_NAME) ./cmd/tsq

.PHONY: run
test: ## Run tests
	@$(GO) test -v ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@$(GO) test -v -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out

.PHONY: clean
clean: ## Clean build artifacts
	@rm -f bin/$(BINARY_NAME)
	@rm -f coverage.out

.PHONY: install
install: build ## Install to GOPATH/bin
	@cp bin/$(BINARY_NAME) $$($(GO) env GOPATH)/bin/

.PHONY: examples
examples: build ## Regenerate and build examples program
	@rm -f ./examples/database/*_tsq.go
	@./bin/$(BINARY_NAME) gen -v $(MODULE)/examples/database
	@$(GO) build -o bin/examples ./examples/

.PHONY: update-examples
update-examples: examples ## Update examples code

.PHONY: update-sample
update-sample: update-examples ## Backward-compatible alias for update-examples


help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target.env>\033[0m\n"} /^[a-zA-Z_0-9\-\\.% ]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

define go-get-tool
	@[ -f $(1) ] || { \
	set -e ;\
	TMP_DIR=$$(mktemp -d) ;\
	cd $$TMP_DIR ;\
	$(GO) mod init tmp ;\
	echo "Downloading $(2)" ;\
	GOBIN=$(LOCALBIN) $(GO) install $(2) ;\
	rm -rf $$TMP_DIR ;\
	}
endef
