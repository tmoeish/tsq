GO ?= $(shell command -v go 2>/dev/null || echo /usr/local/go/bin/go)
MODULE=$(shell $(GO) list -m)
BINARY_NAME=tsq
GEN_BINARY_NAME=tsq-gen
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u '+%Y-%m-%d %H:%M:%S')
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

LDFLAGS=-ldflags "-X '$(MODULE)/internal/buildinfo.version=$(VERSION)' -X '$(MODULE)/internal/buildinfo.buildTime=$(BUILD_TIME)' -X '$(MODULE)/internal/buildinfo.gitCommit=$(GIT_COMMIT)' -X '$(MODULE)/internal/buildinfo.gitBranch=$(GIT_BRANCH)'"

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
FMT_FIX_LINTERS = modernize,tagalign,wsl_v5
$(LINT_BIN): $(LOCALBIN)
	$(call go-get-tool,$(LINT_BIN),github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1)

.PHONY: lint
lint: $(LINT_BIN) ## Run golangci-lint
	@$(LINT_BIN) run

.PHONY: mod-download
mod-download: ## Download dependencies
	@$(GO) mod download

.PHONY: mod-tidy
mod-tidy: ## Tidy dependencies
	@$(GO) mod tidy

# Every step here rewrites source. `go fix` and `--fix` apply semantic rewrites, not
# just formatting, so the trailing `go build` is not ceremony: formatting must never
# hand back a tree that does not compile, and without the guard a bad rewrite is only
# noticed later by `make lint`, which reports it as a typecheck error far from its cause.
.PHONY: fmt
fmt: mod-tidy $(LINT_BIN) ## Format code
	@$(GO) fix ./...
	@$(LINT_BIN) fmt -c .golangci.yml
	@$(LINT_BIN) run --fix --issues-exit-code=0 --enable-only $(FMT_FIX_LINTERS)
	@$(GO) build ./...

.PHONY: vet
vet: ## Run go vet
	@$(GO) vet ./...

.PHONY: build
build: ## Run go build
	@GOOS=$(OS) GOARCH=$(ARCH) $(GO) build -v -trimpath $(GO_GCFLAGS) $(LDFLAGS) $(GO_TAGS) -o ./bin/$(BINARY_NAME) ./cmd/tsq

# Built WITHOUT $(LDFLAGS) on purpose. The generated file header records the TSQ version,
# and $(VERSION) comes from `git describe`, which cannot know the version being released
# (the tag does not exist yet) and differs between a clean checkout and a dirty tree.
# Generation must be reproducible from the sources alone, so the generator used by
# `make examples` and `make gen-check` reports internal/buildinfo's literal instead.
.PHONY: build-gen
build-gen: ## Build the generator used for reproducible codegen (no version ldflags)
	@GOOS=$(OS) GOARCH=$(ARCH) $(GO) build -trimpath $(GO_GCFLAGS) $(GO_TAGS) -o ./bin/$(GEN_BINARY_NAME) ./cmd/tsq

.PHONY: test
test: ## Run tests
	@$(GO) test ./...

.PHONY: test-race
test-race: ## Run tests with the race detector and shuffled order
	@$(GO) test -race -shuffle=on -count=1 ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@$(GO) test -v -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out

.PHONY: clean
clean: ## Clean build artifacts
	@rm -f bin/$(BINARY_NAME) bin/$(GEN_BINARY_NAME)
	@rm -f coverage.out

.PHONY: install
install: build ## Install to GOPATH/bin
	@cp bin/$(BINARY_NAME) $$($(GO) env GOPATH)/bin/

.PHONY: examples
examples: build-gen ## Regenerate and build examples programs
	@rm -f ./examples/academy/*.tsq.go ./examples/academy/*.result.tsq.go ./examples/academy/mysql.sql ./examples/academy/postgres.sql ./examples/academy/sqlite.sql ./examples/academy/ddl.json
	@./bin/$(GEN_BINARY_NAME) gen -v $(MODULE)/examples/academy
	@rm -rf ./bin/examples
	@mkdir -p ./bin/examples
	@$(GO) build -o ./bin/examples/quickstart ./examples/quickstart
	@$(GO) build -o ./bin/examples/advanced ./examples/advanced
	@$(GO) build -o ./bin/examples/full-suite ./examples/full-suite


##@ Agent harness

.PHONY: hooks
hooks: ## Install this repo's git hooks (once per machine)
	@python3 script/install_hooks.py

.PHONY: memory-check
memory-check: ## Require uncommitted functional changes to carry a project-memory entry
	@python3 script/check_change_log.py memory

.PHONY: commit-check
commit-check: ## Require this wave's commit message to explain the change
	@python3 script/check_change_log.py commit

.PHONY: skill-check
skill-check: ## Require skills/tsq and .agents/skills/tsq-dev to track the code
	@python3 script/check_skills.py

.PHONY: gen-check
gen-check: build-gen ## Verify examples/academy is what the current sources generate
	@python3 script/check_generated.py

.PHONY: api-snapshot
api-snapshot: ## Rewrite the public Go API snapshot
	@python3 script/check_api_surface.py write

.PHONY: api-check
api-check: ## Fail when the public Go API drifts from its committed snapshot
	@python3 script/check_api_surface.py check

.PHONY: release-check
release-check: ## Verify buildinfo, CHANGELOG, generated headers and tags agree
	@python3 script/check_release.py

.PHONY: examples-run
examples-run: examples ## Regenerate examples and run the full-suite example end to end
	@./bin/examples/full-suite > /dev/null

.PHONY: harness
harness: skill-check memory-check lint vet gen-check api-check release-check test test-race examples-run commit-check ## Run every deterministic gate a coding agent must pass before handoff

.PHONY: release
release: ## Cut a release: bump, changelog, regenerate, harness, commit, tag, push
	@python3 script/release.py $(RELEASE_ARGS)

.PHONY: release-dry-run
release-dry-run: ## Print the version and changelog entry a release would produce
	@python3 script/release.py --dry-run

##@ Help

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
