SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose -f deploy/docker-compose.yml
POSTGRES_DSN ?= postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable
BIN := bin

# The race detector needs cgo, which is unavailable on machines with no C
# toolchain. Tests still run there; CI enables it.
ifeq ($(shell go env CGO_ENABLED),1)
RACE := -race
endif

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## --- dependencies -----------------------------------------------------------

.PHONY: up
up: ## Start PostgreSQL and Redis and wait for them to be healthy
	$(COMPOSE) up -d --wait

.PHONY: down
down: ## Stop the dependency stack, keeping data
	$(COMPOSE) down

.PHONY: clean-data
clean-data: ## Stop the dependency stack and delete its volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail dependency logs
	$(COMPOSE) logs -f

## --- code generation --------------------------------------------------------

.PHONY: generate
generate: proto sqlc ## Run every code generator

.PHONY: proto
proto: ## Generate gRPC stubs from proto/
	buf lint
	buf generate

.PHONY: sqlc
sqlc: ## Generate type-safe query code from internal/store/queries/
	@if [ -f sqlc.yaml ]; then sqlc generate; else echo "sqlc.yaml not present yet, skipping"; fi

## --- database ---------------------------------------------------------------

.PHONY: migrate-up
migrate-up: ## Apply all pending migrations
	migrate -path migrations -database "$(POSTGRES_DSN)" up

.PHONY: migrate-down
migrate-down: ## Roll back the most recent migration
	migrate -path migrations -database "$(POSTGRES_DSN)" down 1

.PHONY: migrate-new
migrate-new: ## Create a migration pair: make migrate-new NAME=add_something
	@test -n "$(NAME)" || { echo "NAME is required, e.g. make migrate-new NAME=add_jobs"; exit 1; }
	migrate create -ext sql -dir migrations -seq $(NAME)

## --- build and test ---------------------------------------------------------

.PHONY: build
build: ## Build every binary into bin/
	go build -o $(BIN)/ ./cmd/...

.PHONY: test
test: ## Run unit tests
	go test ./... $(RACE) -count=1

.PHONY: test-integration
test-integration: ## Run integration tests against throwaway containers
	go test ./... $(RACE) -count=1 -tags=integration -timeout=15m

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -s -w .

.PHONY: vet
vet: ## Run go vet, and staticcheck when it is installed
	go vet ./...
	@if command -v staticcheck >/dev/null; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

.PHONY: tidy
tidy: ## Tidy and verify module dependencies
	go mod tidy
	go mod verify

.PHONY: demo
demo: ## Run an end-to-end demonstration against the local stack
	bash scripts/demo.sh

.PHONY: check
check: fmt vet test ## Format, vet and test -- run this before committing
