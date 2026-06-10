.PHONY: help build run test test-pg fmt fmt-check lint clippy clean docker spec validate-spec \
        sdks sdks-python sdks-typescript sdks-go check-ts-sdk

CARGO ?= cargo
BIN := queueflow
SPEC_DIR := spec
DOCKER_REGISTRY ?= ghcr.io/queueflow
DOCKER_TAG ?= dev
OPENAPI_IMAGE := openapitools/openapi-generator-cli:v7.10.0

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all crates (release)
	$(CARGO) build --workspace --release

run: ## Run the server (needs DATABASE_URL)
	$(CARGO) run -p queueflow-server -- serve

test: ## Run unit + integration tests (no database needed)
	$(CARGO) test --workspace

test-pg: ## Run Postgres integration tests (needs TEST_DATABASE_URL; any plain PostgreSQL 13+)
	$(CARGO) test -p queueflow-core --features postgres -- --include-ignored

fmt: ## Format the code
	$(CARGO) fmt --all

fmt-check: ## Check formatting
	$(CARGO) fmt --all --check

lint: clippy ## Alias for clippy

clippy: ## Lint with clippy (warnings are errors)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

clean: ## Remove build artifacts
	$(CARGO) clean

docker: ## Build the server docker image
	docker build -t $(DOCKER_REGISTRY)/$(BIN):$(DOCKER_TAG) .

## OpenAPI / SDKs

spec: ## Generate the OpenAPI spec from code into ./spec
	$(CARGO) run -q -p queueflow-server -- spec --output-dir $(SPEC_DIR)

validate-spec: spec ## Validate the generated spec with openapi-generator
	docker run --rm -v "$(CURDIR)/$(SPEC_DIR):/spec:ro" \
		$(OPENAPI_IMAGE) validate -i /spec/openapi.json

sdks: validate-spec ## Regenerate on-demand generated SDKs (python, go). Rust = crates/queueflow-client; TS = hand-written.
	./scripts/generate-sdks.sh all

sdks-python: validate-spec ## Regenerate the Python SDK (on demand)
	./scripts/generate-sdks.sh python

sdks-typescript: ## (hand-written) build the TypeScript SDK in ../queueflow-sdk-nodejs
	cd ../queueflow-sdk-nodejs && npm install && npm run build

sdks-go: validate-spec ## Regenerate the Go SDK (on demand)
	./scripts/generate-sdks.sh go

check-ts-sdk: spec ## Verify the hand-written TS SDK still matches the spec
	node ./scripts/check-ts-sdk.mjs
