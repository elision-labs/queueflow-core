.PHONY: help build run test test-pg check-package fmt fmt-check lint clippy clean docker spec validate-spec \
        sdks sdks-python sdks-typescript sdks-go sdks-rust check-ts-sdk

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
	$(CARGO) run -p queueflow -- serve

test: ## Run unit + integration tests (no database needed)
	$(CARGO) test --workspace

test-pg: ## Run Postgres integration tests (needs TEST_DATABASE_URL; any plain PostgreSQL 13+)
	REQUIRE_PG=1 $(CARGO) test -p queueflow-core --features postgres -- --include-ignored

check-package: ## Prove the packaged queueflow-core builds standalone (catches files missing from the .crate)
	$(CARGO) package -p queueflow-core --allow-dirty
	# Extract OUTSIDE the workspace tree: in-tree, the standalone build walks
	# up, finds this workspace manifest, and refuses to build a non-member.
	rm -rf "$${TMPDIR:-/tmp}/queueflow-package-check" && mkdir -p "$${TMPDIR:-/tmp}/queueflow-package-check"
	tar -xzf target/package/queueflow-core-$$($(CARGO) pkgid -p queueflow-core | sed 's/.*#//').crate -C "$${TMPDIR:-/tmp}/queueflow-package-check"
	cd "$${TMPDIR:-/tmp}/queueflow-package-check"/queueflow-core-* && $(CARGO) build --features postgres

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
	$(CARGO) run -q -p queueflow -- spec --output-dir $(SPEC_DIR)

validate-spec: spec ## Validate the generated spec with openapi-generator
	docker run --rm -v "$(CURDIR)/$(SPEC_DIR):/spec:ro" \
		$(OPENAPI_IMAGE) validate -i /spec/openapi.json

sdks: validate-spec ## Regenerate the Python + Go + Rust SDKs (generated core + injected facade). TS = own repo.
	./scripts/generate-sdks.sh all

sdks-python: validate-spec ## Regenerate the Python SDK (generated core + facade supporting file)
	./scripts/generate-sdks.sh python

sdks-typescript: ## Regenerate the TS core (../queueflow-sdk-nodejs) and build the facade
	cd ../queueflow-sdk-nodejs && npm install && npm run generate-core && npm run build

sdks-go: validate-spec ## Regenerate the Go SDK (generated core + facade supporting file)
	./scripts/generate-sdks.sh go

sdks-rust: validate-spec ## Regenerate the Rust SDK (generated core + facade supporting file)
	./scripts/generate-sdks.sh rust

check-ts-sdk: spec ## Verify the TS generated core + facade match the spec
	node ./scripts/check-ts-sdk.mjs
