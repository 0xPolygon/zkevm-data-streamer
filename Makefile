arguments := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
GOENVVARS := CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH)

# Check dependencies
# Check for Go
.PHONY: check-go
check-go:
	@which go > /dev/null || (echo "Error: Go is not installed" && exit 1)

# Check for Docker
.PHONY: check-docker
check-docker:
	@which docker > /dev/null || (echo "Error: docker is not installed" && exit 1)

# Targets that require the checks
run-server: check-go
build-dsapp: check-go
build-dsrelay: check-go
build-docker: check-docker
build-docker-nc: check-docker

.PHONY: install-linter
install-linter: ## Installs the linter
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.52.2

.PHONY: lint
lint: ## Runs the linter
	export "GOROOT=$$(go env GOROOT)" && $$(go env GOPATH)/bin/golangci-lint run

.PHONY: build-dsapp
build-dsapp: ## Builds datastream demo cli app (server, client, relay)
	$(GOENVVARS) go build -o dsapp cmd/main.go

.PHONY: build-dsrelay
build-dsrelay: ## Builds datastream relay binary into ./dist
	$(GOENVVARS) go build -o dist/dsrelay relay/main.go

.PHONY: build-docker
build-docker: ## Builds a docker image with datastream relay binary
	docker build -t datastream-relay -f ./Dockerfile .

.PHONY: build-docker-nc
build-docker-nc: ## Builds a docker image with datastream relay binary but without build cache
	docker build --no-cache=true -t datastream-relay -f ./Dockerfile .

.PHONY: test
test:
	go test -count=1 -short -race -p 1 -timeout 60s ./...

## Help display.
## Pulls comments from beside commands and prints a nicely formatted
## display with the commands and their usage information.
.DEFAULT_GOAL := help

.PHONY: help
help: ## Prints this help
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| sort \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
