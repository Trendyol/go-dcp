.PHONY: default test

default: init

init:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.55.2
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.15.0

clean:
	rm -rf ./build

linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

test:
	go test ./... .

race:
	CB_VERSION=7.6.1 go test ./... -race .

tidy:
	go mod tidy
	cd example && go mod tidy && cd ..
	cd example/grafana && go mod tidy && cd ../..
	cd test/integration && go mod tidy && cd ../..

create-cluster:
	bash scripts/create_cluster.sh --version $(version)

delete-cluster:
	bash scripts/delete_cluster.sh

docker-build:
	docker build --progress=plain -t docker.io/trendyoltech/dcp .