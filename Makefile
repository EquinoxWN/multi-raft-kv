.PHONY: setup lint test demo bench audit ci

setup:
	go mod download

# gofmt must be clean, then go vet.
lint:
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)
	go vet ./...

# 21 tests: regions, replication, leader crashes, partitions, splits, restarts, the routing
# client, and 25 fault-injected histories checked for linearizability with Porcupine.
test:
	go test -count=1 ./...

# Write 2,000 keys until regions split, crash the busiest leader, restart it, check 25 histories.
demo:
	go run ./cmd/multi-raft-kv

bench:
	@echo "M3: thousands of seeds in a deterministic simulator, and throughput as nodes are added"

# Known vulnerabilities in the modules and the Go standard library the code calls.
audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...

ci: setup lint test demo
