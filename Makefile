API_URL ?= http://localhost:8080

.PHONY: bench chaos test run lint

bench:
	API_URL=$(API_URL) k6 run scripts/bench.js

chaos:
	API_URL=$(API_URL) scripts/chaos.sh

test:
	go test ./... -race -count=1 -cover

run:
	docker compose -f deploy/docker-compose.yml up --build

lint:
	golangci-lint run
