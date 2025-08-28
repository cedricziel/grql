.PHONY: proto proto-server proto-plugin clean build run test test-integration
.PHONY: plugin-install plugin-build plugin-dev plugin-backend plugin-frontend plugin-up plugin-down

# Proto generation
proto:
	protoc --go_out=pkg --go_opt=paths=source_relative \
		--go-grpc_out=pkg --go-grpc_opt=paths=source_relative \
		--proto_path=proto proto/grql/v1/query.proto

clean:
	rm -f pkg/grql/v1/*.pb.go
	rm -f server/internal/proto/*.pb.go
	rm -f grafana-plugin/internal/proto/*.pb.go
	rm -rf bin/

# Server targets
build: proto
	go build -o bin/grql-server ./server/cmd/server

run: build
	./bin/grql-server

test:
	go test ./server/... ./grafana-plugin/pkg/...

test-integration:
	go test -v -tags=integration -timeout 10m ./server/...

# Grafana Plugin Targets
plugin-install:
	cd grafana-plugin && npm install

plugin-backend:
	cd grafana-plugin && mage -v build:linuxARM64

plugin-frontend:
	cd grafana-plugin && npm run build

plugin-build: plugin-frontend plugin-backend

plugin-dev:
	cd grafana-plugin && npm run dev

plugin-up:
	cd grafana-plugin && docker compose up

plugin-down:
	cd grafana-plugin && docker compose down

plugin-logs:
	cd grafana-plugin && docker compose logs -f

# Workspace management
tidy:
	go mod tidy

update-deps:
	go get -u ./...

# Development helpers
dev-server: proto
	go run ./server/cmd/server

dev-all:
	make -j2 dev-server plugin-dev