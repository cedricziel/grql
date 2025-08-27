.PHONY: proto proto-server proto-plugin clean build run test test-integration
.PHONY: plugin-install plugin-build plugin-dev plugin-backend plugin-frontend plugin-up plugin-down

# Proto generation
proto: proto-server proto-plugin

proto-server:
	protoc --go_out=server/internal/proto --go_opt=paths=source_relative \
		--go-grpc_out=server/internal/proto --go-grpc_opt=paths=source_relative \
		--proto_path=proto proto/grql/v1/query.proto

proto-plugin:
	protoc --go_out=grafana-plugin/internal/proto --go_opt=paths=source_relative \
		--go-grpc_out=grafana-plugin/internal/proto --go-grpc_opt=paths=source_relative \
		--proto_path=proto proto/grql/v1/query.proto

clean:
	rm -f server/internal/proto/*.pb.go
	rm -f grafana-plugin/internal/proto/*.pb.go
	rm -rf bin/

# Server targets
build: proto-server
	cd server && go build -o ../bin/grql-server cmd/server/main.go

run: build
	./bin/grql-server

test:
	cd server && go test ./...
	cd grafana-plugin && go test ./pkg/...

test-integration:
	cd server && go test -v -tags=integration -timeout 10m ./...

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
	cd server && go mod tidy
	cd grafana-plugin && go mod tidy

update-deps:
	cd server && go get -u ./...
	cd grafana-plugin && go get -u ./...

# Development helpers
dev-server: proto-server
	cd server && go run cmd/server/main.go

dev-all:
	make -j2 dev-server plugin-dev