.PHONY: proto clean build run plugin-install plugin-build plugin-dev plugin-backend plugin-frontend

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/query.proto

clean:
	rm -f proto/*.pb.go

build: proto
	go build -o bin/grql-server cmd/server/main.go

run: build
	./bin/grql-server

test:
	go test ./...

test-integration:
	go test -v -tags=integration -timeout 10m ./...

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