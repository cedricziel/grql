.PHONY: proto clean build run

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