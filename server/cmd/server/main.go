package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/cedricziel/grql/server/internal/proto/grql/v1"
	"github.com/cedricziel/grql/server/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.Int("port", 50051, "The server port")
	host = flag.String("host", "localhost", "The server host")
)

func main() {
	flag.Parse()

	address := fmt.Sprintf("%s:%d", *host, *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	grpcServer := grpc.NewServer()
	queryServer := server.New()
	pb.RegisterQueryServiceServer(grpcServer, queryServer)

	// Register reflection service for debugging with tools like grpcurl
	reflection.Register(grpcServer)

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	log.Printf("gRPC server listening on %s", address)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
