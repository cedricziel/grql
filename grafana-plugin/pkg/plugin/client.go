package plugin

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	
	pb "github.com/cedricziel/grql/grafana-plugin/internal/proto/grql/v1"
	"github.com/cedricziel/grql/grafana-plugin/pkg/models"
)

// GrqlClient manages the gRPC connection to the grql server
type GrqlClient struct {
	conn   *grpc.ClientConn
	client pb.QueryServiceClient
}

// NewGrqlClient creates a new grql client with the given settings
func NewGrqlClient(settings *models.PluginSettings) (*GrqlClient, error) {
	host := settings.Host
	if host == "" {
		host = "localhost"
	}
	port := settings.Port
	if port == 0 {
		port = 50051
	}
	address := fmt.Sprintf("%s:%d", host, port)
	
	var opts []grpc.DialOption
	
	if settings.UseTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: settings.InsecureSkipVerify,
		}
		
		// Add client certificates if provided
		if settings.Secrets != nil && settings.Secrets.TLSCert != "" && settings.Secrets.TLSKey != "" {
			cert, err := tls.X509KeyPair([]byte(settings.Secrets.TLSCert), []byte(settings.Secrets.TLSKey))
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificates: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	
	// Set reasonable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to grql server: %w", err)
	}
	
	client := pb.NewQueryServiceClient(conn)
	
	return &GrqlClient{
		conn:   conn,
		client: client,
	}, nil
}

// ExecuteQuery executes a query against the grql server
func (c *GrqlClient) ExecuteQuery(ctx context.Context, query string, params map[string]string) (*pb.QueryResponse, error) {
	req := &pb.QueryRequest{
		Query:      query,
		Parameters: params,
	}
	
	return c.client.ExecuteQuery(ctx, req)
}

// StreamQuery executes a streaming query against the grql server
func (c *GrqlClient) StreamQuery(ctx context.Context, query string, params map[string]string) (pb.QueryService_StreamQueryClient, error) {
	req := &pb.QueryRequest{
		Query:      query,
		Parameters: params,
	}
	
	return c.client.StreamQuery(ctx, req)
}

// Close closes the gRPC connection
func (c *GrqlClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}