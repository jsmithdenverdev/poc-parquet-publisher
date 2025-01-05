package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/caarlos0/env/v11"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v", err)
		os.Exit(1)
	}
}

// run executes the main logic of the program.
func run(ctx context.Context, stdout io.Writer) error {
	// Create structured logger using JSON format
	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	// Load env config
	var cfg config
	if err := env.Parse(&cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Open a connection to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Load aws config
	awscfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create a new S3 client using default config
	s3Client := s3.NewFromConfig(awscfg, withEndpointOverride(cfg))

	// Start lambda function
	lambda.StartWithOptions(
		handler(logger, db, s3Client, cfg),
		lambda.WithEnableSIGTERM(func() {
			db.Close()
		}))

	return nil
}
