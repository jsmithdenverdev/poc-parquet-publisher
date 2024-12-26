package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jsmithdenverdev/poc-parquet-publisher/internal/models"
	"github.com/parquet-go/parquet-go"
)

type publishRecordsRequest struct {
	Paths []string `json:"paths"`
}

type publishRecordsResponse struct {
	Paths []string `json:"paths"`
}

const (
	batchSize = 100
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout io.Writer, getenv func(string) string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)

	queueURL := getenv("QUEUE_URL")
	if queueURL == "" {
		return fmt.Errorf("QUEUE_URL environment variable is required")
	}

	handler := handlePublishRecords(s3Client, sqsClient, queueURL)
	lambda.Start(handler)
	return nil
}

func handlePublishRecords(s3Client *s3.Client, sqsClient *sqs.Client, queueURL string) func(context.Context, publishRecordsRequest) (publishRecordsResponse, error) {
	return func(ctx context.Context, req publishRecordsRequest) (publishRecordsResponse, error) {
		for _, path := range req.Paths {
			if err := processParquetFile(ctx, path); err != nil {
				return publishRecordsResponse{}, fmt.Errorf("processing file %s: %w", path, err)
			}
		}

		return publishRecordsResponse{
			Paths: req.Paths,
		}, nil
	}
}

func processParquetFile(ctx context.Context, path string) error {
	// Open the Parquet file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	// Create a new Parquet reader
	reader := parquet.NewReader(f)
	defer reader.Close()

	// Create a buffer for batch reading
	batch := make([]models.Record, 0, batchSize)
	totalRows := 0

	for {
		record := new(models.Record)
		err := reader.Read(record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading record: %w", err)
		}

		batch = append(batch, *record)
		if len(batch) == batchSize {
			// Process the batch
			totalRows += len(batch)
			fmt.Printf("\rTotal records processed: %d", totalRows)
			batch = batch[:0] // Reset the batch
		}
	}

	// Process any remaining records in the batch
	if len(batch) > 0 {
		totalRows += len(batch)
		fmt.Printf("\rTotal records processed: %d", totalRows)
		batch = batch[:0] // Reset the batch
	}

	return nil
}
