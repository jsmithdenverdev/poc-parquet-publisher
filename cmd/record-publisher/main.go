package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
)

type publishRecordsRequest struct {
	Bucket string   `json:"bucket"`
	Paths  []string `json:"paths"`
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

func withEndpointOverride() func(*s3.Options) {
	return func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://s3.localhost.localstack.cloud:4566")
	}
}

func run(ctx context.Context, stdout io.Writer, getenv func(string) string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, withEndpointOverride())
	sqsClient := sqs.NewFromConfig(cfg)

	queueURL := getenv("QUEUE_URL")
	if queueURL == "" {
		return fmt.Errorf("QUEUE_URL environment variable is required")
	}

	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	handler := handlePublishRecords(logger, s3Client, sqsClient, queueURL)
	lambda.Start(handler)
	return nil
}

func handlePublishRecords(logger *slog.Logger, s3Client *s3.Client, sqsClient *sqs.Client, queueURL string) func(context.Context, publishRecordsRequest) (publishRecordsResponse, error) {
	return func(ctx context.Context, req publishRecordsRequest) (publishRecordsResponse, error) {
		for _, path := range req.Paths {
			// Open the Parquet file
			fr, err := s3v2.NewS3FileReaderWithClient(ctx, s3Client, req.Bucket, path)
			if err != nil {
				return publishRecordsResponse{}, fmt.Errorf("failed to create s3 parquet reader: %w", err)
			}
			defer fr.Close()

			pr, err := reader.NewParquetReader(fr, nil, 1)
			if err != nil {
				return publishRecordsResponse{}, fmt.Errorf("failed to create parquet reader: %w", err)
			}

			var (
				readRows  int64 = 0
				numRows   int64 = pr.GetNumRows()
				batchSize int   = 20
			)

			for readRows < numRows {
				_, err := pr.ReadByNumber(batchSize)
				if err != nil {
					if err == io.EOF {
						break
					}
					return publishRecordsResponse{}, fmt.Errorf("failed to read rows: %w", err)
				}
				readRows += int64(batchSize)

				fmt.Fprintf(os.Stdout, "\r Reading %d/%d rows", readRows, numRows)
			}

		}

		return publishRecordsResponse{
			Paths: req.Paths,
		}, nil
	}
}
