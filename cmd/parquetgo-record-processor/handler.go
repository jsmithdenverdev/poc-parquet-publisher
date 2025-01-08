package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// request is the request for the handler function.
type request struct {
	Bucket string   `json:"bucket"`
	Paths  []string `json:"paths"`
}

// response is the response for the handler function.
type response struct {
	Paths []string `json:"paths"`
}

const sqsBatchSize = 10

func handler(logger *slog.Logger, s3Client *s3.Client, sqsClient *sqs.Client, cfg config) func(context.Context, request) (response, error) {
	return func(ctx context.Context, req request) (response, error) {
		logger.InfoContext(ctx, "Received request", "bucket", req.Bucket, "paths", req.Paths)
		logger.ErrorContext(ctx, "TEST TEST TEST")

		// Create temporary directory for all files
		tempDir, err := os.MkdirTemp("", "*")
		if err != nil {
			return response{}, fmt.Errorf("failed to create temp directory: %w", err)
		}

		cleanup := func() {
			if err := os.RemoveAll(tempDir); err != nil {
				logger.ErrorContext(ctx, "Failed to cleanup temporary directory", "path", tempDir, "error", err)
			} else {
				logger.InfoContext(ctx, "Cleaned up temporary directory", "path", tempDir)
			}
		}

		defer cleanup()

		logger.InfoContext(ctx, "Created temporary directory", "path", tempDir)

		for _, path := range req.Paths {
			// Create local file path
			localPath := filepath.Join(tempDir, filepath.Base(path))

			// Create local file
			file, err := os.Create(localPath)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to create local file", "path", localPath, "error", err)
				return response{}, fmt.Errorf("failed to create local file %s: %w", localPath, err)
			}
			defer file.Close()

			// Download file from S3
			result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(req.Bucket),
				Key:    aws.String(path),
			})
			if err != nil {
				logger.ErrorContext(ctx, "Failed to get object from S3", "bucket", req.Bucket, "path", path, "error", err)
				return response{}, fmt.Errorf("failed to get object from S3 %s/%s: %w", req.Bucket, path, err)
			}
			defer result.Body.Close()

			// Copy S3 object to local file
			_, err = io.Copy(file, result.Body)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to copy S3 object to local file", "local_path", localPath, "error", err)
				return response{}, fmt.Errorf("failed to copy S3 object to local file %s: %w", localPath, err)
			}

			logger.InfoContext(ctx, "Copied S3 object to local file", "s3_path", path, "local_path", localPath)

			fr, err := local.NewLocalFileReader(localPath)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to create local file reader for file", "local_path", localPath, "error", err)
				return response{}, fmt.Errorf("failed to create local file reader for file %s: %w", localPath, err)
			}

			logger.InfoContext(ctx, "Created local file reader for file", "local_path", localPath)

			// TODO: move into a cleanup closure
			defer fr.Close()

			pr, err := reader.NewParquetReader(fr, nil, 4)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to create parquet reader for file", "local_path", localPath, "error", err)
				return response{}, fmt.Errorf("failed to create parquet reader for file %s: %w", localPath, err)
			}

			logger.InfoContext(ctx, "Created parquet reader for file", "local_path", localPath)

			totalRows := pr.GetNumRows()
			logger.InfoContext(ctx, "Total rows in file", "local_path", localPath, "num_rows", totalRows)

			readRows := 0

			batchSize := 1000 // Adjust based on your needs
			for {
				rows, err := pr.ReadByNumber(batchSize)
				if err != nil {
					logger.ErrorContext(ctx, "Failed to read batch from parquet file", "file", localPath, "error", err)
					return response{}, fmt.Errorf("failed to read batch from parquet file %s: %w", localPath, err)
				}

				readRows += len(rows)
				logger.InfoContext(ctx, "Read batch from parquet file", "file", localPath, "num_rows", len(rows), "read_rows", readRows, "total_rows", totalRows)

				// If no more rows, we're done
				if len(rows) == 0 {
					break
				}

				// Process sub-batches
				for i := 0; i < len(rows); i += sqsBatchSize {
					end := i + sqsBatchSize
					if end > len(rows) {
						end = len(rows)
					}
					subBatch := rows[i:end]

					// Prepare batch entries
					var entries []types.SendMessageBatchRequestEntry
					for j, record := range subBatch {
						// Convert individual record to JSON
						jsonData, err := json.Marshal(record)
						if err != nil {
							logger.ErrorContext(ctx, "Failed to marshal record to JSON",
								"file", localPath,
								"batch_index", i,
								"record_index", j,
								"error", err,
							)
							return response{}, fmt.Errorf("failed to marshal record to JSON: %w", err)
						}

						// Create batch entry
						entries = append(entries, types.SendMessageBatchRequestEntry{
							Id:          aws.String(strconv.Itoa(i + j)), // Unique ID within the batch
							MessageBody: aws.String(string(jsonData)),
						})
					}

					// Send batch to SQS
					result, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
						QueueUrl: aws.String(cfg.QueueURL),
						Entries:  entries,
					})
					if err != nil {
						logger.ErrorContext(ctx, "Failed to send message batch to SQS",
							"file", localPath,
							"batch_start", i,
							"batch_size", len(subBatch),
							"error", err,
							"queue_url", cfg.QueueURL,
						)
						return response{}, fmt.Errorf("failed to send message batch to SQS: %w", err)
					}

					// Log any failed messages
					if len(result.Failed) > 0 {
						logger.ErrorContext(ctx, "Some messages failed to send",
							"file", localPath,
							"batch_start", i,
							"failed_count", len(result.Failed),
							"failed_messages", result.Failed,
						)
						return response{}, fmt.Errorf("failed to send %d messages in batch", len(result.Failed))
					}

					logger.InfoContext(ctx, "Sent message batch to SQS",
						"file", localPath,
						"batch_start", i,
						"batch_end", end,
						"batch_size", len(subBatch),
						"successful_count", len(result.Successful),
					)
				}
			}

			logger.InfoContext(ctx, "Processed file", "s3_path", path, "local_path", localPath, "num_rows", pr.GetNumRows())
		}

		return response{}, nil
	}
}
