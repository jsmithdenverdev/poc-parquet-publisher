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

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

const (
	// Maximum number of SQS messages to send in a batch (hard limit from AWS)
	sqsBatchSize = 10
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

// publishRequest is the request for the publishBatch function.
type publishRequest struct {
	batchIndex int
	records    []interface{}
	queueURL   string
	localPath  string
}

// publishBatch sends a batch of records to SQS
func publishBatch(ctx context.Context, sqsClient *sqs.Client, logger *slog.Logger, req publishRequest) func() error {
	return func() error {
		// Prepare batch entries
		var entries []types.SendMessageBatchRequestEntry
		for j, record := range req.records {
			// Convert individual record to JSON
			jsonData, err := json.Marshal(record)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to marshal record to JSON",
					"file", req.localPath,
					"batch_index", req.batchIndex,
					"record_index", j,
					"error", err,
				)
				return fmt.Errorf("failed to marshal record to JSON: %w", err)
			}

			// Create batch entry
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(req.batchIndex*sqsBatchSize + j)), // Unique ID within the batch
				MessageBody: aws.String(string(jsonData)),
			})
		}

		// Send batch to SQS
		result, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(req.queueURL),
			Entries:  entries,
		})
		if err != nil {
			logger.ErrorContext(ctx, "Failed to send message batch to SQS",
				"file", req.localPath,
				"batch_index", req.batchIndex,
				"batch_size", len(req.records),
				"error", err,
				"queue_url", req.queueURL,
			)
			return fmt.Errorf("failed to send message batch to SQS: %w", err)
		}

		// Log any failed messages
		if len(result.Failed) > 0 {
			logger.ErrorContext(ctx, "Some messages failed to send",
				"file", req.localPath,
				"batch_index", req.batchIndex,
				"failed_count", len(result.Failed),
				"failed_messages", result.Failed,
			)
			return fmt.Errorf("failed to send %d messages in batch", len(result.Failed))
		}

		return nil
	}
}

// handler processes records from a set of parquet files from S3
func handler(logger *slog.Logger, s3Client *s3.Client, sqsClient *sqs.Client, cfg config) func(context.Context, request) (response, error) {
	return func(ctx context.Context, req request) (response, error) {
		logger.InfoContext(ctx, "Received request", "bucket", req.Bucket, "paths", req.Paths)

		// Create temporary directory for all files
		tempDir, err := os.MkdirTemp("", "*")
		if err != nil {
			return response{}, fmt.Errorf("failed to create temp directory: %w", err)
		}

		defer os.RemoveAll(tempDir)

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

			logger.InfoContext(
				ctx,
				"Created parquet reader for file",
				"local_path", localPath)

			var (
				totalRows     = pr.GetNumRows()
				publishedRows = 0
			)

			for {
				rows, err := pr.ReadByNumber(cfg.RowsPerBatch)
				if err != nil {
					logger.ErrorContext(
						ctx,
						"Failed to read batch from parquet file",
						slog.String("file", localPath),
						slog.Any("error", err))

					return response{}, fmt.Errorf("failed to read batch from parquet file %s: %w", localPath, err)
				}

				// If no more rows, we're done
				if len(rows) == 0 {
					break
				}

				// Create error group for concurrent processing
				g, gctx := errgroup.WithContext(ctx)

				// Prepare all batches first
				var batches [][]interface{}
				for i := 0; i < len(rows); i += sqsBatchSize {
					end := i + sqsBatchSize
					if end > len(rows) {
						end = len(rows)
					}
					batches = append(batches, rows[i:end])
				}

				// Process batches concurrently
				for i, batch := range batches {
					req := publishRequest{
						batchIndex: i,
						records:    batch,
						queueURL:   cfg.QueueURL,
						localPath:  localPath,
					}
					g.Go(publishBatch(gctx, sqsClient, logger, req))
				}

				// Wait for all goroutines to complete
				if err := g.Wait(); err != nil {
					logger.ErrorContext(
						ctx,
						"Error processing batch",
						slog.Any("error", err),
						slog.Int("rows_in_batch", len(rows)),
						slog.Int("total_published_rows", publishedRows),
						slog.Int64("total_rows", totalRows))

					return response{}, err
				}

				publishedRows += len(rows)

				logger.InfoContext(
					ctx,
					"Published batch from parquet file",
					slog.Int("rows_in_batch", len(rows)),
					slog.Int("total_published_rows", publishedRows),
					slog.Int64("total_rows", totalRows),
				)
			}

			logger.InfoContext(
				ctx,
				"Processed file",
				"s3_path", path,
				"local_path", localPath,
				"num_rows", totalRows)
		}

		return response{}, nil
	}
}
