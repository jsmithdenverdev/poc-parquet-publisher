package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// handler processes records from a set of parquet files from S3
func handler(logger *slog.Logger, db *sql.DB, s3Client *s3.Client, cfg config) func(context.Context, request) (response, error) {
	return func(ctx context.Context, req request) (response, error) {
		tempDir, err := os.MkdirTemp("", "record-publisher-*")
		if err != nil {
			return response{}, fmt.Errorf("failed to create temp directory: %w", err)
		}

		defer os.RemoveAll(tempDir)

		for _, path := range req.Paths {
			localFilePath := filepath.Join(tempDir, filepath.Base(path))

			// Create the file
			file, err := os.Create(localFilePath)
			if err != nil {
				return response{}, fmt.Errorf("failed to create file: %w", err)
			}
			defer file.Close()

			// Download the file from S3
			getObjectOutput, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(req.Bucket),
				Key:    aws.String(path),
			})
			if err != nil {
				return response{}, fmt.Errorf("failed to download file: %w", err)
			}

			// Write the downloaded file to the local file
			if _, err = io.Copy(file, getObjectOutput.Body); err != nil {
				return response{}, fmt.Errorf("failed to write file: %w", err)
			}

			var totalRows int

			// Get total row count so we can batch rows across workers
			countResult := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM '%s';", localFilePath))
			if countResult.Err() != nil {
				return response{}, fmt.Errorf("failed to count rows: %w", err)
			}

			if err := countResult.Scan(&totalRows); err != nil {
				return response{}, fmt.Errorf("failed to scan row count: %w", err)
			}

			// Calculate the number of workers needed
			numWorkers := (totalRows + cfg.RowsPerWorker - 1) / cfg.RowsPerWorker // Ceiling division

			// Create a wait group to synchronize workers
			var wg sync.WaitGroup
			wg.Add(numWorkers)

			// Create a channel to capture errors from workers
			errChan := make(chan error, numWorkers)

			// Launch workers
			for i := 0; i < numWorkers; i++ {
				start := i * cfg.RowsPerWorker
				end := start + cfg.RowsPerWorker
				if end > totalRows {
					end = totalRows
				}
				go func(start, end int) {
					defer wg.Done()
					worker(ctx, errChan, db, logger)(localFilePath, start, end)
				}(start, end)
			}

			// Wait for workers or errors
			go func() {
				wg.Wait()
				close(errChan)
			}()

			// Return on first error
			if err := <-errChan; err != nil {
				return response{}, fmt.Errorf("worker error: %w", err)
			}

			logger.InfoContext(ctx, "processed file", "path", path, "count", totalRows)
		}

		return response{
			Paths: req.Paths,
		}, nil
	}
}
