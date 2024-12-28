package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/marcboeker/go-duckdb"
)

const batchSize = 1000

type publishRecordsRequest struct {
	Bucket string   `json:"bucket"`
	Paths  []string `json:"paths"`
}

type publishRecordsResponse struct {
	Paths []string `json:"paths"`
}

func main() {
	if err := run(context.Background(), os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout io.Writer, getenv func(string) string) error {
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		bootQueries := []string{
			`SET s3_endpoint='s3.localhost.localstack.cloud:4566';`,
			`SET s3_access_key_id='test';`,
			`SET s3_secret_access_key='test';`,
			`SET s3_region='us-east-1';`,
		}

		for _, query := range bootQueries {
			if _, err := execer.ExecContext(ctx, query, nil); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to open duckdb: %w", err)
	}

	db := sql.OpenDB(connector)

	// TODO: fix this as a deferred statement will not run in a lambda function
	defer connector.Close()
	defer db.Close()

	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	handler := handlePublishRecords(logger, db)
	lambda.Start(handler)
	return nil
}

func handlePublishRecords(logger *slog.Logger, db *sql.DB) func(context.Context, publishRecordsRequest) (publishRecordsResponse, error) {
	return func(ctx context.Context, req publishRecordsRequest) (publishRecordsResponse, error) {
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(10*time.Minute))
		defer cancel()

		for _, path := range req.Paths {
			s3Path := fmt.Sprintf("s3://%s/%s", req.Bucket, path)

			offset := 0
			totalRows := 0

			for {
				query := fmt.Sprintf(`
					SELECT *
					FROM '%s'
					LIMIT %d OFFSET %d
				`, s3Path, batchSize, offset)

				rows, err := db.QueryContext(ctx, query)
				if err != nil {
					return publishRecordsResponse{}, fmt.Errorf("failed to execute query: %w", err)
				}

				rowCount := 0
				// Iterate over the rows in the current chunk
				for rows.Next() {
					rowCount++
				}

				if err := rows.Err(); err != nil {
					rows.Close() // Close immediately if there's a row iteration error
					return publishRecordsResponse{}, fmt.Errorf("row iteration error: %w", err)
				}

				if rowCount == 0 {
					break
				}

				totalRows += rowCount
				offset += batchSize

				logger.InfoContext(
					ctx,
					"processed batch",
					slog.Int("count", rowCount),
					slog.Int("offset", offset))
			}

			logger.InfoContext(ctx, "processed file", "path", path, "count", totalRows)
		}

		return publishRecordsResponse{
			Paths: req.Paths,
		}, nil
	}
}
