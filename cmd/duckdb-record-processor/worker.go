package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// worker returns a closure that processes a range of rows from the parquet file
func worker(ctx context.Context, errChan chan<- error, db *sql.DB, logger *slog.Logger) func(localFilePath string, start, end int) {
	return func(localFilePath string, start, end int) {
		// Query and process rows from start to end
		query := fmt.Sprintf("SELECT * FROM '%s' LIMIT %d OFFSET %d", localFilePath, end-start, start)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			logger.ErrorContext(ctx, "failed to query rows", slog.Any("error", err))
			errChan <- err
			return
		}
		defer rows.Close()

		// Process the rows
		for rows.Next() {
			// Process data...
		}

		logger.InfoContext(ctx, "processed rows", "start", start, "end", end)
	}
}
