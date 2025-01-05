package main

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

// newS3DuckDBConnector creates a new DuckDB connector with configuration
// for S3.
//
// # NOTE
//
// This function is only needed if you are querying S3 directly. If you
// are downloading files from S3 and processing them locally, you can use the
// default connector.
func newS3DuckDBConnector(ctx context.Context, cfg config) (*duckdb.Connector, error) {
	return duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		bootQueries := make([]string, 0)

		// For local development we need to set S3 configuration variables
		if cfg.Env == "local" {
			bootQueries = append(bootQueries, []string{
				`SET s3_endpoint='s3.localhost.localstack.cloud:4566'`,
				`SET s3_access_key_id='test';`,
				`SET s3_secret_access_key='test';`,
				`SET s3_region='us-east-1';`,
			}...)
		}

		for _, query := range bootQueries {
			if _, err := execer.ExecContext(ctx, query, nil); err != nil {
				return err
			}
		}
		return nil
	})
}
