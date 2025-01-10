package main

// config is the configuration for the program.
type config struct {
	// Env is the environment we're executing in
	Env string `env:"ENV"`

	// QueueURL is the URL of the SQS queue to publish messages to
	QueueURL string `env:"QUEUE_URL"`

	// SQSBatchSize is the number of records to publish in a single SQS message
	SQSBatchSize int `env:"SQS_BATCH_SIZE"`

	// RowsPerBatch is the number of rows to read from parquet file in a single batch
	RowsPerBatch int `env:"ROWS_PER_BATCH"`

	// RowsPerWorker is the number of rows to process per worker
	RowsPerWorker int `env:"ROWS_PER_WORKER"`

	// S3EndpointOverride is the endpoint to use for S3
	S3EndpointOverride string `env:"S3_ENDPOINT_OVERRIDE"`
}
