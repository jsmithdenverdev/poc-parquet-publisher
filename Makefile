.PHONY: build-DuckDBRecordProcessorFunction
.PHONY: build-SQSRecordConsumerFunction


build-DuckDBRecordProcessorFunction:
	CGO_ENABLED=1 \
	CC=aarch64-linux-gnu-gcc \
	GOOS=linux \
	GOARCH=arm64 \
	go build -o ./cmd/duckdb-record-processor/bootstrap -tags lambda.norpc ./cmd/duckdb-record-processor/*.go
	cp ./cmd/duckdb-record-processor/bootstrap $(ARTIFACTS_DIR)

build-SQSRecordConsumerFunction:
	CGO_ENABLED=1 \
	CC=aarch64-linux-gnu-gcc \
	GOOS=linux \
	GOARCH=arm64 \
	go build -o ./cmd/sqs-record-consumer/bootstrap -tags lambda.norpc ./cmd/sqs-record-consumer/*.go
	cp ./cmd/sqs-record-consumer/bootstrap $(ARTIFACTS_DIR)
