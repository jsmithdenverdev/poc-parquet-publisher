package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout io.Writer, getenv func(string) string) error {
	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	lambda.Start(handleConsumeRecords(logger))
	return nil
}

func handleConsumeRecords(logger *slog.Logger) func(context.Context, events.SQSEvent) error {
	return func(ctx context.Context, event events.SQSEvent) error {
		logger.InfoContext(ctx, "Received SQS event", "count", len(event.Records))
		return nil
	}
}
