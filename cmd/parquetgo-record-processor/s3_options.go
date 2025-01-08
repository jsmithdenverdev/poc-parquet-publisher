package main

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// withEndpointOverride returns a function option that sets the endpoint of an
// S3 client based on the configuration.
func withEndpointOverride(cfg config) func(*s3.Options) {
	return func(o *s3.Options) {
		if cfg.S3EndpointOverride != "" {
			o.BaseEndpoint = aws.String(cfg.S3EndpointOverride)
		}
	}
}
