# poc-parquet-publisher

A prototype serverless application. Reads a large parquet file from S3 into Lambda, creates a stream reader on the contents. Publishes the contents to SQS.

# Requirements

- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-installation.html)
- aarch64-linux-gnu-gcc compiler
- x86_64-unknown-linux-gnu
  - [Mac](https://github.com/messense/homebrew-macos-cross-toolchains)
