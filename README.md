# elb-logs-to-cloudwatch

AWS Elastic Load Balancing access logs are [stored in an S3 bucket](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/enable-access-logging.html) by default, which isn't ideal for analysis. This small Go program parses log files stored in S3 and sends them to CloudWatch. To ensure good performance and low memory usage, [io.Pipe](https://pkg.go.dev/io#Pipe) and goroutines are utilized for concurrent processing.

## Configuration

- `LOG_GROUP_NAME` (required): CloudWatch Log Group Name to send logs to.
- `LOG_STREAM_NAME` (required): CloudWatch Log Stream Name to send logs to.
- `FIELDS` (optional): List of comma separated fields to extract from the log line. If not provided, all fields will be sent by default. For a list of all available fields see [ELB docs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format)

## CLI Usage

For example you want to process all log files stored for January 1st, 2024, and send them to CloudWatch. You are only interested in the request URL and the response processing time. You can do this by running:

```
LOG_GROUP_NAME=my-log-group-name \
LOG_STREAM_NAME=my-log-stream-name \
FIELDS=request,response_processing_time \
./elb-logs-to-cloudwatch s3://<bucket>/AWSLogs/<account-id>/elasticloadbalancing/<region>/2024/01/01/
```

## Usage with Lamdba function
This program can be used in a Lamdba function that receives an `s3:ObjectCreated` event. This way logfiles are processed and sent to CloudWatch as soon as they are stored in S3. TODO describe steps for setup.

## Why not just use CloudWatch ELB metrics?

CloudWatch provides basic metrics for ELB, but the access logs contain more details (e.g. request URL, user agent, etc.). For instance you might want to know which URLs have the highest latency. This information is not available in the CloudWatch metrics.





