package main

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	LogGroupName  string
	LogStreamName string
	Fields        string
}

func ParseS3URL(url string) (bucket string, prefix string, err error) {
	if !strings.HasPrefix(url, "s3://") {
		return "", "", fmt.Errorf("invalid S3 URL, missing 's3://' prefix")
	}
	trimmedS3URL := strings.TrimPrefix(url, "s3://")
	splitPos := strings.Index(trimmedS3URL, "/")
	if splitPos == -1 {
		return "", "", fmt.Errorf("invalid S3 URL, no '/' found after bucket name")
	}
	bucket = trimmedS3URL[:splitPos]
	prefix = trimmedS3URL[splitPos+1:]
	return bucket, prefix, nil
}

func LoadConfigFromEnv() (Config, error) {
	logGroupName := os.Getenv("LOG_GROUP_NAME")
	if logGroupName == "" {
		return Config{}, fmt.Errorf("environment variable LOG_GROUP_NAME is required")
	}

	logStreamName := os.Getenv("LOG_STREAM_NAME")
	if logStreamName == "" {
		return Config{}, fmt.Errorf("environment variable LOG_STREAM_NAME is required")
	}

	fields := os.Getenv("FIELDS")

	return Config{
		LogGroupName:  logGroupName,
		LogStreamName: logStreamName,
		Fields:        fields,
	}, nil
}
