package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseS3URL(t *testing.T) {
	t.Run("Valid S3 URL", func(t *testing.T) {
		bucket, key, err := ParseS3URL("s3://mybucket/mykey")
		require.NoError(t, err)
		assert.Equal(t, "mybucket", bucket)
		assert.Equal(t, "mykey", key)
	})

	t.Run("Missing s3 prefix", func(t *testing.T) {
		_, _, err := ParseS3URL("mybucket/mykey")
		require.Error(t, err)
		assert.Equal(t, "invalid S3 URL, missing 's3://' prefix", err.Error())
	})

	t.Run("No slash after bucket", func(t *testing.T) {
		_, _, err := ParseS3URL("s3://mybucket")
		require.Error(t, err)
		assert.Equal(t, "invalid S3 URL, no '/' found after bucket name", err.Error())
	})
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Run("Valid environment variables", func(t *testing.T) {
		os.Setenv("LOG_GROUP_NAME", "test-log-group")
		os.Setenv("LOG_STREAM_NAME", "test-log-stream")
		os.Setenv("FIELDS", "field1,field2")

		config, err := LoadConfigFromEnv()
		require.NoError(t, err)
		assert.Equal(t, "test-log-group", config.LogGroupName)
		assert.Equal(t, "test-log-stream", config.LogStreamName)
		assert.Equal(t, "field1,field2", config.Fields)

		// Cleanup
		os.Unsetenv("LOG_GROUP_NAME")
		os.Unsetenv("LOG_STREAM_NAME")
		os.Unsetenv("FIELDS")
	})

	t.Run("Missing LOG_GROUP_NAME", func(t *testing.T) {
		os.Unsetenv("LOG_GROUP_NAME")
		os.Setenv("LOG_STREAM_NAME", "test-log-stream")
		os.Setenv("FIELDS", "field1,field2")

		_, err := LoadConfigFromEnv()
		require.Error(t, err)
		assert.Equal(t, "environment variable LOG_GROUP_NAME is required", err.Error())

		// Cleanup
		os.Unsetenv("LOG_STREAM_NAME")
		os.Unsetenv("FIELDS")
	})

	t.Run("Missing LOG_STREAM_NAME", func(t *testing.T) {
		os.Setenv("LOG_GROUP_NAME", "test-log-group")
		os.Unsetenv("LOG_STREAM_NAME")
		os.Setenv("FIELDS", "field1,field2")

		_, err := LoadConfigFromEnv()
		require.Error(t, err)
		assert.Equal(t, "environment variable LOG_STREAM_NAME is required", err.Error())

		// Cleanup
		os.Unsetenv("LOG_GROUP_NAME")
		os.Unsetenv("FIELDS")
	})
}
