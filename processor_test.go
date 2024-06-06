package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockS3Api struct {
	mock.Mock
}

func (m *MockS3Api) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*s3.GetObjectOutput), args.Error(1)
}

func (m *MockS3Api) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	args := m.Called(input)
	return args.Get(0).(*s3.ListObjectsV2Output), args.Error(1)
}

func TestProcessLogs(t *testing.T) {
	t.Run("Successful Processing", func(t *testing.T) {
		mockS3 := new(MockS3Api)
		mockCW := new(MockCloudWatchLogsClient)

		// Mock S3 GetObject response
		mockBody := `https 2024-03-21T16:10:26.071854Z app/example-prod-lb/xxxxxxx4 192.0.2.104:36217 10.0.0.24:3003 0.004 0.024 0.003 203 203 1694 10783 "PUT https://example.com:443/api/modify?user_ids=xxxxx4-xxxx-xxxx-xxxx-xxxxxxxxxxxx&ref_date= HTTP/1.1" "axios/1.6.5" ECDHE-RSA-AES256-GCM-SHA384 TLSv1.3 arn:aws:elasticloadbalancing:xx-west-1:987654321098:targetgroup/example-prod-tg/xxxxxxxx4 "Root=1-xxxxxx4-xxxxxxxxxxxxxxxxxxxxxxxx" "example.com" "arn:aws:acm:xx-west-1:987654321098:certificate/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" 203 2024-03-21T16:10:26.061854Z "cache" "-" "-" "10.0.0.24:3003" "203" "-" "-" "TID_a1b2c3d4e5f67890abcdef1234567890"`

		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, err := gz.Write([]byte(mockBody))
		require.NoError(t, err)
		require.NoError(t, gz.Close())

		mockS3.On("GetObject", mock.Anything).Return(&s3.GetObjectOutput{
			Body: io.NopCloser(&buf),
		}, nil)

		// Mock CloudWatch PutLogEvents response
		mockCW.On("PutLogEvents", mock.Anything).Return(&cloudwatchlogs.PutLogEventsOutput{}, nil)

		fieldStore, err := NewFields("")
		require.NoError(t, err)

		lp := &CloudWatchLogProcessor{
			s3Client:   mockS3,
			cwClient:   mockCW,
			fieldStore: fieldStore,
			logConfig:  LogConfig{LogGroupName: "test-log-group", LogStreamName: "test-log-stream"},
		}

		err = lp.ProcessLogs(S3ObjectInfo{Bucket: "test-bucket", Key: "test-key"})
		require.NoError(t, err)

		mockS3.AssertExpectations(t)
		mockCW.AssertExpectations(t)
	})
}

func TestProcessRecords(t *testing.T) {
	t.Run("Process CSV Records", func(t *testing.T) {
		fieldStore, err := NewFields("")
		require.NoError(t, err)

		mockData := `https 2024-03-21T16:10:26.071854Z app/example-prod-lb/xxxxxxx4 192.0.2.104:36217 10.0.0.24:3003 0.004 0.024 0.003 203 203 1694 10783 "PUT https://example.com:443/api/modify?user_ids=xxxxx4-xxxx-xxxx-xxxx-xxxxxxxxxxxx&ref_date= HTTP/1.1" "axios/1.6.5" ECDHE-RSA-AES256-GCM-SHA384 TLSv1.3 arn:aws:elasticloadbalancing:xx-west-1:987654321098:targetgroup/example-prod-tg/xxxxxxxx4 "Root=1-xxxxxx4-xxxxxxxxxxxxxxxxxxxxxxxx" "example.com" "arn:aws:acm:xx-west-1:987654321098:certificate/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" 203 2024-03-21T16:10:26.061854Z "cache" "-" "-" "10.0.0.24:3003" "203" "-" "-" "TID_a1b2c3d4e5f67890abcdef1234567890"`

		mockReader := strings.NewReader(mockData)
		entryChan := make(chan LogEntry, 10)

		go func() {
			err := processRecords(mockReader, entryChan, fieldStore)
			require.NoError(t, err)
			close(entryChan)
		}()

		count := 0
		for entry := range entryChan {
			assert.Equal(t, "PUT https://example.com:443/api/modify?user_ids=xxxxx4-xxxx-xxxx-xxxx-xxxxxxxxxxxx&ref_date= HTTP/1.1", entry.Data["request"])
			count++
		}

		assert.Equal(t, 1, count)
	})
}

func TestRecordToLogEntry(t *testing.T) {
	t.Run("Valid Log Entry", func(t *testing.T) {
		fieldStore, err := NewFields("")
		require.NoError(t, err)

		record := []string{
			"https",
			"2024-03-21T16:10:26.071854Z",
			"app/example-prod-lb/xxxxxxx4",
			"192.0.2.104:36217",
			"10.0.0.24:3003",
			"0.004",
			"0.024",
			"0.003",
			"203",
			"203",
			"1694",
			"10783",
			"PUT https://example.com:443/api/modify?user_ids=xxxxx4-xxxx-xxxx-xxxx-xxxxxxxxxxxx&ref_date= HTTP/1.1",
			"axios/1.6.5",
			"ECDHE-RSA-AES256-GCM-SHA384",
			"TLSv1.3",
			"arn:aws:elasticloadbalancing:xx-west-1:987654321098:targetgroup/example-prod-tg/xxxxxxxx4",
			"Root=1-xxxxxx4-xxxxxxxxxxxxxxxxxxxxxxxx",
			"example.com",
			"arn:aws:acm:xx-west-1:987654321098:certificate/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			"203",
			"2024-03-21T16:10:26.061854Z",
			"cache",
			"-",
			"-",
			"10.0.0.24:3003",
			"203",
			"-",
			"-",
			"TID_a1b2c3d4e5f67890abcdef1234567890",
		}

		logEntry, err := recordToLogEntry(record, fieldStore)
		require.NoError(t, err)
		assert.Equal(t, "2024-03-21T16:10:26.071854Z", logEntry.Timestamp.Format(time.RFC3339Nano))
		assert.Equal(t, "PUT https://example.com:443/api/modify?user_ids=xxxxx4-xxxx-xxxx-xxxx-xxxxxxxxxxxx&ref_date= HTTP/1.1", logEntry.Data["request"])
	})
}
