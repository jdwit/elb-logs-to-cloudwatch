package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockLogProcessor struct {
	mock.Mock
}

func (m *MockLogProcessor) ProcessLogs(s3obj S3ObjectInfo) error {
	args := m.Called(s3obj)
	return args.Error(0)
}

func TestHandleLambdaEvent(t *testing.T) {
	t.Run("Successful Processing", func(t *testing.T) {
		// Raw JSON event data
		eventData := `{
			"Records": [
				{
					"s3": {
						"bucket": {
							"name": "my-bucket"
						},
						"object": {
							"key": "my-folder/my-object.txt"
						}
					}
				}
			]
		}`
		var event S3ObjectCreatedEvent
		err := json.Unmarshal([]byte(eventData), &event)
		require.NoError(t, err)

		// Mock the LogProcessor
		mockProcessor := new(MockLogProcessor)
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object.txt",
		}).Return(nil)

		// Create handler with mock processor
		handler := &Handler{lp: mockProcessor}

		// Call the function under test
		err = handler.HandleLambdaEvent(event)
		require.NoError(t, err)

		// Assert that the ProcessLogs method was called with the correct parameters
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object.txt",
		})
	})

	t.Run("Processing Error", func(t *testing.T) {
		// Raw JSON event data
		eventData := `{
			"Records": [
				{
					"s3": {
						"bucket": {
							"name": "my-bucket"
						},
						"object": {
							"key": "my-folder/my-object.txt"
						}
					}
				}
			]
		}`
		var event S3ObjectCreatedEvent
		err := json.Unmarshal([]byte(eventData), &event)
		require.NoError(t, err)

		// Mock the LogProcessor
		mockProcessor := new(MockLogProcessor)
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object.txt",
		}).Return(fmt.Errorf("process logs error"))

		// Create handler with mock processor
		handler := &Handler{lp: mockProcessor}

		// Call the function under test
		err = handler.HandleLambdaEvent(event)

		// Assert that an error was returned
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error processing logs for s3://my-bucket/my-folder/my-object.txt: process logs error")

		// Assert that the ProcessLogs method was called with the correct parameters
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object.txt",
		})
	})

	t.Run("Multiple Records", func(t *testing.T) {
		// Raw JSON event data
		eventData := `{
			"Records": [
				{
					"s3": {
						"bucket": {
							"name": "my-bucket"
						},
						"object": {
							"key": "my-folder/my-object1.txt"
						}
					}
				},
				{
					"s3": {
						"bucket": {
							"name": "my-bucket"
						},
						"object": {
							"key": "my-folder/my-object2.txt"
						}
					}
				}
			]
		}`
		var event S3ObjectCreatedEvent
		err := json.Unmarshal([]byte(eventData), &event)
		require.NoError(t, err)

		mockProcessor := new(MockLogProcessor)

		handler := &Handler{lp: mockProcessor}

		// Use a WaitGroup to ensure all goroutines complete
		var wg sync.WaitGroup
		wg.Add(2) // Expecting two calls

		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object1.txt",
		}).Return(nil).Run(func(args mock.Arguments) {
			wg.Done()
		})
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object2.txt",
		}).Return(nil).Run(func(args mock.Arguments) {
			wg.Done()
		})

		go func() {
			err := handler.HandleLambdaEvent(event)
			require.NoError(t, err)
		}()

		// Wait for all goroutines to finish
		wg.Wait()

		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object1.txt",
		})
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "my-bucket",
			Key:    "my-folder/my-object2.txt",
		})
	})
}

func TestHandleS3URL(t *testing.T) {
	t.Run("Successful Processing", func(t *testing.T) {
		// Mock the LogProcessor
		mockProcessor := new(MockLogProcessor)
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-key",
		}).Return(nil)

		// Mock the S3Api
		mockS3Api := new(MockS3Api)
		mockS3Api.On("ListObjectsV2", mock.Anything).Return(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("mock-key")},
			},
		}, nil)

		// Create handler with mock processor and mock S3 API
		handler := &Handler{lp: mockProcessor, s3Client: mockS3Api}

		// Call the function under test
		err := handler.HandleS3URL("s3://mock-bucket/mock-prefix")
		require.NoError(t, err)

		// Assert that the ProcessLogs method was called with the correct parameters
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-key",
		})
	})

	t.Run("Error in ProcessLogs", func(t *testing.T) {
		// Mock the LogProcessor
		mockProcessor := new(MockLogProcessor)
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-key",
		}).Return(fmt.Errorf("process logs error"))

		// Mock the S3Api
		mockS3Api := new(MockS3Api)
		mockS3Api.On("ListObjectsV2", mock.Anything).Return(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("mock-key")},
			},
		}, nil)

		// Create handler with mock processor and mock S3 API
		handler := &Handler{lp: mockProcessor, s3Client: mockS3Api}

		// Call the function under test
		err := handler.HandleS3URL("s3://mock-bucket/mock-prefix")

		// Assert that an error was returned
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error processing logs for s3://mock-bucket/mock-key: process logs error")

		// Assert that the ProcessLogs method was called with the correct parameters
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-key",
		})
	})

	t.Run("Multiple Objects", func(t *testing.T) {
		mockProcessor := new(MockLogProcessor)

		mockS3Api := new(MockS3Api)
		mockS3Api.On("ListObjectsV2", mock.Anything).Return(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("mock-prefix/object1")},
				{Key: aws.String("mock-prefix/object2")},
			},
		}, nil)

		handler := &Handler{lp: mockProcessor, s3Client: mockS3Api}

		// Use a WaitGroup to ensure all goroutines complete
		var wg sync.WaitGroup
		wg.Add(2) // Expecting two calls

		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-prefix/object1",
		}).Return(nil).Run(func(args mock.Arguments) {
			wg.Done()
		})
		mockProcessor.On("ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-prefix/object2",
		}).Return(nil).Run(func(args mock.Arguments) {
			wg.Done()
		})

		go func() {
			err := handler.HandleS3URL("s3://mock-bucket/mock-prefix")
			require.NoError(t, err)
		}()

		// Wait for all goroutines to finish
		wg.Wait()

		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-prefix/object1",
		})
		mockProcessor.AssertCalled(t, "ProcessLogs", S3ObjectInfo{
			Bucket: "mock-bucket",
			Key:    "mock-prefix/object2",
		})
	})
}
