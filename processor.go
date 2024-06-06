package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"sync"
	"time"
)

type LogProcessor interface {
	ProcessLogs(s3Object S3ObjectInfo) error
}

type S3Api interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

type LogEntry struct {
	Data      map[string]string // Map of field name to value, this will be converted to JSON
	Timestamp time.Time
}

type CloudWatchLogProcessor struct {
	s3Client   S3Api
	cwClient   CloudWatchLogsAPI
	fieldStore Fields
	logConfig  LogConfig
}

type LogConfig struct {
	LogGroupName  string
	LogStreamName string
}

const (
	// maxBatchSize The maximum batch size of a PutLogEvents request to CloudWatch is 1MB (1_048_576 bytes)
	maxBatchSize = 1_048_576
	// maxBatchCount The maximum number of events in a PutLogEvents request to CloudWatch is 10_000
	maxBatchCount = 10_000
)

func NewLogProcessor(config Config) (LogProcessor, error) {
	sess := session.Must(session.NewSession())
	fieldStore, _ := NewFields(config.Fields)
	logConfig := LogConfig{config.LogGroupName, config.LogStreamName}
	cwClient := cloudwatchlogs.New(sess)
	err := EnsureLogGroupAndLogStreamExists(cwClient, logConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating log group and stream: %v", err)
	}
	return &CloudWatchLogProcessor{
		s3Client:   s3.New(sess),
		cwClient:   cwClient,
		fieldStore: fieldStore,
		logConfig:  logConfig,
	}, nil
}

func (lp *CloudWatchLogProcessor) ProcessLogs(s3Object S3ObjectInfo) error {

	log.Printf("processing logs from s3://%s/%s", s3Object.Bucket, s3Object.Key)

	obj, err := lp.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3Object.Bucket),
		Key:    aws.String(s3Object.Key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object: %v", err)
	}
	defer obj.Body.Close()

	reader, writer := io.Pipe()

	// Decompress the gzip file in a goroutine
	go func() {
		gzipReader, err := gzip.NewReader(obj.Body)
		if err != nil {
			writer.CloseWithError(err)

			return
		}
		defer gzipReader.Close()
		// Copy decompressed data to writer
		if _, err := io.Copy(writer, gzipReader); err != nil {
			writer.CloseWithError(err)

			return
		}
		writer.Close()
	}()

	// Set channel buffer size to 1.25 times the max batch count to avoid blocking
	entryChan := make(chan LogEntry, int(float64(maxBatchCount)*1.25))

	counter := SafeCounter{v: 0}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		var events []*cloudwatchlogs.InputLogEvent
		var currentBatchSize int
		for entry := range entryChan {
			jsonData, err := json.Marshal(entry.Data)
			if err != nil {
				fmt.Println("error marshaling log entry to JSON:", err)
			}
			event := &cloudwatchlogs.InputLogEvent{
				Message:   aws.String(string(jsonData)),
				Timestamp: aws.Int64(entry.Timestamp.UnixMilli()),
			}
			eventSize := EstimateEventSize(event)
			// Check if adding this event would exceed the size limit
			if len(events) > 0 && (currentBatchSize+eventSize > maxBatchSize || len(events) >= maxBatchCount) {
				// If it does, send the current batch
				err := SendEventsToCloudWatch(lp.cwClient, lp.logConfig, events)
				if err != nil {
					fmt.Println("error sending events to CloudWatch:", err)
				}
				// Increment counter and reset the batch
				counter.Increment(len(events))
				events = nil
				currentBatchSize = 0
			}
			// Add the event to the batch
			events = append(events, event)
			currentBatchSize += eventSize
		}
		// Send any remaining events
		if len(events) > 0 {
			err := SendEventsToCloudWatch(lp.cwClient, lp.logConfig, events)
			if err != nil {
				fmt.Println("error sending events to CloudWatch:", err)
			}
			counter.Increment(len(events))
		}
	}()

	if err := processRecords(reader, entryChan, lp.fieldStore); err != nil {
		fmt.Println("error processing records", err)
	}

	close(entryChan)
	wg.Wait()
	fmt.Printf("processed %d log entries\n", counter.Value())

	return nil
}

func processRecords(reader io.Reader, entryChan chan LogEntry, fieldStore Fields) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ' '
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading a record: %v", err)
		}
		entry, err := recordToLogEntry(record, fieldStore)
		if err != nil {
			return err
		}
		entryChan <- entry
	}

	return nil
}

func recordToLogEntry(record []string, fieldStore Fields) (LogEntry, error) {
	// Check if the record has the expected number of fields
	if len(record) != len(fieldNames) {
		return LogEntry{}, fmt.Errorf("invalid log format: expected %d fields, got %d", len(fieldNames), len(record))
	}
	timestamp, err := time.Parse(time.RFC3339, record[1]) // Timestamp should be at index 1
	if err != nil {
		return LogEntry{}, fmt.Errorf("error parsing timestamp: %v", err)
	}
	entryMap := make(map[string]string)
	for i, value := range record {
		// Only include the fields that we want
		if fieldStore.IncludeField(i) {
			fieldName, _ := fieldStore.GetFieldNameByIndex(i)
			entryMap[fieldName] = value
		}
	}

	return LogEntry{
		Data:      entryMap,
		Timestamp: timestamp,
	}, nil
}
