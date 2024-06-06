package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"log"
	"sort"
)

type CloudWatchLogsAPI interface {
	PutLogEvents(*cloudwatchlogs.PutLogEventsInput) (*cloudwatchlogs.PutLogEventsOutput, error)
	CreateLogGroup(*cloudwatchlogs.CreateLogGroupInput) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(*cloudwatchlogs.CreateLogStreamInput) (*cloudwatchlogs.CreateLogStreamOutput, error)
	DescribeLogGroups(*cloudwatchlogs.DescribeLogGroupsInput) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
	DescribeLogStreams(*cloudwatchlogs.DescribeLogStreamsInput) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
}

func EnsureLogGroupAndLogStreamExists(client CloudWatchLogsAPI, logConfig LogConfig) error {
	err := ensureLogGroupExists(client, logConfig.LogGroupName)
	if err != nil {
		return err
	}
	err = ensureLogStreamExists(client, logConfig.LogGroupName, logConfig.LogStreamName)

	return err
}

func ensureLogGroupExists(client CloudWatchLogsAPI, name string) error {
	resp, err := client.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{})
	if err != nil {
		return err
	}
	for _, logGroup := range resp.LogGroups {
		if *logGroup.LogGroupName == name {
			return nil
		}
	}
	log.Printf("creating log group %s", name)
	_, err = client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(name),
	})

	return err
}

func ensureLogStreamExists(client CloudWatchLogsAPI, logGroupName, logStreamName string) error {
	resp, err := client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(logGroupName),
	})
	if err != nil {
		return err
	}
	for _, logStream := range resp.LogStreams {
		if *logStream.LogStreamName == logStreamName {
			return nil
		}
	}
	log.Printf("creating log stream %s in log group %s", logStreamName, logGroupName)
	_, err = client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	})

	return err
}

func SendEventsToCloudWatch(client CloudWatchLogsAPI, logConfig LogConfig, events []*cloudwatchlogs.InputLogEvent) error {
	// Log events in a single PutLogEvents request must be in chronological order
	sort.Slice(events, func(i, j int) bool {
		return aws.Int64Value(events[i].Timestamp) < aws.Int64Value(events[j].Timestamp)
	})
	_, err := client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     events,
		LogGroupName:  aws.String(logConfig.LogGroupName),
		LogStreamName: aws.String(logConfig.LogStreamName),
	})

	return err
}

func EstimateEventSize(event *cloudwatchlogs.InputLogEvent) int {
	// Request size to CloudWatch is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
	return len(*event.Message) + 26
}
