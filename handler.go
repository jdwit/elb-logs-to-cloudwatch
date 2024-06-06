package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"sync"
)

type Handler struct {
	lp       LogProcessor
	s3Client S3Api
}

type S3ObjectInfo struct {
	Bucket string
	Key    string
}

// concurrency is the max number of concurrent log processing operations
const concurrency = 10

func NewHandler() (*Handler, error) {
	sess := session.Must(session.NewSession())
	config, err := LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	lp, err := NewLogProcessor(config)
	if err != nil {
		return nil, err
	}
	return &Handler{lp: lp, s3Client: s3.New(sess)}, nil
}

func (h *Handler) processS3Objects(s3Objects []S3ObjectInfo) error {
	errs := make(chan error)
	var wg sync.WaitGroup
	concurrent := make(chan int, concurrency) // limit concurrent processing
	for _, s3obj := range s3Objects {
		wg.Add(1)
		concurrent <- 1
		go func(s3obj S3ObjectInfo) {
			defer func() { wg.Done(); <-concurrent }()
			err := h.lp.ProcessLogs(s3obj)
			if err != nil {
				errs <- fmt.Errorf("error processing logs for s3://%s/%s: %w", s3obj.Bucket, s3obj.Key, err)
			}
		}(s3obj)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) HandleLambdaEvent(event S3ObjectCreatedEvent) error {
	var s3Objects []S3ObjectInfo
	for _, record := range event.Records {
		s3Objects = append(s3Objects, S3ObjectInfo{
			Bucket: record.S3.Bucket.Name,
			Key:    record.S3.Object.Key,
		})
	}
	return h.processS3Objects(s3Objects)
}

func (h *Handler) HandleS3URL(url string) error {
	bucket, prefix, err := ParseS3URL(url)
	if err != nil {
		return fmt.Errorf("failed to parse S3 URL: %v", err)
	}

	var s3Objects []S3ObjectInfo
	var continuationToken *string
	for {
		resp, err := h.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}

		for _, item := range resp.Contents {
			s3Objects = append(s3Objects, S3ObjectInfo{
				Bucket: bucket,
				Key:    *item.Key,
			})
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	return h.processS3Objects(s3Objects)
}
