package storage

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
)

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.S3
}

func NewS3StorageManager(bucket string) *S3StorageManager {
	s3c := s3.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &S3StorageManager{bucket: "vsreekanti", s3Client: s3c}
}

func (s3 *S3StorageManager) StartTransaction(id string) bool {
	return true
}

func (s3 *S3StorageManager) CommitTransaction(id string) bool {
	return true
}

func (s3 *S3StorageManager) AbortTransaction(id string) bool {
	return true
}

func (s3 *S3StorageManager) Get(key string, _ string) ([]byte, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	result, err := s3.s3Client.GetObject(input)
	if err != nil {
		return nil, err
	}

	body := make([]byte, *result.ContentLength)
	_, err = result.Body.Read(body)

	if err != nil {
		return nil, err
	}

	return body, nil
}

func (s3 *S3StorageManager) Put(key string, val []byte, _ string) bool {
	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(val),
	}

	_, err := s3.s3Client.PutObject(input)

	return err == nil
}
