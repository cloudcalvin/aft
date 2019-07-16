package main

import (
	"bytes"

	awss3 "github.com/aws/aws-sdk-go/service/s3"
)

type StorageManager interface {
	Put(key string, val []byte) bool
	Get(key string) ([]byte, error)
}

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.S3
}

func (s3 *S3StorageManager) Get(key string) ([]byte, error) {
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

func (s3 *S3StorageManager) Put(key string, val []byte) bool {
	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(val),
	}

	_, err := s3.s3Client.PutObject(input)

	return err == nil
}
