package storage

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.S3
}

var transactionKey = "/transactions/%s-%d"

func NewS3StorageManager(bucket string) *S3StorageManager {
	s3c := s3.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &S3StorageManager{bucket: "vsreekanti", s3Client: s3c}
}

func (s3 *S3StorageManager) StartTransaction(id string) error {
	return nil
}

func (s3 *S3StorageManager) CommitTransaction(transaction pb.TransactionRecord) error {
	key := fmt.Sprintf(transactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(&transaction)
	if err != nil {
		return err
	}

	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(serialized),
	}

	_, err = s3.s3Client.PutObject(input)

	return err
}

func (s3 *S3StorageManager) AbortTransaction(transaction pb.TransactionRecord) error {
	// TODO: Delete the aborted keys.
	return nil
}

func (s3 *S3StorageManager) Get(key string) (pb.KeyValuePair, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	getObjectOutput, err := s3.s3Client.GetObject(input)
	if err != nil {
		return pb.KeyValuePair{}, err
	}

	body := make([]byte, *getObjectOutput.ContentLength)
	_, err = getObjectOutput.Body.Read(body)

	if err != nil {
		return pb.KeyValuePair{}, err
	}

	result := pb.KeyValuePair{}
	err = proto.Unmarshal(body, &result)
	if err != nil {
		return pb.KeyValuePair{}, err
	}

	return result, nil
}

func (s3 *S3StorageManager) Put(key string, val pb.KeyValuePair) error {
	serialized, err := proto.Marshal(&val)
	if err != nil {
		return err
	}

	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(serialized),
	}

	_, err = s3.s3Client.PutObject(input)

	return err
}

func (s3 *S3StorageManager) Delete(key string) error {
	input := &awss3.DeleteObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	_, err := s3.s3Client.DeleteObject(input)
	return err
}

func (s3 *S3StorageManager) List(prefix string) ([]string, error) {
	input := &awss3.ListObjectsV2Input{
		Bucket: &s3.bucket,
		Prefix: &prefix,
	}

	result, err := s3.s3Client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	returnValue := make([]string, len(result.Contents))

	for index, val := range result.Contents {
		returnValue[index] = *val.Key
	}

	return returnValue, nil
}
