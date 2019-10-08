package storage

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamo "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/protobuf/proto"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type DynamoStorageManager struct {
	dataTable        string
	transactionTable string
	dynamoClient     *awsdynamo.DynamoDB
}

func NewDynamoStorageManager(dataTable string, transactionTable string) *DynamoStorageManager {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &DynamoStorageManager{
		dataTable:        dataTable,
		transactionTable: transactionTable,
		dynamoClient:     dc,
	}
}

func (dynamo *DynamoStorageManager) StartTransaction(id string) error {
	return nil
}

func (dynamo *DynamoStorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(transactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	input := constructPutInput(key, dynamo.transactionTable, serialized)
	_, err = dynamo.dynamoClient.PutItem(input)

	return err
}

func (dynamo *DynamoStorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	// TODO: Delete the aborted keys.
	return nil
}

func (dynamo *DynamoStorageManager) Get(key string) (*pb.KeyValuePair, error) {
	input := &awsdynamo.GetItemInput{
		Key:       *constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}

	result := &pb.KeyValuePair{}
	item, err := dynamo.dynamoClient.GetItem(input)
	if err != nil {
		return result, err
	}

	for item == nil || item.Item == nil {
		item, err = dynamo.dynamoClient.GetItem(input)
	}

	bts := item.Item["Value"].B
	err = proto.Unmarshal(bts, result)
	return result, err
}

func (dynamo *DynamoStorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	input := &awsdynamo.GetItemInput{
		Key:       *constructKeyData(transactionKey),
		TableName: aws.String(dynamo.transactionTable),
	}

	result := &pb.TransactionRecord{}
	item, err := dynamo.dynamoClient.GetItem(input)
	if err != nil {
		return result, err
	}

	bts := item.Item["Value"].B
	err = proto.Unmarshal(bts, result)
	return result, err
}

func (dynamo *DynamoStorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	input := constructPutInput(key, dynamo.dataTable, serialized)
	_, err = dynamo.dynamoClient.PutItem(input)

	return err
}

func (dynamo *DynamoStorageManager) Delete(key string) error {
	input := &awsdynamo.DeleteItemInput{
		Key:       *constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}

	_, err := dynamo.dynamoClient.DeleteItem(input)

	return err
}

func (dynamo *DynamoStorageManager) List(prefix string) ([]string, error) {
	// Append a '/' to all prefixes if necessary.
	if prefix[0] != '/' {
		prefix = fmt.Sprintf("/%s", prefix)
	}

	expr := fmt.Sprintf("begins_with(DataKey, :p)")
	additionalKeys := true

	result := []string{}

	input := &awsdynamo.ScanInput{
		ExpressionAttributeValues: map[string]*awsdynamo.AttributeValue{
			":p": {
				S: aws.String(prefix),
			},
		},
		FilterExpression:     aws.String(expr),
		ProjectionExpression: aws.String("DataKey"),
		TableName:            aws.String(dynamo.dataTable),
	}

	for additionalKeys {
		output, err := dynamo.dynamoClient.Scan(input)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Items {
			result = append(result, *obj["DataKey"].S)
		}

		if len(output.LastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = output.LastEvaluatedKey
		} else {
			additionalKeys = false
		}
	}

	return result, nil
}

func constructKeyData(key string) *map[string]*awsdynamo.AttributeValue {
	return &map[string]*awsdynamo.AttributeValue{
		"DataKey": {
			S: aws.String(key),
		},
	}
}

func constructPutInput(key string, table string, data []byte) *awsdynamo.PutItemInput {
	return &awsdynamo.PutItemInput{
		Item: map[string]*awsdynamo.AttributeValue{
			"DataKey": {
				S: aws.String(key),
			},
			"Value": {
				B: data,
			},
		},
		TableName: aws.String(table),
	}
}