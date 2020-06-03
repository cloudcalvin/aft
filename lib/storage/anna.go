package storage

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"

	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	TransactionsKey = "Transactions"
)

type AnnaStorageManager struct {
	annaClient *AnnaClient
}

func NewAnnaStorageManager(string, ipAddress string, elbAddress string) *AnnaStorageManager {
	anna := NewAnnaClient(elbAddress, ipAddress, false, 1)

	return &AnnaStorageManager{annaClient: anna}
}

func (anna *AnnaStorageManager) StartTransaction(id string) error {
	return nil
}

func (anna *AnnaStorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(TransactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	_, err = anna.annaClient.Put(key, serialized)

	if err != nil {
		return err
	}

	// Add this transaction key to the set of committed transactions.
	txns, _ := anna.annaClient.GetSet(TransactionsKey)
	txns = append(txns, key)
	_, err = anna.annaClient.PutSet(TransactionsKey, txns)

	return err
}

func (anna *AnnaStorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	// TODO: Delete the aborted keys.
	return nil
}

func (anna *AnnaStorageManager) Get(key string) (*pb.KeyValuePair, error) {
	result := &pb.KeyValuePair{}

	bts, err := anna.annaClient.Get(key)
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal(bts, result)
	return result, err
}

func (anna *AnnaStorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	result := &pb.TransactionRecord{}

	bts, err := anna.annaClient.Get(transactionKey)
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal(bts, result)
	return result, err
}

func (anna *AnnaStorageManager) MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error) {
	results := make([]*pb.TransactionRecord, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := anna.GetTransaction(key)
		if err != nil {
			return &[]*pb.TransactionRecord{}, err
		}

		results[index] = txn
	}

	return &results, nil
}

func (anna *AnnaStorageManager) Put(key string, val *pb.KeyValuePair) error {
	serialized, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	_, err = anna.annaClient.Put(key, serialized)
	return err
}

func (anna *AnnaStorageManager) MultiPut(data *map[string]*pb.KeyValuePair) error {
	for key, val := range *data {
		err := anna.Put(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (anna *AnnaStorageManager) Delete(key string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) MultiDelete(keys *[]string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) List(prefix string) ([]string, error) {
	if prefix != "transactions" {
		return nil, errors.New(fmt.Sprintf("Unexpected prefix: %s", prefix))
	}

	return anna.annaClient.GetSet(TransactionsKey)
}
