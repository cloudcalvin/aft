package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	uuid "github.com/nu7hatch/gouuid"

	"github.com/vsreekanti/aft/config"
	"github.com/vsreekanti/aft/consistency"
	pb "github.com/vsreekanti/aft/proto/aft"
	"github.com/vsreekanti/aft/storage"
)

type AftServer struct {
	Id                      string
	StorageManager          storage.StorageManager
	ConsistencyManager      consistency.ConsistencyManager
	RunningTransactions     map[string]*pb.TransactionRecord
	RunningTransactionLock  *sync.RWMutex
	UpdateBuffer            map[string][]*keyUpdate
	UpdateBufferLock        *sync.RWMutex
	ReadCache               map[string]pb.KeyValuePair
	ReadCacheLock           *sync.RWMutex
	FinishedTransactions    map[string]*pb.TransactionRecord
	FinishedTransactionLock *sync.RWMutex
	KeyVersionIndex         map[string]*[]string
	KeyVersionIndexLock     *sync.RWMutex
}

func NewAftServer() (*AftServer, *config.AftConfig) {
	conf := config.ParseConfig("conf/aft-config.yml")

	var consistencyManager consistency.ConsistencyManager
	switch conf.ConsistencyType {
	case "lww":
		consistencyManager = &consistency.LWWConsistencyManager{}
	case "read-atomic":
		consistencyManager = &consistency.ReadAtomicConsistencyManager{}
	default:
		log.Fatal(fmt.Sprintf("Unrecognized consistencyType %s. Valid types are: lww, read-atomic.", conf.ConsistencyType))
		os.Exit(3)
	}

	// TODO: These paths should be in the conf.
	var storageManager storage.StorageManager
	switch conf.StorageType {
	case "s3":
		storageManager = storage.NewS3StorageManager("vsreekanti")
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("AftData", "AftData")
	case "redis":
		storageManager = storage.NewRedisStorageManager("aft-test.kxmfgs.clustercfg.use1.cache.amazonaws.com:6379", "")
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", conf.StorageType))
		os.Exit(3)
	}

	uid, err := uuid.NewV4()
	if err != nil {
		log.Fatal("Unexpected error while generating UUID: %v", err)
		os.Exit(1)
	}

	server := &AftServer{
		Id:                      uid.String(),
		ConsistencyManager:      consistencyManager,
		StorageManager:          storageManager,
		RunningTransactions:     map[string]*pb.TransactionRecord{},
		RunningTransactionLock:  &sync.RWMutex{},
		UpdateBuffer:            map[string][]*keyUpdate{},
		UpdateBufferLock:        &sync.RWMutex{},
		ReadCache:               map[string]pb.KeyValuePair{},
		ReadCacheLock:           &sync.RWMutex{},
		FinishedTransactions:    map[string]*pb.TransactionRecord{},
		FinishedTransactionLock: &sync.RWMutex{},
		KeyVersionIndex:         map[string]*[]string{},
		KeyVersionIndexLock:     &sync.RWMutex{},
	}

	// Retrieve the list of committed transactions
	transactionKeys, _ := storageManager.List("transactions")
	for _, txnKey := range transactionKeys {
		txnRecord, err := storageManager.GetTransaction(txnKey)
		if err != nil {
			log.Fatal("Unexpected error while retrieving transaction data:\n%v", err)
			os.Exit(1)
		}

		server.FinishedTransactions[txnRecord.Id] = txnRecord

		// Prepopulate the KeyVersionIndex with the list of keys already in the
		// storage engine.
		for _, key := range txnRecord.WriteSet {
			kvName := consistencyManager.GetStorageKeyName(key, txnRecord.Timestamp, txnRecord.Id)

			index, ok := server.KeyVersionIndex[key]
			if !ok {
				index = &[]string{}
				server.KeyVersionIndex[key] = index
			}

			result := append(*index, kvName)
			server.KeyVersionIndex[key] = &result
		}
	}

	fmt.Printf("Prepopulation finished: Found %d transactions and %d keys.\n", len(server.FinishedTransactions), len(server.KeyVersionIndex))

	return server, conf
}
