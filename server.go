package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	uuid "github.com/nu7hatch/gouuid"
	"gopkg.in/yaml.v2"

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

type aftConfig struct {
	ConsistencyType string   `yaml:"consistencyType"`
	StorageType     string   `yaml:"storageType"`
	IpAddress       string   `yaml:"ipAddress"`
	ReplicaList     []string `yaml:"replicaList"`
}

func NewAftServer() (*AftServer, *aftConfig) {
	bts, err := ioutil.ReadFile("aft-config.yml")
	if err != nil {
		log.Fatal("Unable to read aft-config.yml. Please make sure that the config is properly configured and retry:\n%v", err)
		os.Exit(1)
	}

	var config aftConfig
	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		log.Fatal("Unable to correctly parse aft-config.yml. Please check the config file and retry:\n%v", err)
		os.Exit(2)
	}

	var consistencyManager consistency.ConsistencyManager
	if config.ConsistencyType == "lww" {
		consistencyManager = &consistency.LWWConsistencyManager{}
	} else if config.ConsistencyType == "read-atomic" {
		consistencyManager = &consistency.ReadAtomicConsistencyManager{}
	} else {
		log.Fatal(fmt.Sprintf("Unrecognized consistencyType %s. Valid types are: lww, read-atomic.", config.ConsistencyType))
		os.Exit(3)
	}

	// TODO: These paths should be in the conf.
	var storageManager storage.StorageManager
	if config.StorageType == "s3" {
		storageManager = storage.NewS3StorageManager("vsreekanti")
	} else if config.StorageType == "dynamo" {
		storageManager = storage.NewDynamoStorageManager("AftData", "AftData")
	} else if config.StorageType == "redis" {
		storageManager = storage.NewRedisStorageManager("aft-test.kxmfgs.clustercfg.use1.cache.amazonaws.com:6379", "")
	} else {
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", config.StorageType))
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

	return server, &config
}
