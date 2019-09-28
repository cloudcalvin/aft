package main

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/nu7hatch/gouuid"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

	"github.com/vsreekanti/aft/consistency"
	pb "github.com/vsreekanti/aft/proto/aft"
	"github.com/vsreekanti/aft/storage"
)

type keyUpdate struct {
	key     string
	tid     string
	value   []byte
	written bool
}

type aftConfig struct {
	ConsistencyType string   `yaml:"consistencyType"`
	StorageType     string   `yaml:"storageType"`
	IpAddress       string   `yaml:"ipAddress"`
	ReplicaList     []string `yaml:"replicaList"`
}

type aftServer struct {
	id                   string
	transactions         map[string]pb.TransactionRecord
	updateBuffer         map[string][]keyUpdate
	readCache            map[string]pb.KeyValuePair
	storageManager       storage.StorageManager
	consistencyManager   consistency.ConsistencyManager
	FinishedTransactions map[string]pb.TransactionRecord
	TransactionLock      *sync.Mutex
}

func (s *aftServer) StartTransaction(ctx context.Context, _ *empty.Empty) (*pb.TransactionTag, error) {
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	tid := uid.String()
	transactionsTs := time.Now().UnixNano()

	txn := pb.TransactionRecord{
		Id:        tid,
		Timestamp: transactionsTs,
		Status:    pb.TransactionStatus_RUNNING,
		ReplicaId: s.id,
		WriteSet:  []string{},
		ReadSet:   map[string]string{},
	}

	s.transactions[tid] = txn
	s.storageManager.StartTransaction(tid)

	return &pb.TransactionTag{Id: tid, Status: pb.TransactionStatus_RUNNING}, nil
}

func (s *aftServer) Write(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	txn := s.transactions[requests.Tid]
	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, update := range requests.Pairs {
		key := update.Key
		txn.WriteSet = append(txn.WriteSet, key)

		s.updateBuffer[requests.Tid] = append(s.updateBuffer[requests.Tid],
			keyUpdate{key: key, tid: requests.Tid, value: update.Value})
		resp.Pairs = append(resp.Pairs, &pb.KeyRequest_KeyPair{Key: key})
	}

	return resp, nil
}

func (s *aftServer) Read(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	txn := s.transactions[requests.Tid]
	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, request := range requests.Pairs {
		var returnValue []byte
		// If the key is in the local update buffer, return it immediately.
		found := false
		if buffer, ok := s.updateBuffer[txn.Id]; ok {
			for _, update := range buffer {
				if update.key == request.Key {
					returnValue = update.value
					found = true
				}
			}
		}

		if !found {
			key, err := s.consistencyManager.GetValidKeyVersion(request.Key, txn, txn.ReadSet, s.storageManager, s.readCache)
			if err != nil {
				return &pb.KeyRequest{}, err
			}

			// If we've read the key version before, return that version.
			if val, ok := s.readCache[key]; ok {
				returnValue = val.Value
			} else { // Otherwise, get the correct key version from storage.
				returnValue, err := s.storageManager.Get(key)

				// If the GET request returns an error, that means the key was not
				// accessible, so we return nil.
				if err != nil {
					return &pb.KeyRequest{}, err
				} else { // Otherwise, add this key to our read cache.
					s.readCache[key] = returnValue
				}
			}

			if returnValue != nil {
				txn.ReadSet[request.Key] = key
			}
		}

		resp.Pairs = append(resp.Pairs, &pb.KeyRequest_KeyPair{Key: request.Key, Value: returnValue})
	}

	return resp, nil
}

func (s *aftServer) CommitTransaction(ctx context.Context, tag *pb.TransactionTag) (*pb.TransactionTag, error) {
	tid := tag.Id
	txn := s.transactions[tid]

	ok := s.consistencyManager.ValidateTransaction(tid, txn.ReadSet, txn.WriteSet)

	if ok {
		// Construct the set of keys that were written together to put into the KVS
		// metadata.
		cowrittenKeys := make([]string, len(s.updateBuffer[tid]))
		for index, update := range s.updateBuffer[tid] {
			cowrittenKeys[index] = update.key
		}

		// Write updates to storage manager.
		success := true
		for _, update := range s.updateBuffer[tid] {
			key := s.consistencyManager.GetStorageKeyName(update.key, &txn)
			val := pb.KeyValuePair{
				Key:           update.key,
				Value:         update.value,
				CowrittenKeys: cowrittenKeys,
				Tid:           tid,
				Timestamp:     txn.Timestamp,
			}

			err := s.storageManager.Put(key, val)

			if err != nil {
				success = false
				break
			}
		}

		if !success {
			// TODO: Rollback the transaction.
			txn.Status = pb.TransactionStatus_ABORTED
		} else {
			txn.Status = pb.TransactionStatus_COMMITTED
		}
	} else {
		txn.Status = pb.TransactionStatus_ABORTED
	}

	err := s.storageManager.CommitTransaction(txn)
	if err != nil {
		return nil, err
	}

	// Move the transaction from the running transactions to the finished set.
	delete(s.transactions, tid)
	s.FinishedTransactions[tid] = txn

	delete(s.updateBuffer, tid)
	return &pb.TransactionTag{Id: tid, Status: txn.Status}, nil
}

func (s *aftServer) AbortTransaction(ctx context.Context, tag *pb.TransactionTag) (*pb.TransactionTag, error) {
	tid := tag.Id
	txn := s.transactions[tid]
	delete(s.updateBuffer, tid)
	s.storageManager.AbortTransaction(s.transactions[tid])
	txn.Status = pb.TransactionStatus_ABORTED

	// Move the transaction from the running transactions to the finished set.
	delete(s.transactions, tid)
	s.FinishedTransactions[tid] = txn

	return &pb.TransactionTag{Id: tid, Status: pb.TransactionStatus_ABORTED}, nil
}

func (s *aftServer) UpdateMetadata(ctx context.Context, list *pb.TransactionList) (*empty.Empty, error) {
	s.TransactionLock.Lock()
	for _, record := range list.Records {
		s.FinishedTransactions[record.Id] = *record
	}
	s.TransactionLock.Unlock()

	return &empty.Empty{}, nil
}

const (
	port = ":7654"
)

func newAftServer() (*aftServer, *aftConfig) {
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
		log.Fatal("Unrecognized consistencyType %s. Valid types are: lww, read-atomic.", config.ConsistencyType)
		os.Exit(3)
	}

	var storageManager storage.StorageManager
	if config.StorageType == "s3" {
		storageManager = &storage.S3StorageManager{}
	} else {
		log.Fatal("Unrecognized storageType %s. Valid types are: s3.", config.ConsistencyType)
		os.Exit(3)
	}

	uid, err := uuid.NewV4()
	if err != nil {
		log.Fatal("Unexpected error while generating UUID: %v", err)
		os.Exit(1)
	}

	return &aftServer{
		id:                   uid.String(),
		transactions:         map[string]pb.TransactionRecord{},
		updateBuffer:         map[string][]keyUpdate{},
		consistencyManager:   consistencyManager,
		storageManager:       storageManager,
		FinishedTransactions: map[string]pb.TransactionRecord{},
		TransactionLock:      &sync.Mutex{},
	}, &config
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}

	server := grpc.NewServer()
	aft, config := newAftServer()
	pb.RegisterAftServer(server, aft)

	// Start the multicast goroutine.
	go MulticastRoutine(aft, config.ReplicaList)

	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}
}
