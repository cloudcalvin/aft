package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/nu7hatch/gouuid"
	"google.golang.org/grpc"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type keyUpdate struct {
	key     string
	tid     string
	value   []byte
	written bool
}

func (s *AftServer) StartTransaction(ctx context.Context, _ *empty.Empty) (*pb.TransactionTag, error) {
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	tid := uid.String()
	transactionsTs := time.Now().UnixNano()

	txn := &pb.TransactionRecord{
		Id:        tid,
		Timestamp: transactionsTs,
		Status:    pb.TransactionStatus_RUNNING,
		ReplicaId: s.Id,
		WriteSet:  []string{},
		ReadSet:   map[string]string{},
	}

	s.StorageManager.StartTransaction(tid)

	s.RunningTransactionLock.Lock()
	s.RunningTransactions[tid] = txn
	s.RunningTransactionLock.Unlock()

	return &pb.TransactionTag{Id: tid, Status: pb.TransactionStatus_RUNNING}, nil
}

func (s *AftServer) Write(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[requests.Tid]
	s.RunningTransactionLock.RUnlock()

	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, update := range requests.Pairs {
		key := update.Key
		txn.WriteSet = append(txn.WriteSet, key)

		s.UpdateBufferLock.Lock()
		s.UpdateBuffer[requests.Tid] = append(s.UpdateBuffer[requests.Tid],
			&keyUpdate{key: key, tid: requests.Tid, value: update.Value})
		s.UpdateBufferLock.Unlock()

		resp.Pairs = append(resp.Pairs, &pb.KeyRequest_KeyPair{Key: key})
	}

	return resp, nil
}

func (s *AftServer) Read(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[requests.Tid]
	s.RunningTransactionLock.RUnlock()

	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, request := range requests.Pairs {
		var returnValue []byte
		// If the key is in the local update buffer, return it immediately.
		found := false
		s.UpdateBufferLock.RLock()
		buffer, ok := s.UpdateBuffer[txn.Id]
		s.UpdateBufferLock.RUnlock()

		if ok {
			for _, update := range buffer {
				if update.key == request.Key {
					returnValue = update.value
					found = true
				}
			}
		}

		if !found {
			key, err := s.ConsistencyManager.GetValidKeyVersion(request.Key, txn, &s.ReadCache, s.ReadCacheLock, &s.KeyVersionIndex, s.KeyVersionIndexLock)
			if err != nil {
				return &pb.KeyRequest{}, err
			}

			// If we've read the key version before, return that version.
			s.ReadCacheLock.RLock()
			val, ok := s.ReadCache[key]
			s.ReadCacheLock.RUnlock()

			if ok {
				returnValue = val.Value
			} else { // Otherwise, get the correct key version from storage.
				kvPair, err := s.StorageManager.Get(key)

				// If the GET request returns an error, that means the key was not
				// accessible, so we return nil.
				if err != nil {
					return &pb.KeyRequest{}, err
				} else { // Otherwise, add this key to our read cache.
					s.ReadCacheLock.Lock()
					s.ReadCache[key] = *kvPair
					s.ReadCacheLock.Unlock()

					returnValue = kvPair.Value
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

func (s *AftServer) CommitTransaction(ctx context.Context, tag *pb.TransactionTag) (*pb.TransactionTag, error) {
	tid := tag.Id
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	ok := s.ConsistencyManager.ValidateTransaction(tid, txn.ReadSet, txn.WriteSet)

	if ok {
		// Construct the set of keys that were written together to put into the KVS
		// metadata.
		s.UpdateBufferLock.RLock()
		cowrittenKeys := make([]string, len(s.UpdateBuffer[tid]))
		keyUpdates := make([]*keyUpdate, len(s.UpdateBuffer[tid]))

		for index, update := range s.UpdateBuffer[tid] {
			cowrittenKeys[index] = update.key
			keyUpdates[index] = update
		}

		s.UpdateBufferLock.RUnlock()

		// Write updates to storage manager.
		success := true
		for _, update := range keyUpdates {
			key := s.ConsistencyManager.GetStorageKeyName(update.key, txn.Timestamp, tid)
			val := &pb.KeyValuePair{
				Key:           update.key,
				Value:         update.value,
				CowrittenKeys: cowrittenKeys,
				Tid:           tid,
				Timestamp:     txn.Timestamp,
			}

			err := s.StorageManager.Put(key, val)

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

	err := s.StorageManager.CommitTransaction(txn)
	if err != nil {
		return nil, err
	}

	// Move the transaction from the running transactions to the finished set.
	s.RunningTransactionLock.Lock()
	delete(s.RunningTransactions, tid)
	s.RunningTransactionLock.Unlock()

	s.FinishedTransactionLock.Lock()
	s.FinishedTransactions[tid] = txn
	s.FinishedTransactionLock.Unlock()

	s.UpdateBufferLock.Lock()
	delete(s.UpdateBuffer, tid)
	s.UpdateBufferLock.Unlock()

	s.updateKeyVersionIndex(txn)
	return &pb.TransactionTag{Id: tid, Status: txn.Status}, nil
}

func (s *AftServer) AbortTransaction(ctx context.Context, tag *pb.TransactionTag) (*pb.TransactionTag, error) {
	tid := tag.Id
	s.RunningTransactionLock.RLock()
	txn := s.RunningTransactions[tid]
	s.RunningTransactionLock.RUnlock()

	s.UpdateBufferLock.Lock()
	delete(s.UpdateBuffer, tid)
	s.UpdateBufferLock.Unlock()

	s.StorageManager.AbortTransaction(txn)
	txn.Status = pb.TransactionStatus_ABORTED

	// Move the transaction from the running transactions to the finished set.
	s.RunningTransactionLock.Lock()
	delete(s.RunningTransactions, tid)
	s.RunningTransactionLock.Unlock()

	s.FinishedTransactionLock.Lock()
	s.FinishedTransactions[tid] = txn
	s.FinishedTransactionLock.Unlock()

	return &pb.TransactionTag{Id: tid, Status: pb.TransactionStatus_ABORTED}, nil
}

func (s *AftServer) UpdateMetadata(ctx context.Context, list *pb.TransactionList) (*empty.Empty, error) {
	s.FinishedTransactionLock.Lock()
	for _, record := range list.Records {
		s.FinishedTransactions[record.Id] = record
		s.updateKeyVersionIndex(record)
	}
	s.FinishedTransactionLock.Unlock()

	return &empty.Empty{}, nil
}

func (s *AftServer) updateKeyVersionIndex(transaction *pb.TransactionRecord) {
	s.KeyVersionIndexLock.Lock()
	for _, key := range transaction.WriteSet {
		kvName := s.ConsistencyManager.GetStorageKeyName(key, transaction.Timestamp, transaction.Id)

		index, ok := s.KeyVersionIndex[key]
		if !ok {
			index = &[]string{}
			s.KeyVersionIndex[key] = index
		}

		result := append(*index, kvName)
		s.KeyVersionIndex[key] = &result
	}
	s.KeyVersionIndexLock.Unlock()
}

const (
	port = ":7654"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}

	server := grpc.NewServer()
	aft, config := NewAftServer()
	pb.RegisterAftServer(server, aft)

	// Start the multicast goroutine.
	go MulticastRoutine(aft, config.ReplicaList)

	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}
}
