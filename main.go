package main

import (
	"context"
	"log"
	"net"

	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/nu7hatch/gouuid"
	"google.golang.org/grpc"

	"github.com/vsreekanti/aft/consistency"
	pb "github.com/vsreekanti/aft/proto/aft"
	"github.com/vsreekanti/aft/storage"
)

type aftServer struct {
	transactions       map[string]Transaction
	updateBuffer       map[string][]KeyUpdate
	storageManager     storage.StorageManager
	consistencyManager consistency.ConsistencyManager
}

func (s *aftServer) StartTransaction(ctx context.Context, _ *empty.Empty) (*pb.Transaction, error) {
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	tid := uid.String()

	txn := Transaction{id: tid, txnStatus: pb.TransactionStatus_RUNNING}
	s.transactions[tid] = txn

	s.storageManager.StartTransaction(tid)

	return &pb.Transaction{Id: tid}, nil
}

func (s *aftServer) Write(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	txn := s.transactions[requests.Tid]
	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, update := range requests.Pairs {
		key := update.Key
		txn.writeSet = append(txn.writeSet, key)

		s.updateBuffer[requests.Tid] = append(s.updateBuffer[requests.Tid],
			KeyUpdate{key: key, tid: requests.Tid, value: update.Value})
		resp.Pairs = append(resp.Pairs, &pb.KeyRequest_KeyPair{Key: key})
	}

	return resp, nil
}

func (s *aftServer) Read(ctx context.Context, requests *pb.KeyRequest) (*pb.KeyRequest, error) {
	txn := s.transactions[requests.Tid]
	resp := &pb.KeyRequest{Tid: requests.Tid}

	for _, request := range requests.Pairs {
		key := s.consistencyManager.GetValidKeyVersion(request.Key, requests.Tid)
		val, err := s.storageManager.Get(key, requests.Tid)
		if err == nil {
			// TODO: the transaction should probably abort? or do we just return that
			// the key doesn't exist
		}

		txn.readSet = append(txn.readSet, key)
		resp.Pairs = append(resp.Pairs, &pb.KeyRequest_KeyPair{Key: key, Value: val})
	}

	return resp, nil
}

func (s *aftServer) CommitTransaction(ctx context.Context, transaction *pb.Transaction) (*pb.Transaction, error) {
	tid := transaction.Id
	txn := s.transactions[tid]

	ok := s.consistencyManager.ValidateTransaction(tid, txn.readSet, txn.writeSet)
	var status pb.TransactionStatus

	if ok {
		// write updates to storage managers
		success := true
		for _, update := range s.updateBuffer[tid] {
			ok = s.storageManager.Put(update.key, update.value, tid)

			if !ok {
				success = false
				break
			}
		}

		if !success {
			// TODO: how do we deal with this? we'd have to roll back, but is this a
			// real concern?
			status = pb.TransactionStatus_ABORTED
		} else {
			status = pb.TransactionStatus_COMMITTED
		}
	} else {
		status = pb.TransactionStatus_ABORTED
	}

	s.storageManager.CommitTransaction(tid)
	s.transactions[tid].SetStatus(status)

	// TODO: we should eventually GC finished transactions, but when?
	delete(s.updateBuffer, tid)
	return &pb.Transaction{Id: tid, Status: status}, nil
}

func (s *aftServer) AbortTransaction(ctx context.Context, transaction *pb.Transaction) (*pb.Transaction, error) {
	tid := transaction.Id
	delete(s.updateBuffer, tid)
	s.storageManager.AbortTransaction(tid)
	s.transactions[tid].SetStatus(pb.TransactionStatus_ABORTED)

	return &pb.Transaction{Id: tid, Status: pb.TransactionStatus_ABORTED}, nil
}

const (
	port = ":7654"
)

func newAftServer() *aftServer {
	// TODO: add configs for different consistency, storage managers
	l := &consistency.LWWConsistencyManager{}

	s := storage.NewS3StorageManager("vsreekanti")

	return &aftServer{
		transactions:       map[string]Transaction{},
		updateBuffer:       map[string][]KeyUpdate{},
		consistencyManager: l,
		storageManager:     s,
	}
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v", port, err)
	}

	server := grpc.NewServer()
	aft := newAftServer()
	pb.RegisterAftServer(server, aft)

	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v", port, err)
	}
}
