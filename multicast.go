package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Ports to notify and be notified of new transactions.
	TxnPort = 7777

	// Ports to notify and be notified of pending transaction deletes.
	PendingTxnDeletePullPort = 7779
	PendingTxnDeletePushPort = 7780

	// Port to be notified of successful transaction deletes.
	TxnDeletePullPort = 7781
)

func createSocket(tp zmq.Type, context *zmq.Context, address string, bind bool) *zmq.Socket {
	sckt, err := context.NewSocket(tp)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}

	if bind {
		err = sckt.Bind(address)
	} else {
		err = sckt.Connect(address)
	}

	if err != nil {
		fmt.Println("Unexpected error while binding/connecting socket:\n", err)
		os.Exit(1)
	}

	return sckt
}

func MulticastRoutine(server *AftServer, ipAddress string, replicaList []string, managerAddress string) {
	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	// Sockets to receive and send notifications about new transactions.
	updatePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, TxnPort), true)
	managerPusher := createSocket(zmq.PUSH, context, fmt.Sprintf(PushTemplate, managerAddress, TxnPort), false)

	updatePushers := make([]*zmq.Socket, len(replicaList)-1)
	index := 0
	for _, replica := range replicaList {
		if replica != ipAddress {
			address := fmt.Sprintf(PushTemplate, replica, TxnPort)
			fmt.Println("Connecting to ", address)
			pusher := createSocket(zmq.PUSH, context, address, false)
			updatePushers[index] = pusher
			index += 1
		}
	}

	// Sockets to receive and send notifications about pending transaction
	// deletes.
	pendingDeletePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, PendingTxnDeletePullPort), true)
	pendingDeletePusher := createSocket(zmq.PUSH, context, fmt.Sprintf(PushTemplate, managerAddress, PendingTxnDeletePushPort), false)

	// Sockets to pull udpates about successfully deleted transactions.
	deletePuller := createSocket(zmq.PULL, context, fmt.Sprintf(PullTemplate, TxnDeletePullPort), true)

	// Create a new poller to wait for new updates.
	poller := zmq.NewPoller()
	poller.Add(updatePuller, zmq.POLLIN)
	poller.Add(pendingDeletePuller, zmq.POLLIN)
	poller.Add(deletePuller, zmq.POLLIN)

	// A set to track which transactions we've already gossiped.
	seenTransactions := map[string]bool{}

	// We use this map to make sure we don't say to GC the same transaction more
	// than once.
	deletedMarkedTransactions := map[string]bool{}

	reportStart := time.Now()
	for true {
		// Wait a 100ms for a new message; we know by default that there is only
		// one socket to poll, so we don't have to check which socket we've
		// received a message on.
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case updatePuller:
				{
					bts, _ := updatePuller.RecvBytes(zmq.DONTWAIT)

					newTransactions := &pb.TransactionList{}
					err = proto.Unmarshal(bts, newTransactions)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}
					fmt.Printf("Received %d new transactions.\n", len(newTransactions.Records))

					// Filter out any transactions we have already heard about.
					unseenTransactions := &pb.TransactionList{}
					for _, record := range newTransactions.Records {
						if _, ok := seenTransactions[record.Id]; !ok {
							unseenTransactions.Records = append(unseenTransactions.Records, record)
							seenTransactions[record.Id] = true
						}
					}

					server.UpdateMetadata(unseenTransactions)
				}
			case pendingDeletePuller:
				{
					bts, _ := pendingDeletePuller.RecvBytes(zmq.DONTWAIT)

					pendingDeleteIds := &pb.TransactionIdList{}
					err = proto.Unmarshal(bts, pendingDeleteIds)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}
					fmt.Printf("Received %d transaction IDs for pending deletes.\n", len(pendingDeleteIds.Ids))

					validDeleteIds := &pb.TransactionIdList{}
					zeroDepsCount := 0
					depsAverage := 0
					for _, id := range pendingDeleteIds.Ids {
						// Make sure we don't delete a transaction we haven't seen before.
						if _, ok := seenTransactions[id]; ok {
							// Only delete it if we've already considered it dominated
							// locally and we have no dependencies on it. We are guaranteed
							// to have no dependencies because we already deleted it.
							server.LocallyDeletedTransactionsLock.RLock()
							_, deleted := server.LocallyDeletedTransactions[id]
							_, marked := deletedMarkedTransactions[id]
							server.LocallyDeletedTransactionsLock.RUnlock()

							if deleted && !marked {
								validDeleteIds.Ids = append(validDeleteIds.Ids, id)
								deletedMarkedTransactions[id] = true
							}
						}

						server.TransactionDependenciesLock.RLock()
						if server.TransactionDependencies[id] <= 0 {
							zeroDepsCount += 1
						} else {
							depsAverage += server.TransactionDependencies[id]
						}
						server.TransactionDependenciesLock.RUnlock()
					}

					average := float64(depsAverage) / float64(len(pendingDeleteIds.Ids)-len(validDeleteIds.Ids))

					fmt.Printf("Received %d pending deletes and determined %d of them were valid---%d of them had <= 0 dependencies; of there there was an average of %f.\n", len(pendingDeleteIds.Ids), len(validDeleteIds.Ids), zeroDepsCount, average)
					bts, _ = proto.Marshal(validDeleteIds)
					pendingDeletePusher.SendBytes(bts, zmq.DONTWAIT)
				}
			case deletePuller:
				{
					bts, _ := deletePuller.RecvBytes(zmq.DONTWAIT)

					deleteIds := &pb.TransactionIdList{}
					err = proto.Unmarshal(bts, deleteIds)
					if err != nil {
						fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
						continue
					}
					fmt.Printf("Received %d transaction IDs for successful deletes.\n", len(deleteIds.Ids))

					// Delete the successfully deleted IDs from our local seen
					// transactions metadata.
					for _, id := range deleteIds.Ids {
						delete(seenTransactions, id)
						delete(deletedMarkedTransactions, id)

						server.FinishedTransactionLock.Lock()
						delete(server.FinishedTransactions, id)
						server.FinishedTransactionLock.Unlock()

						server.LocallyDeletedTransactionsLock.Lock()
						delete(server.LocallyDeletedTransactions, id)
						server.LocallyDeletedTransactionsLock.Unlock()
					}
				}
			}
		}

		reportEnd := time.Now()
		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			// Lock the mutex, serialize the newly committed transactions, and unlock.
			server.FinishedTransactionLock.RLock()
			message := pb.TransactionList{}

			for tid := range server.FinishedTransactions {
				if _, ok := seenTransactions[tid]; !ok {
					record := server.FinishedTransactions[tid]
					message.Records = append(message.Records, record)
					seenTransactions[tid] = true
				}
			}
			server.FinishedTransactionLock.RUnlock()

			if len(message.Records) > 0 {
				bts, err := proto.Marshal(&message)
				if err != nil {
					fmt.Println("Unexpected error while marshaling Protobufs:\n", err)
					continue
				}

				for _, pusher := range updatePushers {
					pusher.SendBytes(bts, zmq.DONTWAIT)
				}
				managerPusher.SendBytes(bts, zmq.DONTWAIT)
			}

			reportStart = time.Now()
		}
	}
}

func LocalGCRoutine(server *AftServer) {
	for true {
		// Run the GC routine every 100ms.
		// time.Sleep(10 * time.Millisecond)

		dominatedTransactions := map[string]*pb.TransactionRecord{}

		// We create a local copy of the keys so that we don't modify the list
		// while we are iterating over it. Otherwise, we'd have to RLock for the
		// whole GC duration.
		server.FinishedTransactionLock.RLock()
		keys := make([]string, len(server.FinishedTransactions))
		index := 0
		for tid := range server.FinishedTransactions {
			keys[index] = tid
			index += 1
		}
		server.FinishedTransactionLock.RUnlock()

		for _, tid := range keys {
			server.FinishedTransactionLock.RLock()
			txn := server.FinishedTransactions[tid]
			server.FinishedTransactionLock.RUnlock()

			if txn != nil && isTransactionDominated(txn, server) {
				server.LocallyDeletedTransactionsLock.RLock()
				if _, ok := server.LocallyDeletedTransactions[tid]; !ok {
					// We can skip this record if we've already looked at this txn.
					server.TransactionDependenciesLock.RLock()
					if val, ok := server.TransactionDependencies[tid]; !ok || val == 0 {
						dominatedTransactions[tid] = txn
					}
					server.TransactionDependenciesLock.RUnlock()
				}
				server.LocallyDeletedTransactionsLock.RUnlock()
			}

			if len(dominatedTransactions) > 1000 {
				break
			}
		}

		go transactionClearRoutine(&dominatedTransactions, server)

	}
}

func transactionClearRoutine(dominatedTransactions *map[string]*pb.TransactionRecord, server *AftServer) {
	// Delete all of the key version index data, then clean up committed
	// transactions and the read cache.
	cachedKeys := []string{}
	for dominatedTid := range *dominatedTransactions {
		txn := (*dominatedTransactions)[dominatedTid]
		for _, key := range txn.WriteSet {
			keyVersion := server.ConsistencyManager.GetStorageKeyName(key, txn.Timestamp, txn.Id)

			server.KeyVersionIndexLock.Lock()
			if (*server.KeyVersionIndex[key])[keyVersion] {
				cachedKeys = append(cachedKeys, keyVersion)
			}

			delete(*server.KeyVersionIndex[key], keyVersion)
			server.KeyVersionIndexLock.Unlock()
		}
	}

	for tid := range *dominatedTransactions {
		server.LocallyDeletedTransactionsLock.Lock()
		server.LocallyDeletedTransactions[tid] = true
		server.LocallyDeletedTransactionsLock.Unlock()
	}

	// Consider moving these locks inside the loop.
	for _, keyVersion := range cachedKeys {
		server.ReadCacheLock.Lock()
		delete(server.ReadCache, keyVersion)
		server.ReadCacheLock.Unlock()
	}
}

// A transaction is dominated if all the keys in its write set have versions
// that are newer than the version from this transaction.
func isTransactionDominated(transaction *pb.TransactionRecord, server *AftServer) bool {
	dominated := true
	for _, key := range transaction.WriteSet {
		if !isKeyVersionDominated(key, transaction, server) {
			dominated = false
		}
	}

	return dominated
}

func isKeyVersionDominated(key string, transaction *pb.TransactionRecord, server *AftServer) bool {
	// We know the key has to be in the index because we always add it when we
	// hear about a new key.
	server.KeyVersionIndexLock.RLock()
	index := server.KeyVersionIndex[key]
	keyList := make([]string, len(*index))

	idx := 0
	for key := range *index {
		keyList[idx] = key
		idx += 1
	}
	server.KeyVersionIndexLock.RUnlock()

	storageKey := server.ConsistencyManager.GetStorageKeyName(key, transaction.Timestamp, transaction.Id)

	for _, other := range keyList {
		// We return true if *any* key dominates this one, so we can short circuit.
		if server.ConsistencyManager.CompareKeys(other, storageKey) {
			return true
		}
	}

	return false
}
