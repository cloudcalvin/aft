package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	"github.com/vsreekanti/aft/config"
	"github.com/vsreekanti/aft/consistency"
	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	PushTemplate = "tcp://%s:%d"
	PushPort     = 7777

	PullTemplate = "tcp://*:%d"
	PullPort     = 7778
)

var replicaList = flag.String("replicaList", "", "A comma separated list of addresses of Aft replicas")

func main() {
	flag.Parse()
	if *replicaList == "" {
		fmt.Println("No replicaList provided. Please use the --replicaList flag to pass in a comma-separated list of replicas.")
		os.Exit(1)
	}

	conf := config.ParseConfig("../conf/aft-config.yml")

	var consistencyManager consistency.ConsistencyManager
	switch conf.ConsistencyType {
	case "lww":
		consistencyManager = &consistency.LWWConsistencyManager{}
	case "read-atomic":
		consistencyManager = &consistency.ReadAtomicConsistencyManager{}
	}

	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	replicas := strings.Split(*replicaList, ",")
	txnUpdateSockets := make([]*zmq.Socket, len(replicas))

	for index, replica := range replicas {
		address := fmt.Sprintf(PushTemplate, replica, PushPort)
		fmt.Println("Connecting to ", address)
		socket, err := context.NewSocket(zmq.PUSH)
		if err != nil {
			fmt.Println("Unexpected error while creating new socket:\n", err)
			os.Exit(1)
		}

		socket.Connect(address)
		txnUpdateSockets[index] = socket
	}

	address := fmt.Sprintf(PullTemplate, PullPort)
	txnUpdatePuller, err := context.NewSocket(zmq.PULL)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}
	txnUpdatePuller.Bind(address)

	poller := zmq.NewPoller()
	poller.Add(txnUpdatePuller, zmq.POLLIN)

	allTransactions := map[string]*pb.TransactionRecord{}
	keyVersionIndex := map[string]*[]string{}
	newTransactions := []*pb.TransactionRecord{}

	reportStart := time.Now()
	for true {
		// Wait a 100ms for a new message; we know by default that there is only
		// one socket to poll, so we don't have to check which socket we've
		// received a message on.
		sockets, err := poller.Poll(100 * time.Millisecond)
		if err != nil {
			fmt.Println("Unexpected error returned by poller:\n", err)
			continue
		}

		// This means we received a message -- we don't need to check which socket
		// because there is only one.
		if len(sockets) > 0 {
			fmt.Println("Received new update!")
			bts, _ := txnUpdatePuller.RecvBytes(zmq.DONTWAIT)

			txnList := &pb.TransactionList{}
			err = proto.Unmarshal(bts, txnList)
			if err != nil {
				fmt.Println("Unable to parse received TransactionList:\n", err)
				continue
			}

			for _, record := range txnList.Records {
				newTransactions = append(newTransactions, record)
				allTransactions[record.Id] = record

				for _, key := range record.WriteSet {
					index, ok := keyVersionIndex[key]
					if !ok {
						index = &[]string{}
						keyVersionIndex[key] = index
					}

					result := append(*index, consistencyManager.GetStorageKeyName(key, record.Timestamp, record.Id))
					keyVersionIndex[key] = &result
				}
			}
		}

		reportEnd := time.Now()

		// Broadcast out all new transactions every second. If any transaction has
		// been dominated, drop it from the list.
		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			list := &pb.TransactionList{}

			for _, record := range newTransactions {
				// Only add the new transaction to the send set if it has not been
				// dominated by other transactions.
				if !isTransactionDominated(record, &consistencyManager, &keyVersionIndex) {
					list.Records = append(list.Records, record)
				}
			}

			// Send the new transactions to all the replicas.
			message, err := proto.Marshal(list)
			if err != nil {
				fmt.Println("Unexpected error while marshaling TransactionList protobuf:\n", err)
				continue
			}

			for _, socket := range txnUpdateSockets {
				socket.SendBytes(message, zmq.DONTWAIT)
			}

			reportStart = time.Now()
		}

		// TODO: Garbage collect transactions.
	}
}

// A transaction is dominated if all the keys in its write set have versions
// that are newer than the version from this transaction.
func isTransactionDominated(transaction *pb.TransactionRecord, cm *consistency.ConsistencyManager, keyVersionIndex *map[string]*[]string) bool {
	dominated := true
	for _, key := range transaction.WriteSet {
		storageKey := (*cm).GetStorageKeyName(key, transaction.Timestamp, transaction.Id)
		if !isKeyVersionDominated(key, storageKey, cm, keyVersionIndex) {
			dominated = false
		}
	}

	return dominated
}

func isKeyVersionDominated(key string, storageKey string, cm *consistency.ConsistencyManager, keyVersionIndex *map[string]*[]string) bool {
	// We know the key has to be in the index because we always add it when we
	// hear about a new key.
	index := (*keyVersionIndex)[key]
	for _, other := range *index {
		if (*cm).CompareKeys(other, storageKey) {
			return true
		}
	}

	return false
}
