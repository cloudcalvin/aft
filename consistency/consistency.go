package consistency

import (
	"sync"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type ConsistencyManager interface {
	// Based on the set of keys read and written by this transaction check whether or not it should be allowed to commit.
	// The decision about whether or not the transaction should commit is based on the consistency and isolation levels
	// the consistency manage wants to support.
	ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool

	// Return the valid versions of a key that can be read by a particular transaction. Again, this should be determined
	// by the isolation and consistency modes supported. The inputs are the requesting transactions TID and the
	// non-transformed key requested. The output is a list of actual, potentially versioned, keys stored in the underlying
	// storage system.
	GetValidKeyVersion(
		key string,
		transaction *pb.TransactionRecord,
		finishedTransactions *map[string]*pb.TransactionRecord,
		finishedTransactionsLock *sync.RWMutex,
		keyVersionIndex *map[string]*map[string]bool,
		keyVersionIndexLock *sync.RWMutex,
		transactionDependencies *map[string]int,
		transactionDependenciesLock *sync.RWMutex,
		latestVersionIndex *map[string]string,
		latestVersionIndexLock *sync.RWMutex,
	) (string, error)

	// TODO: Add description of this function.
	GetStorageKeyName(key string, timestamp int64, transactionId string) string

	// TODO: Add description of this function.
	CompareKeys(one string, two string) bool

	// TODO: Add description of this function.
	UpdateTransactionDependencies(
		keyVersion string,
		finished bool,
		transactionDependencies *map[string]int,
		transactionDependenciesLock *sync.RWMutex,
	)
}
