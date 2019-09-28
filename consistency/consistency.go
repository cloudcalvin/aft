package consistency

import (
	pb "github.com/vsreekanti/aft/proto/aft"
	"github.com/vsreekanti/aft/storage"
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
		transaction pb.TransactionRecord,
		readSet map[string]string,
		storageManager storage.StorageManager,
		readCache map[string]pb.KeyValuePair) (string, error)

	// TODO: Add description of this function.
	GetStorageKeyName(key string, transaction *pb.TransactionRecord) string
}
