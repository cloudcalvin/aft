package storage

type StorageManager interface {
	// Start a new transaction with the execution ID passed in; this ID will be
	// used for all operations relevant to this particular transaction.
	StartTransaction(id string) bool

	// Commit all of the changes made in this transaction to the storage engine.
	CommitTransaction(id string) bool

	// Abort all of the changes made in this transaction, so none of them will be
	// persisted in the storage engine.
	AbortTransaction(id string) bool

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, val []byte, tid string) bool

	// Retrieve the given key as a part of the transaction tid.
	Get(key string, tid string) ([]byte, error)
}
