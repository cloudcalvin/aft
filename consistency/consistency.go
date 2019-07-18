package consistency

type ConsistencyManager interface {
	// Based on the set of keys read and written by this transaction check whether or not it should be allowed to commit.
	// The decision about whether or not the transaction should commit is based on the consistency and isolation levels
	// the consistency manage wants to support.
	ValidateTransaction(tid string, readSet []string, writeSet []string) bool

	// Return the valid versions of a key that can be read by a particular transaction. Again, this should be determined
	// by the isolation and consistency modes supported. The inputs are the requesting transactions TID and the
	// non-transformed key requested. The output is a list of actual, potentially versioned, keys stored in the underlying
	// storage system.
	// TODO: should this be a list of keys, or just a single key?
	GetValidKeyVersion(key string, tid string) string
}
