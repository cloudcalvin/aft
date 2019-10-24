package consistency

import (
	"sync"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type LWWConsistencyManager struct{}

func (lww *LWWConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

func (lww *LWWConsistencyManager) GetValidKeyVersion(
	key string,
	transaction *pb.TransactionRecord,
	readCache *map[string]pb.KeyValuePair,
	readCacheLock *sync.RWMutex,
	keyVersionIndex *map[string]*[]string,
	keyVersionIndexLock *sync.RWMutex,
) (string, error) {
	return key, nil
}

func (lww *LWWConsistencyManager) GetStorageKeyName(key string, timestamp int64, transactionId string) string {
	return key
}

func (lww *LWWConsistencyManager) CompareKeys(one string, two string) bool {
	return false
}
