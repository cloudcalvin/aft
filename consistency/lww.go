package consistency

import (
	pb "github.com/vsreekanti/aft/proto/aft"
	"github.com/vsreekanti/aft/storage"
)

type LWWConsistencyManager struct{}

func (lww *LWWConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

func (lww *LWWConsistencyManager) GetValidKeyVersion(
	key string,
	transaction pb.TransactionRecord,
	readSet map[string]string,
	storageManager storage.StorageManager,
	readCache map[string]pb.KeyValuePair) (string, error) {

	return key, nil
}

func (lww *LWWConsistencyManager) GetStorageKeyName(key string, transaction *pb.TransactionRecord) string {
	return key
}
