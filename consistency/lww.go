package consistency

type LWWConsistencyManager struct{}

func (lww *LWWConsistencyManager) ValidateTransaction(tid string, readSet []string, writeSet []string) bool {
	return true
}

func (lww *LWWConsistencyManager) GetValidKeyVersion(key string, tid string) string {
	return key
}
