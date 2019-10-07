package consistency

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type ReadAtomicConsistencyManager struct{}

const (
	keyTemplate = "/data/%s/%d/%s"
	// keyPrefix   = "/data/%s"
)

func (racm *ReadAtomicConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

func (racm *ReadAtomicConsistencyManager) GetValidKeyVersion(
	key string,
	transaction *pb.TransactionRecord,
	readCache *map[string]pb.KeyValuePair,
	readCacheLock *sync.RWMutex,
	keyVersionIndex *map[string]*[]string,
	keyVersionIndexLock *sync.RWMutex,
) (string, error) {

	if val, ok := transaction.ReadSet[key]; ok {
		return val, nil
	}

	// Check if the key version is constrained by any of the keys we've already
	// read.
	constraintSet := []string{}
	for read := range transaction.ReadSet {
		// We ignore the boolean because we are guaranteed to have the key in the
		// readCache.
		fullName, _ := transaction.ReadSet[read]
		readCacheLock.RLock()
		kvPair, _ := (*readCache)[fullName]
		readCacheLock.RUnlock()

		for _, cowritten := range kvPair.CowrittenKeys {
			if key == cowritten {
				constraintSet = append(constraintSet, racm.GetStorageKeyName(key, kvPair.Timestamp, kvPair.Tid))
			}
		}
	}

	// Pick the latest timestamp in the constraint set if there are any keys in
	// it.
	if len(constraintSet) > 0 {
		latest := constraintSet[0]

		for _, constraint := range constraintSet {
			if compareKeys(constraint, latest) {
				latest = constraint
			}
		}

		return latest, nil
	}

	// Retrieve a version of the key that is no newer than the transaction than
	// the oldest transaction we have read. If there's no such version, then look
	// at newer versions---if anything conflicts, we abort.

	// Retrieve all of the versions available for this key.
	keyVersionIndexLock.RLock()
	keyVersions, ok := (*keyVersionIndex)[key]
	keyVersionIndexLock.RUnlock()

	if !ok || len(*keyVersions) == 0 {
		return "", errors.New(fmt.Sprintf("There are no versions of key %s.", key))
	}

	// The current implementation is conservative. It only returns versions that
	// are older than things we've already read.
	var latest string
	latest = ""
	for _, keyVersion := range *keyVersions {
		validVersion := true

		// Check to see if this keyVersion is older than all of the keys that we
		// have already read.
		for read := range transaction.ReadSet {
			readVersion := racm.GetStorageKeyName(read, transaction.Timestamp, transaction.Id)
			if !compareKeys(readVersion, keyVersion) {
				validVersion = false
				break
			}
		}

		// If the version is valid and is newer than we have already seen as the
		// latest, we update the latest version.
		if validVersion && (len(latest) == 0 || compareKeys(keyVersion, latest)) {
			latest = keyVersion
		}
	}

	if len(latest) == 0 {
		return "", errors.New(fmt.Sprintf("There are no valid versions of key %s.", key))
	}

	return latest, nil
}

func (racm *ReadAtomicConsistencyManager) GetStorageKeyName(key string, timestamp int64, transactionId string) string {
	return fmt.Sprintf(keyTemplate, key, timestamp, transactionId)
}

// This function takes in two keys that are expected to conform to the string
// format defined in keyTemplate above. It returns true if the key passed in as
// argument `one` is newer than the key passed in as argument `two`. It returns
// false otherwise.
func compareKeys(one string, two string) bool {
	if one[0] == '/' {
		rn := []rune(one)
		one = string(rn[1:len(rn)])
	}

	if two[0] == '/' {
		rn := []rune(two)
		two = string(rn[1:len(rn)])
	}

	oneTs, oneTid := splitKey(one)
	twoTs, twoTid := splitKey(two)

	return oneTs > twoTs || (oneTs == twoTs && oneTid > twoTid)
}

// This function takes in a single key that conforms to the keyTemplate defined
// above, and it returns the timestamp and UUID of the transaction that wrote
// it.
func splitKey(key string) (int64, string) {
	splits := strings.Split(key, "/")

	// We know err won't be nil unless someone is interfering with the system.
	txnTs, _ := strconv.ParseInt(splits[2], 10, 64)
	tid := splits[1]

	return txnTs, tid
}
