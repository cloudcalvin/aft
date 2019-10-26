package consistency

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type ReadAtomicConsistencyManager struct{}

const (
	keyTemplate = "data/%s/%d/%s"
)

func (racm *ReadAtomicConsistencyManager) ValidateTransaction(tid string, readSet map[string]string, writeSet []string) bool {
	return true
}

func (racm *ReadAtomicConsistencyManager) GetValidKeyVersion(
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
) (string, error) {

	if val, ok := transaction.ReadSet[key]; ok {
		return val, nil
	}

	latestVersionIndexLock.RLock()
	latestVersion, ok := (*latestVersionIndex)[key]
	latestVersionIndexLock.RUnlock()

	if ok {
		_, latestTxnId := splitKey(latestVersion)
		finishedTransactionsLock.RLock()
		latestTxn := (*finishedTransactions)[latestTxnId]
		finishedTransactionsLock.RUnlock()
		isLatestValid := true

		// Check to see if the latest version of the key we know about is valid or
		// not.
		for _, latestCowritten := range latestTxn.WriteSet {
			for coread := range transaction.ReadSet {
				if latestCowritten == coread {
					coreadVersion := transaction.ReadSet[coread]
					cowrittenVersion := racm.GetStorageKeyName(latestCowritten, latestTxn.Timestamp, latestTxn.Id)

					if racm.CompareKeys(cowrittenVersion, coreadVersion) { // If the latest version contains a write we should've read we ignore it.
						isLatestValid = false
						break
					}
				}
			}

			if !isLatestValid {
				break
			}
		}

		if isLatestValid {
			racm.UpdateTransactionDependencies(latestVersion, false, transactionDependencies, transactionDependenciesLock)
			return latestVersion, nil
		}
	}

	// Check if the key version is constrained by any of the keys we've already
	// read.
	constraintSet := []string{}
	for read := range transaction.ReadSet {
		// We ignore the boolean because we are guaranteed to have the key in the
		// readCache.
		fullName, _ := transaction.ReadSet[read]
		_, tid := splitKey(fullName)
		finishedTransactionsLock.RLock()
		txn, _ := (*finishedTransactions)[tid]
		finishedTransactionsLock.RUnlock()

		for _, cowritten := range txn.WriteSet {
			if key == cowritten {
				constraintSet = append(constraintSet, racm.GetStorageKeyName(key, txn.Timestamp, txn.Id))
			}
		}
	}

	// Pick the latest timestamp in the constraint set if there are any keys in
	// it.
	if len(constraintSet) > 0 {
		latest := constraintSet[0]

		for _, constraint := range constraintSet {
			if racm.CompareKeys(constraint, latest) {
				latest = constraint
			}
		}

		racm.UpdateTransactionDependencies(latest, false, transactionDependencies, transactionDependenciesLock)
		return latest, nil
	}

	// Retrieve a version of the key that is no newer than the transaction than
	// the oldest transaction we have read. If there's no such version, then look
	// at newer versions---if anything conflicts, we abort.

	// Retrieve all of the versions available for this key.
	keyVersionIndexLock.RLock()
	kvList, ok := (*keyVersionIndex)[key]

	if !ok || len(*kvList) == 0 {
		return "", errors.New(fmt.Sprintf("There are no versions of key %s.", key))
	}

	keyVersions := map[string]bool{}
	for kv, _ := range *kvList {
		keyVersions[kv] = (*kvList)[kv]
	}
	keyVersionIndexLock.RUnlock()

	// The current implementation is conservative. It only returns versions that
	// are older than things we've already read.
	latest := ""
	isCached := false

	for keyVersion := range keyVersions {
		validVersion := true
		_, keyTxnId := splitKey(keyVersion)

		finishedTransactionsLock.RLock()
		keyTxn := (*finishedTransactions)[keyTxnId]
		finishedTransactionsLock.RUnlock()

		// Check to see if this keyVersion is older than all of the keys that we
		// have already read.
		for read := range transaction.ReadSet {
			readVersion := racm.GetStorageKeyName(read, transaction.Timestamp, transaction.Id)
			if !racm.CompareKeys(readVersion, keyVersion) {
				for _, cowritten := range keyTxn.WriteSet {
					if cowritten == readVersion {
						validVersion = false
					}
				}

				if !validVersion {
					break
				}
			}
		}

		// If the version is valid and is newer than we have already seen as the
		// latest, we update the latest version; we choose to reject it if we have
		// an older version that is cached locally.
		if validVersion && (len(latest) == 0 || racm.CompareKeys(keyVersion, latest)) {
			if keyVersions[keyVersion] || !isCached {
				latest = keyVersion
				isCached = keyVersions[keyVersion]
			}
		}

		// If we've found a cached version less than 2 seconds old, just return it.
		latestTs, _ := splitKey(latest)
		threshold := (4 * time.Second).Nanoseconds()
		if isCached && (time.Now().UnixNano()-latestTs) < threshold {
			break
		}
	}

	if len(latest) == 0 {
		return "", errors.New(fmt.Sprintf("There are no valid versions of key %s.", key))
	}

	racm.UpdateTransactionDependencies(latest, false, transactionDependencies, transactionDependenciesLock)
	return latest, nil
}

func (racm *ReadAtomicConsistencyManager) UpdateTransactionDependencies(
	keyVersion string,
	finished bool,
	transactionDependencies *map[string]int,
	transactionDependenciesLock *sync.RWMutex,
) {
	_, tid := splitKey(keyVersion)

	transactionDependenciesLock.Lock()
	if !finished {
		if _, ok := (*transactionDependencies)[tid]; !ok {
			(*transactionDependencies)[tid] = 0
		}

		(*transactionDependencies)[tid] += 1
	} else {
		// We don't need to check if the key exists because we're guaranteed to have created this data.
		(*transactionDependencies)[tid] -= 1
	}

	transactionDependenciesLock.Unlock()
}

func (racm *ReadAtomicConsistencyManager) GetStorageKeyName(key string, timestamp int64, transactionId string) string {
	return fmt.Sprintf(keyTemplate, key, timestamp, transactionId)
}

// This function takes in two keys that are expected to conform to the string
// format defined in keyTemplate above. It returns true if the key passed in as
// argument `one` is newer than the key passed in as argument `two`. It returns
// false otherwise.
func (racm *ReadAtomicConsistencyManager) CompareKeys(one string, two string) bool {
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
	tid := splits[3]

	return txnTs, tid
}
