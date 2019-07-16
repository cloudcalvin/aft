package main

import (
	"encoding/json"

	pb "github.com/vsreekanti/aft/proto/aft"
)

type Transaction struct {
	id        string
	txnStatus pb.TransactionStatus
	readSet   []string
	writeSet  []string
}

func (t Transaction) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		return err.Error()
	}

	return string(b)
}

func (t Transaction) SetStatus(status pb.TransactionStatus) {
	t.txnStatus = status
}

type KeyUpdate struct {
	key   string
	tid   string
	value []byte
}
