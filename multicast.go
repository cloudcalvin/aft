package main

import (
	ctx "context"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"

	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	PullTemplate = "tcp://*:%d"
	PullPort     = 7777

	PushTemplate = "tcp://%s:%d"
	PushPort     = 7778
)

func MulticastRoutine(server *AftServer, ipAddress string, managerAddress string) {
	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	updatePuller, err := context.NewSocket(zmq.PULL)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}
	err = updatePuller.Bind(fmt.Sprintf(PullTemplate, PullPort))
	if err != nil {
		fmt.Println("Unexpected error while binding socket:\n", err)
		os.Exit(1)
	}

	updatePusher, err := context.NewSocket(zmq.PUSH)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}
	err = updatePusher.Connect(fmt.Sprintf(PushTemplate, managerAddress, PushPort))
	if err != nil {
		fmt.Println("Unexpected error while connecting socket:\n", err)
		os.Exit(1)
	}

	// Create a new poller to wait for new updates.
	poller := zmq.NewPoller()
	poller.Add(updatePuller, zmq.POLLIN)

	// A set to track which transactions we've already gossiped.
	seenTransactions := map[string]bool{}

	reportStart := time.Now()
	for true {
		// Wait a 100ms for a new message; we know by default that there is only
		// one socket to poll, so we don't have to check which socket we've
		// received a message on.
		sockets, err := poller.Poll(10 * time.Millisecond)

		if err != nil {
			fmt.Println("Unexpected error returned by poller:\n", err)
			continue
		}

		// This means we received a message -- we don't need to check which socket
		// because there is only one.
		if len(sockets) > 0 {
			bts, _ := updatePuller.RecvBytes(zmq.DONTWAIT)

			newTransactions := &pb.TransactionList{}
			err = proto.Unmarshal(bts, newTransactions)
			fmt.Printf("Received %d new transactions.\n", len(newTransactions.Records))
			if err != nil {
				fmt.Println("Unexpected error while deserializing Protobufs:\n", err)
				continue
			}

			// Filter out any transactions we have already heard about.
			unseenTransactions := &pb.TransactionList{}
			for _, record := range newTransactions.Records {
				if _, ok := seenTransactions[record.Id]; !ok {
					unseenTransactions.Records = append(unseenTransactions.Records, record)
					seenTransactions[record.Id] = true
				}
			}

			server.UpdateMetadata(ctx.TODO(), unseenTransactions)
		}

		reportEnd := time.Now()
		if reportEnd.Sub(reportStart).Seconds() > 1.0 {
			fmt.Printf("I currently know about %d transations.\n", len(seenTransactions))
			// Lock the mutex, serialize the newly committed transactions, and unlock.
			server.FinishedTransactionLock.RLock()
			message := pb.TransactionList{}

			for tid := range server.FinishedTransactions {
				if _, ok := seenTransactions[tid]; !ok {
					record := server.FinishedTransactions[tid]
					message.Records = append(message.Records, record)
					seenTransactions[tid] = true
				}
			}
			server.FinishedTransactionLock.RUnlock()

			if len(message.Records) > 0 {
				bts, err := proto.Marshal(&message)
				if err != nil {
					fmt.Println("Unexpected error while marshaling Protobufs:\n", err)
					continue
				}

				fmt.Printf("Sending %d records.\n", len(message.Records))
				updatePusher.SendBytes(bts, zmq.DONTWAIT)
			}

			reportStart = time.Now()
		}
	}
}
