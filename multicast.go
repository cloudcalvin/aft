package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	pb "github.com/vsreekanti/aft/proto/aft"
)

const (
	addressTemplate = "%s%s"
)

func MulticastRoutine(server *aftServer, replicaList []string) {
	// Create a new Ticker that times out 1 second.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	clients := make([]pb.AftClient, len(replicaList))
	for index, ip := range replicaList {
		address := fmt.Sprintf(addressTemplate, ip, port)
		conn, err := grpc.Dial(address)
		if err != nil {
			log.Fatal("Unable to connect to replica %s:\n%v", ip, err)
		}

		clients[index] = pb.NewAftClient(conn)
	}

	// A set to track which transactions we've already gossiped.
	seenTransactions := map[string]bool{}

	for true {
		// Wait for the Ticker channel to trigger.
		_ = <-ticker.C

		// Lock the mutex, serialize the newly committed transactions, and unlock.
		server.TransactionLock.Lock()
		message := pb.TransactionList{}
		for tid := range server.FinishedTransactions {
			if _, ok := seenTransactions[tid]; !ok {
				record := server.FinishedTransactions[tid]
				message.Records = append(message.Records, &record)
			}
		}
		server.TransactionLock.Unlock()

		// Gossip to the other machines in the replica list.
		// TODO: Figure out how to make this asynchronous. There's no need to check
		// for a response here...
		for _, client := range clients {
			client.UpdateMetadata(context.Background(), &message)
		}
	}
}
