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

func MulticastRoutine(server *AftServer, ipAddress string, replicaList []string) {
	// Create a new Ticker that times out 1 second.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	clients := make([]pb.AftClient, len(replicaList)-1)
	index := 0
	for _, ip := range replicaList {
		if ip != ipAddress {
			address := fmt.Sprintf(addressTemplate, ip, port)
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatal(fmt.Sprintf("Unable to connect to replica %s:\n%v", ip, err))
			}

			clients[index] = pb.NewAftClient(conn)
			index += 1
		}
	}

	// A set to track which transactions we've already gossiped.
	seenTransactions := map[string]bool{}

	for true {
		// Wait for the Ticker channel to trigger.
		_ = <-ticker.C

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

		// Gossip to the other machines in the replica list.
		// TODO: Figure out how to make this asynchronous. There's no need to check
		// for a response here...
		for _, client := range clients {
			start := time.Now()
			client.UpdateMetadata(context.Background(), &message)
			end := time.Now()
			fmt.Println(end.Sub(start).Seconds())
		}
	}
}
