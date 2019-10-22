package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/montanaflynn/stats"
	"google.golang.org/grpc"

	pb "github.com/vsreekanti/aft/proto/aft"
)

var numRequests = flag.Int("numRequests", 1000, "The total number of requests in the benchmark")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")
var numKeys = flag.Int64("numKeys", 1000, "The number of keys to operate over")

func main() {
	flag.Parse()

	requestsPerThread := int64(*numRequests / *numThreads)
	fmt.Printf("Starting benchmark with %d requests across %d threads...\n", *numRequests, *numThreads)

	latencyChannel := make(chan []float64)
	errorChannel := make(chan []string)
	totalTimeChannel := make(chan float64)

	for tid := 0; tid < *numThreads; tid++ {
		go benchmark(tid, requestsPerThread, latencyChannel, errorChannel, totalTimeChannel)
	}

	latencies := []float64{}
	errors := []string{}
	thruputs := []float64{}

	for tid := 0; tid < *numThreads; tid++ {
		latencyArray := <-latencyChannel
		latencies = append(latencies, latencyArray...)

		errorArray := <-errorChannel
		errors = append(errors, errorArray...)

		threadTime := <-totalTimeChannel
		threadThruput := float64(requestsPerThread) / threadTime
		thruputs = append(thruputs, threadThruput)
	}

	median, _ := stats.Median(latencies)
	fifth, _ := stats.Percentile(latencies, 5.0)
	nfifth, _ := stats.Percentile(latencies, 95.0)
	first, _ := stats.Percentile(latencies, 1.0)
	nninth, _ := stats.Percentile(latencies, 99.0)
	totalThruput, _ := stats.Sum(thruputs)

	if len(errors) > 0 {
		fmt.Printf("Errors: %v\n", errors)
	}

	fmt.Printf("Number of errors: %d\n", len(errors))
	fmt.Printf("Median latency: %.6f\n", median)
	fmt.Printf("5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
	fmt.Printf("Total throughput: %f\n", totalThruput)
}

func benchmark(
	tid int,
	threadRequestCount int64,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	pyld := map[string]int{"count": 1}
	payload, _ := json.Marshal(pyld)
	input := &lambda.InvokeInput{
		FunctionName:   aws.String("aft-test"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()
	requestId := int64(0)
	for ; requestId < threadRequestCount; requestId++ {
		requestStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		requestEnd := time.Now()


		// Log the elapsed request time.
		latencies = append(latencies, requestEnd.Sub(requestStart).Seconds())

		// First, we check if the request itself returned an error. This should be
		// very unlikely.
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := response.Payload

			runtimes := []float64{}
			err = json.Unmarshal(bts, &runtimes)
		}
	}

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}

func benchmarkPlain(
	tid int,
	threadRequestCount int64,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}
	rrInconsistencies := 0
	wrInconsistencies := 0

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	pyld := map[string]int{"count": 1}
	payload, _ := json.Marshal(pyld)
	input := &lambda.InvokeInput{
		FunctionName:   aws.String("ddb-txn"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()
	requestId := int64(0)
	for ; requestId < threadRequestCount; requestId++ {
		requestStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		requestEnd := time.Now()

		// Log the elapsed request time.
		latencies = append(latencies, float64(requestEnd.Sub(requestStart).Seconds()))

		// First, we check if the request itself returned an error. This should be
		// very unlikely.
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := response.Payload
			inconsistencies := []int{}
			err = json.Unmarshal(bts, &inconsistencies)

			if len(inconsistencies) < 2 {
				fmt.Println(string(bts))
				fmt.Println(err)
			} else {
				wrInconsistencies += inconsistencies[0]
				rrInconsistencies += inconsistencies[1]
			}
		}
	}

	fmt.Println("Write-read inconsistencies: ", wrInconsistencies)
	fmt.Println("Read-read inconsistencies: ", rrInconsistencies)

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}

func benchmarkLocal(
	tid int,
	threadRequestCount int64,
	latencyChannel chan []float64,
	errorChannel chan []string,
	totalTimeChannel chan float64,
) {
	errors := []string{}
	latencies := []float64{}

	conn, err := grpc.Dial("18.232.50.191:7654", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Generate a new random source and create a Zipfian distribution.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, 1.1, 1.0, 999)

	writeData := make([]byte, 4096)
	r.Read(writeData)
	client := pb.NewAftClient(conn)

	requestId := int64(0)
	benchStart := time.Now()
	errored := false

	for ; requestId < threadRequestCount; requestId++ {
		var err error
		keyCount := 6 // Set the number of keys accessed in this txn.
		writeCount := 2

		requestStart := time.Now()
		tag, _ := client.StartTransaction(context.Background(), &empty.Empty{})

		for keyId := 0; keyId < keyCount; keyId++ {
			key := strconv.FormatUint(zipf.Uint64(), 10)

			update := &pb.KeyRequest{Tid: tag.Id}
			pair := &pb.KeyRequest_KeyPair{Key: key}

			update.Pairs = append(update.Pairs, pair)

			// Do the two writes first, then do the 4 reads.
			if keyId < writeCount {
				pair.Value = writeData
				_, err = client.Write(context.Background(), update)

				if err != nil {
					errored = true
					break
				}
			} else {
				_, err = client.Read(context.Background(), update)

				if err != nil {
					errored = true
					break
				}
			}
		}

		if !errored {
			tag, err = client.CommitTransaction(context.Background(), tag)
		}
		requestEnd := time.Now()

		if err != nil {
			errors = append(errors, err.Error())
		} else {
			latencies = append(latencies, float64(requestEnd.Sub(requestStart).Seconds()))
		}
	}

	benchEnd := time.Now()
	totalTime := benchEnd.Sub(benchStart).Seconds()

	latencyChannel <- latencies
	errorChannel <- errors
	totalTimeChannel <- totalTime
}
