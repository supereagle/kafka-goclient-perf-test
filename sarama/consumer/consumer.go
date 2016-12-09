package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "oldest", "The offset to start with. Can be `oldest`, `newest`")
	verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize = flag.Int("buffer-size", 512, "The buffer size of the message channel.")
	duration   = flag.Int("duration", 120, "The duration of the test")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}

	totalCountChan := make(chan uint64)

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(strings.Split(*brokerList, ","), nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	start := time.Now()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				//log.Printf("consumer %d, message %d: %s", message.Partition, message.Offset, message.Value)
				messages <- message
			}
		}(pc)
	}

	go func(partitionLength int) {
		time.Sleep(time.Duration(*duration) * time.Second)
		close(closing)
	}(len(partitionList))

	go func() {
		var count uint64 = 0
		for range messages {
			count++
		}
		totalCountChan <- count
	}()

	logger.Println("before wg.Wait")
	wg.Wait()
	end := time.Now()
	logger.Println("after wg.Wait")
	close(messages)

	var consumed uint64 = 0
	consumed += <-totalCountChan

	logger.Println("Done consuming topic", *topic)
	logger.Printf("Read %d messages\n", consumed)

	elapsed := uint64(end.Sub(start).Seconds())
	log.Printf("Consumed: %v\n", consumed)
	log.Printf("Elapsed Time: %v\n", elapsed)
	log.Printf("TPS: %v\n", float64(consumed/elapsed))

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
