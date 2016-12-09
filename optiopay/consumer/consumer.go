package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

var (
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "oldest", "The offset to start with. Can be `oldest`, `newest`")
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

	totalCountChan := make(chan uint64)

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = kafka.StartOffsetOldest
	case "newest":
		initialOffset = kafka.StartOffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	conf := kafka.NewBrokerConf("test-client")
	conf.AllowTopicCreation = true
	c, err := kafka.Dial(strings.Split(*brokerList, ","), conf)
	if err != nil {
		printErrorAndExit(69, "cannot connect to kafka cluster: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *proto.Message, *bufferSize)
		closing  = make(chan struct{})
		closed   = false
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
		conf := kafka.NewConsumerConf(*topic, partition)
		conf.StartOffset = initialOffset
		pc, err := c.Consumer(conf)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc kafka.Consumer) {
			for {
				message, _ := pc.Consume()
				// Sometimes there is panic error when messages chain is closed at the end.
				messages <- message
			}
		}(pc)
	}

	go func(partitionLength int) {
		time.Sleep(time.Duration(*duration) * time.Second)
		close(closing)
	}(len(partitionList))

	go func(c *kafka.Broker) {
		<-closing
		logger.Println("Close the brocker")
		closed = true
		c.Close()
	}(c)

	go func() {
		var count uint64 = 0
		for range messages {
			count++
		}
		totalCountChan <- count
	}()

	logger.Println("before wait")
	<-closing
	end := time.Now()
	logger.Println("after wait")
	close(messages)

	var consumed uint64 = 0
	consumed += <-totalCountChan

	logger.Println("Done consuming topic", *topic)
	logger.Printf("Read %d messages\n", consumed)

	elapsed := uint64(end.Sub(start).Seconds())
	log.Printf("Consumed: %v\n", consumed)
	log.Printf("Elapsed Time: %v\n", elapsed)
	log.Printf("TPS: %v\n", float64(consumed/elapsed))
}

func getPartitions(b *kafka.Broker) ([]int32, error) {
	if *partitions == "all" {
		md, err := b.Metadata()
		if err != nil {
			return nil, err
		}
		for _, mt := range md.Topics {
			if mt.Name == *topic {
				partitionIds := []int32{}
				for _, partition := range mt.Partitions {
					partitionIds = append(partitionIds, partition.ID)
				}
				return partitionIds, nil
			}
		}
		return nil, fmt.Errorf("Fail to get partitions for topic %s", *topic)
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
