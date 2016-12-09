package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

const randomChars = "abcdefghijklmnopqrstuvwxyz0123456789~#$%^&*@ABCDEFGHIJKLMNOPQRSTUVQXYZ"

var (
	brokerList   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic        = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key          = flag.String("key", "", "The key of the message to produce. Can be empty.")
	distribution = flag.String("distribution", "roundrobin", "The distribution scheme for partition. Can be `hash`, `roundrobin`, or `random`")
	showMetrics  = flag.Bool("metrics", true, "Output metrics on successful publish to stderr")
	bufferSize   = flag.Int("buffer-size", 512, "The buffer size of the message channel.")
	messageSize  = flag.Int("message-size", 400, "The size of the each message in bytes.")
	messageCount = flag.Int("message-count", 4000, "The count of the messages.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	conf := kafka.NewBrokerConf("test-client")
	c, err := kafka.Dial(strings.Split(*brokerList, ","), conf)
	if err != nil {
		printErrorAndExit(69, "cannot connect to kafka cluster: %s", err)
	}

	producerConf := kafka.NewProducerConf()
	producer := c.Producer(producerConf)
	var distProducer kafka.DistributingProducer

	numPartitions, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}
	partitionLength := int32(len(numPartitions))

	switch *distribution {
	case "roundrobin":
		distProducer = kafka.NewRoundRobinProducer(producer, partitionLength)
	case "hash":
		distProducer = kafka.NewHashProducer(producer, partitionLength)
	case "random":
		distProducer = kafka.NewRandomProducer(producer, partitionLength)
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *distribution))
	}

	var (
		messages = make(chan []byte, *bufferSize)
		closing  = make(chan struct{})
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of producer...")
		close(closing)
	}()

	// Produce the messages
	go func() {
		charNumbers := len(randomChars)
		msgBufferSize := *messageSize / 2
		r := rand.New(rand.NewSource(1))
		for count := 0; count < *messageCount; count++ {
			msgBuffer := []byte{}
			for i := 0; i < msgBufferSize; i++ {
				index := r.Intn(charNumbers)
				msgBuffer = append(msgBuffer, randomChars[index])
			}

			messages <- msgBuffer
		}
	}()

	start := time.Now()
	logger.Println("start messages producing")
	message := &proto.Message{}
	for count := 0; count < *messageCount; count++ {
		msgValue := <-messages
		message.Value = msgValue
		offset, err := distProducer.Distribute(*topic, message)
		if err != nil {
			printErrorAndExit(69, "Failed to produce message: %s", err)
		}
		fmt.Printf("topic=%s\toffset=%d\n", *topic, offset)
	}

	end := time.Now()
	logger.Println("finish messages producing")

	logger.Println("Done producing topic", *topic)
	logger.Printf("Produce %d messages\n", *messageCount)

	elapsed := uint64(end.Sub(start).Seconds())
	log.Printf("Elapsed Time: %v\n", elapsed)
	log.Printf("TPS: %v\n", float64(uint64(*messageCount)/elapsed))
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func getPartitions(b *kafka.Broker) ([]int32, error) {
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
