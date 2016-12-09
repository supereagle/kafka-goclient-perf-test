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

	"github.com/Shopify/sarama"
)

const randomChars = "abcdefghijklmnopqrstuvwxyz0123456789~#$%^&*@ABCDEFGHIJKLMNOPQRSTUVQXYZ"

var (
	brokerList   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic        = flag.String("topic", "", "REQUIRED: the topic to produce to")
	partitions   = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	key          = flag.String("key", "", "The key of the message to produce. Can be empty.")
	partitioner  = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition    = flag.Int("partition", -1, "The partition to produce to.")
	verbose      = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
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

	if *verbose {
		sarama.Logger = logger
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	var (
		messages = make(chan []byte, *bufferSize)
		closing  = make(chan struct{})
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
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

	producer, err := sarama.NewSyncProducer(strings.Split(*brokerList, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}

	go func(producer sarama.SyncProducer) {
		<-closing
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}(producer)

	message := &sarama.ProducerMessage{Topic: *topic}
	if *key != "" {
		message.Key = sarama.StringEncoder(*key)
	}

	start := time.Now()
	logger.Println("start messages producing")
	for count := 0; count < *messageCount; count++ {
		msgValue := <-messages
		message.Value = sarama.StringEncoder(msgValue)
		producer.SendMessage(message)
		/*if err != nil {
			printErrorAndExit(69, "Failed to produce message: %s", err)
		}
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", *topic, partition, offset)*/
	}

	end := time.Now()
	logger.Println("finish messages producing")
	close(messages)

	logger.Println("Done producing topic", *topic)
	logger.Printf("Produce %d messages\n", *messageCount)

	elapsed := uint64(end.Sub(start).Seconds())
	log.Printf("Elapsed Time: %v\n", elapsed)
	log.Printf("TPS: %v\n", float64(uint64(*messageCount)/elapsed))

	if err := producer.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}

	/*if *showMetrics {
		metrics.WriteOnce(config.MetricRegistry, os.Stderr)
	}*/
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
