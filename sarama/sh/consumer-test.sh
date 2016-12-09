#!/bin/bash

MSG_SIZES="100 200 400 800 1000"
TIMES=5

for size in $MSG_SIZES
do
  echo "consume messages with size $size"
  for ((i=1; i<=$TIMES; i++))
  do
    ./consumer -brokers broker1.kafka.com:9092,broker2.kafka.com:9092,broker3.kafka.com:9092 -topic size-test-$size --duration 3
  done
done
