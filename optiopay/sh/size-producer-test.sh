#!/bin/bash

MSG_SIZES="100 200 400 800 1000"
TIMES=4
DISTRIBUTION=roundrobin

for size in $MSG_SIZES
do
  echo "produce messages with size $size"
  for ((i=1; i<=$TIMES; i++))
  do
    ./producer -brokers broker1.kafka.com:9092,broker2.kafka.com:9092,broker3.kafka.com:9092 -distribution $DISTRIBUTION -topic size-test-$size -message-size $size -message-count 50000
  done
done
