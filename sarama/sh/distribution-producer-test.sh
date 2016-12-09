#!/bin/bash

MSG_SIZE=400
TIMES=5
DISTRIBUTIONS="roundrobin random" # Not support hash as it equals to random when the message key is empty.

for distr in $DISTRIBUTIONS
do
  echo "produce messages with distribution $distr"
  for ((i=1; i<=$TIMES; i++))
  do
    ./producer -brokers broker1.kafka.com:9092,broker2.kafka.com:9092,broker3.kafka.com:9092 -distribution $distr -topic distribution-test-$distr -message-size $MSG_SIZE -message-count 50000
  done
done
