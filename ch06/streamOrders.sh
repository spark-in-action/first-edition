#!/bin/bash
BROKER=$1
if [ -z "$1" ]; then
        BROKER="192.168.10.2:9092"
fi

cat orders.txt | while read line; do
        echo "$line"
        sleep 0.1
done | /usr/local/kafka/bin/kafka-console-producer.sh --broker-list $BROKER --topic orders

