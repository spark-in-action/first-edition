#!/bin/bash

if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi

split -l 10000 --additional-suffix=.ordtmp orders.txt orders

for f in `ls *.ordtmp`; do
        if [ "$2" == "local" ]; then
                mv $f $1
        else
                hdfs dfs -copyFromLocal $f $1
                rm -f $f
        fi
        sleep 3
done
