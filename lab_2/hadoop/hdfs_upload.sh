#!/bin/bash

docker exec -it namenode bash -c "
hdfs dfs -mkdir -p /data &&
hdfs dfs -put -f /data/stackoverflow_100k.csv /data/
"