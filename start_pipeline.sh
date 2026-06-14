#!/bin/bash

echo "====================================="
echo "Starting Fraud Detection Pipeline"
echo "====================================="

PROJECT_ROOT=/home/ubuntu/Fraud-Detection-Project

# Activate venv
source $PROJECT_ROOT/venv/bin/activate

echo ""
echo "1. Starting Kafka..."
echo ""

cd $PROJECT_ROOT/docker

./start-kafka.sh

sleep 15

echo ""
echo "2. Starting Spark Consumer..."
echo ""

cd $PROJECT_ROOT/streaming_pipeline

nohup spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4 \
stream_consumer.py \
> $PROJECT_ROOT/logs/consumer.log 2>&1 &

sleep 15

echo ""
echo "3. Starting Kafka Producer..."
echo ""

nohup $PROJECT_ROOT/venv/bin/python \
stream_producer.py \
> $PROJECT_ROOT/logs/producer.log 2>&1 &

echo ""
echo "====================================="
echo "Pipeline Started Successfully"
echo "====================================="


echo ""
echo "tail -f $PROJECT_ROOT/logs/consumer.log"
echo "tail -f $PROJECT_ROOT/logs/producer.log"
echo ""
