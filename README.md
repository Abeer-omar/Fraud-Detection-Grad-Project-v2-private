# TransactSafe – Streaming Ingestion & S3 Data Lake

## 📌 My Contribution

I was responsible for the **real-time data ingestion layer** of the TransactSafe fraud detection pipeline, from generating/streaming transaction events through **Apache Kafka** to storing the incoming data in **Amazon S3** as the raw data lake layer.

My part establishes the foundation for the downstream fraud detection and analytics components by continuously collecting transaction events and persisting them in a scalable cloud storage environment.

---

## 🏗️ My Part of the Architecture

```text
Transaction Data
      │
      ▼
Data Generator
      │
      ▼
Apache Kafka
      │
      ▼
Spark Structured Streaming
      │
      ▼
Amazon S3
   Raw/Bronze
      │
      ▼
Downstream Processing
(Databricks / Spark / AI Scoring)
```

---

## 🔄 Data Ingestion Flow

### 1. Transaction Data Generation

A transaction data generator produces simulated credit card transaction events continuously.

Each transaction contains information such as:

* Transaction ID
* Customer information
* Credit card information
* Transaction amount
* Transaction timestamp
* Merchant information
* Transaction location
* Other transaction attributes required for fraud detection

The generated transactions are divided into streaming batches and sent to Kafka.

---

### 2. Kafka Streaming

**Apache Kafka** is used as the message broker for the real-time transaction stream.

The producer publishes transaction events to the Kafka topic:

```text
fraud-stream-topic
```

Kafka provides a scalable and fault-tolerant mechanism for receiving transaction events before they are processed by the streaming application.

```text
Producer
   │
   │ Transaction Events
   ▼
Kafka Broker
   │
   └── fraud-stream-topic
```

---

### 3. Spark Structured Streaming

**Apache Spark Structured Streaming** consumes transaction events from Kafka continuously.

The streaming application:

1. Connects to the Kafka broker.
2. Subscribes to the `fraud-stream-topic`.
3. Reads incoming transaction messages.
4. Parses and transforms the incoming data.
5. Converts the data into a structured format.
6. Writes the processed streaming data to Amazon S3.

This allows the pipeline to process transactions continuously rather than waiting for a complete batch.

---

## ☁️ Amazon S3 Data Lake

The processed streaming transactions are stored in **Amazon S3**, which acts as the raw data lake layer of the pipeline.

The data is stored in **Parquet** format to provide efficient storage and querying for downstream processing.

Example structure:

```text
s3://<bucket-name>/
│
└── transactions/
    │
    ├── trans_date=2026-08-20/
    │   └── *.parquet
    │
    ├── trans_date=2026-08-21/
    │   └── *.parquet
    │
    └── trans_date=2026-08-22/
        └── *.parquet
```

### Why Parquet?

Parquet was selected because it:

* Uses columnar storage
* Provides efficient compression
* Reduces storage requirements
* Improves analytical query performance
* Works efficiently with Spark and other big-data tools

### Why Partition the Data?

The data is partitioned by transaction date:

```text
trans_date=YYYY-MM-DD
```

This allows downstream queries to scan only the relevant partitions instead of reading the entire dataset.

---

## 🛠️ Technologies Used

| Technology       | Purpose                                  |
| ---------------- | ---------------------------------------- |
| **Python**       | Data generation and streaming scripts    |
| **Apache Kafka** | Real-time event streaming                |
| **Apache Spark** | Structured Streaming and data processing |
| **AWS S3**       | Cloud data lake storage                  |
| **Parquet**      | Optimized storage format                 |
| **Docker**       | Containerization of Kafka components     |
| **AWS EC2**      | Hosting the ingestion environment        |

---

## 📂 Main Components

### `stream_producer.py`

Responsible for generating and publishing transaction events to Kafka.

```text
Transaction Generator
        │
        ▼
Kafka Producer
        │
        ▼
fraud-stream-topic
```

### Spark Streaming Consumer

The Spark streaming application consumes the Kafka topic and writes the incoming data to S3.

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ▼
Transformation
  │
  ▼
Parquet
  │
  ▼
Amazon S3
```

### `start_pipeline.sh`

A startup script is used to simplify execution of the ingestion pipeline.

It starts the required services and launches the streaming producer and Spark consumer.

---

## 🚀 Running the Ingestion Pipeline

### 1. Start Kafka

Start the Kafka environment using Docker Compose.

```bash
docker compose up -d
```

Verify that the Kafka containers are running:

```bash
docker ps
```

---

### 2. Start the Streaming Pipeline

Run:

```bash
./start_pipeline.sh
```

The script starts the required components and launches the streaming pipeline.

---

### 3. Verify Kafka Messages

Check the Kafka producer logs to verify that transaction events are being published to:

```text
fraud-stream-topic
```

---

### 4. Verify Data in S3

After Spark processes the Kafka events, the resulting Parquet files should appear in the configured S3 bucket.

Example:

```text
s3://<bucket-name>/transactions/
```

---

## 🔐 AWS Configuration

The ingestion application requires access to the AWS environment in order to write data to S3.

The following configuration values should be provided through environment variables or the project's configuration mechanism:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
S3_BUCKET_NAME
```

**Never commit AWS credentials or other secrets to GitHub.**

For production environments, IAM roles should be preferred over hard-coded credentials.

---

## 📊 Result

The ingestion layer provides a continuous pipeline:

```text
Transaction Events
       ↓
    Kafka
       ↓
Spark Structured Streaming
       ↓
    Parquet
       ↓
     AWS S3
       ↓
Databricks / Fraud Detection
       ↓
Snowflake Data Warehouse
       ↓
Power BI / Analytics
```

This layer enables the rest of the TransactSafe system to consume transaction data continuously and provides scalable cloud storage for downstream fraud detection, historical analysis, and reporting.

---

## 👩‍💻 Contribution Summary

**My responsibility:** Streaming Data Ingestion & S3 Data Lake

* Designed and implemented the Kafka-based ingestion flow.
* Developed the transaction streaming producer.
* Configured Kafka for real-time transaction events.
* Implemented Spark Structured Streaming to consume Kafka data.
* Configured streaming output to Amazon S3.
* Used Parquet for efficient data storage.
* Implemented date-based partitioning in S3.
* Containerized Kafka components using Docker.
* Configured the ingestion environment on AWS EC2.
* Validated the end-to-end flow from transaction generation to S3 storage.
