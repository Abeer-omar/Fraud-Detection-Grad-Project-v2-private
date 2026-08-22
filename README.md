## 1- Data Streaming & Ingestion

The **Data Streaming & Ingestion** layer is the first stage of the **TransactSafe** fraud detection pipeline. It was implemented on **Amazon Web Services (AWS)** and is responsible for ingesting both **historical transaction data** and **real-time transaction streams**, processing the incoming events, and storing them in **Amazon S3** as the foundation for the downstream Machine Learning and fraud detection pipeline.

The ingestion architecture combines **AWS EC2, Docker, Apache Kafka, Python, Spark Structured Streaming, and Amazon S3** to provide a scalable and continuous data ingestion layer.

---

### 1. AWS-Based Ingestion Architecture

The complete ingestion environment was deployed on **AWS**, with an **EC2 instance** used to host the streaming infrastructure.

The architecture can be divided into two ingestion modes:

* **Historical Data Mode** – used to load existing transaction records into the data lake.
* **Real-Time Streaming Mode** – used to continuously generate and stream new transaction events through Kafka and Spark into S3.

```text
                         AWS CLOUD
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│                         Amazon EC2                                      │
│                  ┌─────────────────────┐                                 │
│                  │   Docker Environment│                                 │
│                  │                     │                                 │
│                  │  Kafka + Producer   │                                 │
│                  └──────────┬──────────┘                                 │
│                             │                                            │
│                             ▼                                            │
│                     Apache Kafka                                         │
│                  fraud-stream-topic                                      │
│                             │                                            │
│                             ▼                                            │
│              Spark Structured Streaming                                  │
│                             │                                            │
│                             ▼                                            │
│                       Amazon S3                                          │
│                       Data Lake                                          │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

<p align="center">
  <img src="pics/aws_architecture.png" alt="AWS Data Ingestion Architecture" width="900"/>
</p>

---

### 2. Historical Data Ingestion

The first ingestion mode handles the **historical transaction dataset**.

Historical transaction records are uploaded to the AWS environment and stored in **Amazon S3**. This provides the historical data required by the downstream pipeline for model development, feature engineering, analysis, and retraining.

```text
Historical Transaction Dataset
              │
              ▼
        AWS / Amazon S3
              │
              ▼
      Raw Historical Data
              │
              ▼
      Databricks / Spark
              │
              ▼
       Feature Engineering
              │
              ▼
        ML / Retraining
```

<p align="center">
  <img src="pics/historical_data.png" alt="Historical Data Ingestion" width="800"/>
</p>

The historical data provides the foundation for:

* Exploratory Data Analysis (EDA)
* Data quality analysis
* Feature engineering
* Model training
* Model evaluation
* Historical fraud analysis
* Periodic model retraining

---

### 3. Real-Time Streaming Ingestion

The second mode is the **real-time streaming pipeline**.

A Python-based transaction producer continuously generates transaction events and publishes them to **Apache Kafka**.

The Kafka topic used for the transaction stream is:

```text
fraud-stream-topic
```

The events are then consumed by **Spark Structured Streaming**, processed, and written to Amazon S3.

```text
┌───────────────────┐
│ Transaction       │
│ Data Generator    │
└─────────┬─────────┘
          │
          │ Real-Time Events
          ▼
┌───────────────────┐
│ Apache Kafka      │
│                   │
│ fraud-stream-topic│
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Spark Structured  │
│ Streaming         │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│ Amazon S3         │
│ Raw Data Lake     │
└─────────┬─────────┘
          │
          ▼
   Databricks ML Pipeline
```

<p align="center">
  <img src="pics/streaming_pipeline.png" alt="Real-Time Streaming Pipeline" width="900"/>
</p>

---

### 4. Amazon EC2 Infrastructure

The real-time ingestion environment was deployed on **AWS EC2**.

The EC2 instance provided the compute environment for running the Kafka-based streaming infrastructure and the ingestion services.

The environment was configured to support:

* Kafka broker
* Kafka producer
* Spark streaming consumer
* Docker containers
* AWS S3 connectivity

```text
AWS EC2
│
├── Docker
│   └── Kafka
│
├── Transaction Producer
│
└── Spark Streaming Consumer
```

<p align="center">
  <img src="pics/ec2_instance.png" alt="AWS EC2 Ingestion Server" width="800"/>
</p>

---

### 5. Dockerized Kafka Environment

Kafka was deployed using **Docker**, making the streaming environment easier to configure, start, stop, and reproduce.

The Kafka services were managed using **Docker Compose**.

```text
Docker Compose
      │
      ▼
┌─────────────────────┐
│ Kafka Container     │
│                     │
│ Broker               │
│ Topic                │
│ Networking           │
└─────────────────────┘
```

<p align="center">
  <img src="pics/docker_kafka.png" alt="Docker Kafka Environment" width="800"/>
</p>

The Dockerized setup allowed the Kafka infrastructure to run consistently within the AWS EC2 environment.

---

### 6. Kafka Producer

A Python-based producer was implemented to continuously publish transaction events to Kafka.

The producer sends transactions to:

```text
fraud-stream-topic
```

The streaming process can be summarized as:

```text
Transaction Generator
        │
        ▼
 Python Kafka Producer
        │
        ▼
    Kafka Broker
        │
        ▼
fraud-stream-topic
```

<p align="center">
  <img src="pics/kafka_producer.png" alt="Kafka Producer Streaming Transactions" width="800"/>
</p>

The producer was designed to simulate a continuous stream of credit card transactions rather than sending the entire dataset at once.

---

### 7. Spark Structured Streaming

**Spark Structured Streaming** acts as the bridge between Kafka and the S3 data lake.

The Spark application continuously consumes Kafka messages and processes them as streaming micro-batches.

The main processing flow is:

```text
Kafka Messages
      │
      ▼
Read Stream
      │
      ▼
Parse Transaction Data
      │
      ▼
Apply Schema
      │
      ▼
Transform / Clean
      │
      ▼
Write Stream
      │
      ▼
Parquet Files
      │
      ▼
Amazon S3
```

<p align="center">
  <img src="pics/spark_streaming.png" alt="Spark Structured Streaming" width="850"/>
</p>

Spark Structured Streaming enables the pipeline to process transactions continuously as new events arrive.

---

### 8. Writing Streaming Data to Amazon S3

After processing, Spark writes the streaming transactions to **Amazon S3**.

The data is stored in **Parquet** format.

Example S3 structure:

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

<p align="center">
  <img src="pics/s3_bucket.png" alt="Amazon S3 Transaction Data" width="850"/>
</p>

---

### 9. Parquet Format & Partitioning

The streaming output is stored using **Parquet**, a columnar storage format optimized for analytical workloads.

The data is partitioned by transaction date:

```text
trans_date=YYYY-MM-DD
```

This provides several benefits:

* Efficient storage
* Compression
* Faster downstream processing
* Columnar access
* Better compatibility with Spark and Databricks
* Partition pruning for date-based queries

For example, when processing transactions for a specific date, downstream engines can read only the corresponding partition instead of scanning the complete dataset.

---

### 10. Streaming Checkpoints

The Spark Structured Streaming application uses **checkpoints** to maintain streaming state and track processing progress.

Checkpoints allow the streaming job to recover from failures and continue processing from its previous state rather than restarting the entire stream from the beginning.

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├──────────────► Checkpoint Location
  │
  ▼
Amazon S3
```

This is particularly important for a continuously running fraud detection pipeline where data should not be unnecessarily reprocessed.

---

### 11. End-to-End AWS Data Flow

The complete ingestion process combines the historical and real-time paths:

```text
                    ┌──────────────────────────┐
                    │     Historical Data      │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                           ┌───────────┐
                           │ Amazon S3 │
                           └─────┬─────┘
                                 │
                                 │
                                 │
┌─────────────────────┐          │
│ Real-Time Generator │          │
└──────────┬──────────┘          │
           │                     │
           ▼                     │
     ┌────────────┐              │
     │   Kafka    │              │
     └─────┬──────┘              │
           │                     │
           ▼                     │
 ┌──────────────────────┐        │
 │ Spark Structured     │        │
 │ Streaming            │        │
 └──────────┬───────────┘        │
            │                    │
            ▼                    ▼
          ┌─────────────────────────┐
          │       Amazon S3         │
          │       Data Lake         │
          └────────────┬────────────┘
                       │
                       ▼
              Databricks / Spark
                       │
                       ▼
              Feature Engineering
                       │
                       ▼
                 ML Scoring
```

---

### 12. Technologies Used

| Technology         | Purpose                                   |
| :----------------- | :---------------------------------------- |
| **AWS EC2**        | Hosting the ingestion environment         |
| **Amazon S3**      | Cloud data lake / raw data storage        |
| **Apache Kafka**   | Real-time event streaming                 |
| **Python**         | Transaction generation and Kafka producer |
| **Apache Spark**   | Structured Streaming and data processing  |
| **Docker**         | Containerization of Kafka services        |
| **Docker Compose** | Kafka environment orchestration           |
| **Parquet**        | Optimized transaction data storage        |

---

### 13. Key Responsibilities

My contribution to the project covered the complete ingestion path from transaction generation to the S3 data lake:

* Designed the **AWS-based data ingestion architecture**.
* Configured and managed the **AWS EC2** ingestion environment.
* Containerized the Kafka infrastructure using **Docker and Docker Compose**.
* Implemented the **Python transaction producer**.
* Configured the `fraud-stream-topic` Kafka topic.
* Implemented **real-time transaction streaming** through Kafka.
* Implemented **Spark Structured Streaming** for Kafka consumption.
* Configured the streaming pipeline to write processed transactions to **Amazon S3**.
* Used **Parquet** as the storage format.
* Implemented **date-based partitioning** in S3.
* Configured **streaming checkpoints** for reliable continuous processing.
* Implemented and validated both **historical data ingestion** and **real-time streaming ingestion**.
* Tested the complete flow from **transaction generation → Kafka → Spark → S3**.
* Prepared the S3 data layer for consumption by the downstream **Databricks Machine Learning pipeline**.

> **Output of this stage:** A continuously updated AWS S3 data lake containing historical and real-time transaction data in partitioned Parquet format, ready for downstream feature engineering, real-time fraud scoring, and model retraining.
