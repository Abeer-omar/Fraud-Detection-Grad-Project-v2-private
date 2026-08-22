## 1- Data Streaming & Ingestion

The **Data Streaming & Ingestion** component is the first stage of the **TransactSafe** fraud detection pipeline. This part was implemented on **AWS** and is responsible for preparing both **historical transaction data** and **real-time streaming data**, running the ingestion infrastructure, and delivering transaction data to **Amazon S3** for downstream fraud detection and Machine Learning.

The ingestion layer combines **Python, Apache Kafka, Spark Structured Streaming, Docker, AWS EC2, and Amazon S3**.

---

### 1.1 Transaction Data

The pipeline works with credit card transaction data containing customer, card, transaction, merchant, financial, and geographical attributes.

A transaction record contains fields such as:

```text
• ssn            → Customer identifier
• cc_num         → Credit card number
• first / last   → Customer name
• gender         → Customer gender
• street / city / state / zip → Customer address
• lat / long     → Customer geographical coordinates
• job / dob      → Customer demographic information
• acct_num       → Account number
• trans_num      → Unique transaction identifier
• trans_date     → Transaction date
• trans_time     → Transaction time
• unix_time      → Unix timestamp
• category       → Transaction category
• amt            → Transaction amount
• merchant       → Merchant name
• merch_lat      → Merchant latitude
• merch_long     → Merchant longitude
```

<img width="976" height="816" alt="TransactSafe Record" src="https://github.com/user-attachments/assets/fe3ad542-e7a9-4783-9be0-8b6184ab4b98" />


The transaction schema provides the information required by the downstream feature engineering and fraud detection stages.

---

### 1.2 Data Preparation — Historical & Streaming Modes

A major part of the ingestion layer is the **data preparation process**, implemented through `prepare_data.py`.

The pipeline supports two execution modes depending on whether a historical dataset already exists.

```text
                         prepare_data.py
                                │
                                ▼
                    Historical dataset exists?
                         /             \
                       No               Yes
                       │                 │
                       ▼                 ▼
                Historical Mode    Streaming Mode
                       │                 │
                       ▼                 ▼
                Generate data      Reuse existing
                                  customers
                       │                 │
                       ▼                 ▼
              Generate customers   Read stream_state.txt
              + transactions       to determine current day
                       │                 │
                       ▼                 ▼
              Merge transaction    Randomly sample
              CSV files            max 500 transactions
                       │                 │
                       ▼                 ▼
              Validate records    Remove is_fraud
              / skip malformed    from stream data
              rows
                       │                 │
                       ▼                 ▼
                Historical CSV       stream_batch_*.csv
```

<p align="center">
  <img src="pics/ingestion_flow.png" alt="Historical and Streaming Data Preparation Flow" width="650"/>
</p>

---

### 1.3 Historical Data Mode

The **Historical Mode** is responsible for creating the initial transaction dataset used as the historical foundation of the pipeline.

When a historical dataset does not already exist, `prepare_data.py` triggers the data generation process.

The flow is:

```text
prepare_data.py
      │
      ▼
Historical Dataset Does Not Exist
      │
      ▼
Historical Mode
      │
      ▼
Execute Spark / Data Generation
      │
      ▼
Generate Customers + Transaction CSV Files
      │
      ▼
Merge Transaction CSV Files
      │
      ▼
Validate Records
      │
      ├── Valid Records
      │
      └── Skip Malformed Rows
      │
      ▼
Clean Temporary Output
      │
      ▼
historical_transactions.csv
customers.csv
```

The historical mode creates the initial data foundation that can later be used for:

* Historical analysis
* Data profiling
* Feature engineering
* Model training
* Model evaluation
* Fraud analysis
* Model retraining

---

### 1.4 Real-Time Streaming Mode

Once the historical dataset exists, the pipeline switches to **Streaming Mode**.

Instead of regenerating the complete dataset, the streaming process reuses the existing customer information and generates a controlled stream of new transactions.

The streaming process:

1. Reuses the existing customer data.
2. Reads `stream_state.txt` to determine the current streaming day.
3. Randomly samples up to **500 transactions**.
4. Removes the `is_fraud` column from the streaming data.
5. Cleans the temporary output.
6. Produces a new streaming batch.

Example output:

```text
stream_batch_*.csv
```

This allows the pipeline to simulate a continuous stream of transactions while keeping the historical dataset separate from incoming events.

---

### 1.5 Why Remove `is_fraud` from Streaming Data?

The historical dataset contains the `is_fraud` field because it represents the **ground-truth label** used during model development and evaluation.

For real-time transactions, however, the fraud status should not be provided to the scoring pipeline.

Therefore, the streaming preparation step removes:

```text
is_fraud
```

before the transaction enters the real-time detection pipeline.

This better represents a real-world fraud detection scenario:

```text
Historical Data
      │
      ├── Features
      └── is_fraud → Known Ground Truth
                       │
                       ▼
                  Model Training


Real-Time Data
      │
      └── Features only
               │
               ▼
          ML Model
               │
               ▼
        Fraud Probability
               │
               ▼
          Fraud Alert
```

---

### 1.6 AWS Infrastructure

The ingestion environment was deployed on **Amazon Web Services (AWS)**.

An **AWS EC2 instance** was used as the compute environment for the ingestion infrastructure.

The environment contains the components required to generate, stream, consume, and store transaction data.

```text
                         AWS
                          │
                          ▼
                    ┌──────────┐
                    │   EC2    │
                    └────┬─────┘
                         │
                    Docker Environment
                         │
              ┌──────────┴──────────┐
              │                     │
              ▼                     ▼
         Kafka Broker        Streaming Services
              │                     │
              └──────────┬──────────┘
                         │
                         ▼
                 Spark Streaming
                         │
                         ▼
                    Amazon S3
```

---

### 1.7 Dockerized Kafka Infrastructure

The Kafka environment was containerized using **Docker** and managed through **Docker Compose**.

The repository contains a dedicated `docker` directory for the Kafka infrastructure.

The Dockerized setup provides a reproducible environment for the streaming components and simplifies starting and stopping the Kafka services.

```text
docker/
└── Kafka Environment
       │
       ▼
Kafka Broker
       │
       ▼
fraud-stream-topic
```

---

### 1.8 Real-Time Event Streaming with Kafka

After the streaming batches are generated, the transaction events are published to **Apache Kafka**.

The main Kafka topic used by the pipeline is:

```text
fraud-stream-topic
```

The real-time flow is:

```text
stream_batch_*.csv
        │
        ▼
Python Kafka Producer
        │
        ▼
Kafka Broker
        │
        ▼
fraud-stream-topic
        │
        ▼
Spark Structured Streaming
```

Kafka provides the event-driven communication layer between the transaction producer and the Spark streaming application.

---

### 1.9 Spark Structured Streaming

**Spark Structured Streaming** consumes the transaction events from Kafka and processes them continuously.

The streaming consumer:

```text
Kafka
  │
  ▼
Read fraud-stream-topic
  │
  ▼
Parse Transaction Events
  │
  ▼
Apply Schema
  │
  ▼
Process Streaming Data
  │
  ▼
Write to Amazon S3
```

Spark acts as the processing bridge between the Kafka event stream and the AWS S3 data lake.

---

### 1.10 Amazon S3 Data Lake

The processed transaction stream is ultimately stored in **Amazon S3**, which acts as the data lake layer for the downstream pipeline.

The streaming data is stored in **Parquet** format and organized using transaction-date partitions.

```text
Amazon S3
│
└── transactions/
    │
    ├── trans_date=2026-08-20/
    │     └── *.parquet
    │
    ├── trans_date=2026-08-21/
    │     └── *.parquet
    │
    └── trans_date=2026-08-22/
          └── *.parquet
```

Using Parquet provides:

* Columnar storage
* Compression
* Efficient analytical processing
* Compatibility with Spark and Databricks
* Reduced storage and processing overhead

Date-based partitioning also allows downstream processing engines to read only the required partitions.

---

### 1.11 Streaming State Management

The streaming preparation process uses:

```text
stream_state.txt
```

to keep track of the current streaming day.

This allows the streaming process to continue from the appropriate point rather than starting the simulation from the beginning every time.

The state-management flow is:

```text
stream_state.txt
       │
       ▼
Determine Current Day
       │
       ▼
Generate Next Streaming Batch
       │
       ▼
stream_batch_*.csv
       │
       ▼
Kafka
```

---

### 1.12 Pipeline Automation

The ingestion repository also contains scripts for controlling the pipeline:

```text
start_pipeline.sh
stop_pipeline.sh
```

These scripts simplify starting and stopping the required ingestion services.

The repository is organized as follows:

```text
1- Data Streaming & Ingestion/
│
├── data_generation/
│   └── Data preparation & generation scripts
│
├── historical_pipeline/
│   └── Historical data ingestion
│
├── streaming_pipeline/
│   └── Real-time streaming components
│
├── docker/
│   └── Kafka Docker configuration
│
├── requirements.txt
├── venv_requirements.txt
├── start_pipeline.sh
└── stop_pipeline.sh
```

<p align="center">
  <img src="pics/ingestion_github_structure.png" alt="Data Streaming and Ingestion GitHub Structure" width="850"/>
</p>

---

### 1.13 End-to-End Data Flow

The complete ingestion process can be summarized as:

```text
                    ┌──────────────────────┐
                    │    prepare_data.py   │
                    └──────────┬───────────┘
                               │
                               ▼
                  Historical Dataset Exists?
                       /               \
                     No                 Yes
                     │                   │
                     ▼                   ▼
             Historical Mode       Streaming Mode
                     │                   │
                     ▼                   ▼
             Generate Data        Reuse Customers
                     │                   │
                     ▼                   ▼
             Validate & Merge      Generate Batch
                     │                   │
                     │                   ▼
                     │             Remove is_fraud
                     │                   │
                     └─────────┬─────────┘
                               │
                               ▼
                       Transaction Data
                               │
                               ▼
                         Kafka Producer
                               │
                               ▼
                            Kafka
                     fraud-stream-topic
                               │
                               ▼
                  Spark Structured Streaming
                               │
                               ▼
                            Parquet
                               │
                               ▼
                           Amazon S3
                               │
                               ▼
                     Databricks ML Pipeline
                               │
                               ▼
                       Real-Time Scoring
```

---

### 1.14 Technologies Used

| Technology         | Purpose                                  |
| :----------------- | :--------------------------------------- |
| **AWS EC2**        | Hosting the ingestion environment        |
| **Amazon S3**      | Data lake and transaction storage        |
| **Apache Kafka**   | Real-time event streaming                |
| **Apache Spark**   | Data generation and Structured Streaming |
| **Python**         | Data preparation and Kafka producer      |
| **Docker**         | Containerizing Kafka services            |
| **Docker Compose** | Managing the Kafka environment           |
| **Parquet**        | Efficient transaction data storage       |
| **Bash**           | Pipeline startup and shutdown automation |

---

### 1.15 Key Responsibilities

My contribution covered the ingestion layer from **data preparation to the AWS S3 data lake**:

* Designed the **AWS-based data ingestion architecture**.
* Implemented both **Historical Mode** and **Streaming Mode**.
* Developed the data preparation and generation workflow.
* Generated customer and transaction datasets.
* Implemented validation and handling of malformed records.
* Implemented transaction batch generation for streaming.
* Implemented streaming state management using `stream_state.txt`.
* Removed the `is_fraud` label from real-time transaction data.
* Implemented the **Python Kafka producer**.
* Configured the `fraud-stream-topic`.
* Set up and managed **Kafka using Docker and Docker Compose**.
* Configured the **AWS EC2** ingestion environment.
* Implemented **Spark Structured Streaming** for Kafka consumption.
* Configured streaming output to **Amazon S3**.
* Used **Parquet** for efficient data storage.
* Implemented **date-based partitioning**.
* Created scripts to start and stop the complete ingestion pipeline.
* Validated the end-to-end flow from data generation to S3.
* Prepared the S3 data layer for the downstream **Databricks Machine Learning & Streaming Pipeline**.

> **Output of this stage:** Historical transaction data and continuously generated real-time transaction events are prepared, streamed through Kafka, processed using Spark Structured Streaming, and delivered to the AWS S3 data lake for downstream fraud detection and Machine Learning.
