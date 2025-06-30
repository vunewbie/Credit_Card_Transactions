# Credit Card Transactions Streaming System

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-231F20?style=flat-square&logo=apache-kafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.6-E25A1C?style=flat-square&logo=apache-spark)](https://spark.apache.org/)
[![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.4.1-66CCFF?style=flat-square&logo=apache-hadoop)](https://hadoop.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.5-017CEE?style=flat-square&logo=apache-airflow)](https://airflow.apache.org/)

A real-time credit card transactions streaming system using Kafka, Spark, HDFS, and Airflow for fraud detection and analytics.

## Table of Contents

- [Overview](#-overview)
- [Installation](#-installation)
- [Project Structure](#-project-structure)
- [Environment Configuration](#️-environment-configuration)
- [Start services](#-start-services)

## Overview

This system performs real-time streaming and analysis of credit card transactions with the following features:

- **Kafka Producer**: Reads CSV data and streams in real-time
- **Spark Streaming**: Processes, transforms, and enriches data
- **HDFS Storage**: Stores processed data
- **Power BI Integration**: Pushes data to dashboard
- **Airflow Scheduling**: Automates the pushing data to Power BI job



## Installation

### Clone repository
```bash
git clone <repository-url>
cd Credit_Card_Transactions
```

### Install Python dependencies
```bash
pip install -r requirements.txt
```

### Install tools and framework following this link
[Installation Guide](https://www.notion.so/0-Installation-1ed4d3cd38d580cf8e1fd5e071ce84f3?source=copy_link)

## Project Structure

```
Credit_Card_Transactions/
├── Airflow/              
│   └── scheduler.py         
├── Dataset/              
├── Hadoop/               
│   ├── serving.py           
│   └── last_push_timestamp.txt
├── Kafka/                
│   ├── producer.py          
│   └── offset.txt           
├── Spark/                
│   └── streaming.py         
├── .env                  
├── requirements.txt      
├── reset.sh             
└── value_population.ipynb
```

## Environment Configuration

### .env File Configuration

Update the environment variables in the `.env` file if you clone this project to `/home/<username>`:

#### Kafka Configuration
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=credit_card_transactions
```

#### Data Source
```bash
DATA_FILE_PATH=Dataset/credit_card_transactions.csv
OFFSET_FILE_PATH=Kafka/offset.txt
```

#### HDFS Configuration
```bash
HDFS_PATH=hdfs://localhost:9000/odap/credit_card_transactions
HDFS_CHECKPOINT=hdfs://localhost:9000/odap/checkpoint/
HDFS_NAMENODE_URL=http://localhost:9870
HDFS_USERNAME=<your-username>
HDFS_DATA_PATH=/odap/credit_card_transactions
```

#### External APIs
```bash
EXCHANGE_RATE_API_URL=https://wise.com/vn/currency-converter/usd-to-vnd-rate
```

#### Power BI Integration
```bash
POWERBI_URL=<your-powerbi-api-endpoint>
```

#### Timestamp Tracking
```bash
LAST_PUSH_TIMESTAMP_FILE_PATH=/home/<username>/Credit_Card_Transactions/Hadoop/last_push_timestamp.txt
```

#### Airflow Configuration
```bash
PROJECT_ROOT=/home/<username>/Credit_Card_Transactions
```

## Start services

#### Kafka
```bash
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties
```

#### Hadoop
```bash
start-dfs.sh
start-yarn.sh
jps  # Check running processes
```

### Initialize
```bash
bash reset.sh
```
*This script will create Kafka topics and necessary HDFS directories*

### Run streaming pipeline

#### Spark Streaming
```bash
python3 Spark/streaming.py
```

#### Kafka Producer
```bash
python3 Kafka/producer.py
```

#### Hadoop Serving (Optional)
```bash
python3 Hadoop/serving.py
```

### Airflow Scheduling

1. Access Airflow Web UI: http://localhost:8080/
2. Login with your created account
3. Find DAG `push_data_to_powerbi`
4. Enable and trigger the DAG

