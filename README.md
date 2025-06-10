# 💳 Real-Time Credit Card Transaction Processing

## 🚀 Overview

This project simulates real-time credit card transaction data flow from POS machines to Power BI, consisting of 2 phases:

1. **Kafka + Spark Streaming**: simulates data streaming and real-time processing  
2. **Hadoop + Power BI + Airflow**: data storage, visualization, and automated scheduling

---

## 🛠️ Initial Setup: Start Required Services

Before running the pipeline, ensure the following services are started:

### 1. Start Hadoop (HDFS)

```bash
start-dfs.sh
```

Check if Namenode is out of safe mode:

```bash
hdfs dfsadmin -safemode get
```

If still in safe mode, force it out:

```bash
hdfs dfsadmin -safemode leave
```

### 2. Start Kafka in KRaft Mode (Kafka 3.9+)

Kafka version 3.9 and above supports KRaft mode (no need for Zookeeper).  
Make sure you’ve initialized the metadata directory and created `KRaft` config:

```bash
# Create logs directory
mkdir -p /tmp/kraft-combined-logs

# Start Kafka in KRaft mode
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties
```

> Ensure that `KAFKA_HOME` is set and you are using Kafka 3.9+ with properly configured `server.properties` for KRaft mode (e.g., `process.roles`, `node.id`, `controller.quorum.voters`, etc.)


### 3. Reset HDFS Checkpoints and Output Folder For First Time Run

If this is your first run or you want a clean slate:

```bash
bash src/reset_hdfs.sh
```

This will:
- Delete and recreate `/odap` and `/odap_checkpoint` on HDFS
- Reset timestamp tracker `src/last_push_timestamp.txt`

---

## 📁 Directory Structure

```
Credit_Card_Transactions/
├── src/
│   ├── producer.py
│   ├── streaming_job.py
│   ├── serving.py
│   ├── scheduler.py
│   └── last_push_timestamp.txt
├── requirements.txt
├── README.md
```

---

## 🧹 Phase 1: Kafka + Spark Streaming

### 1. Run Spark Streaming Job (read from Kafka, write to HDFS)

```bash
python3 src/streaming_job.py
```

### 2. Run Producer (simulate transaction data sent to Kafka)

```bash
python3 src/producer.py
```

> Each transaction from the CSV file will be sent to Kafka with a random delay (1–5 seconds)

---

## 🧸 Phase 2: Hadoop + Power BI + Airflow

### 1. Configure `.env`

Create a `.env` file in the root `ODAP/` directory with the following content:

```ini
# producer
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=credit-card-transactions
KAFKA_CLIENT_ID=odap-producer

# streaming job
HDFS_PATH=/odap
HDFS_CHECKPOINT=/checkpoint
EXCHANGE_RATE_API_URL=https://... (if used)

# serving
POWER_BI_URL=https://api.powerbi.com/... (replace with actual URL)
HDFS_USER=hadoop
HDFS_WEB_URL=http://localhost:9870
HDFS_INTERNAL_PATH=/odap

# scheduler
HOME=/home/{user}
```

> 🔁 **Note**: If this is your first run, please clear all contents in `src/last_push_timestamp.txt`

### 2. Set Up Airflow

In your HOME directory, create a virtual environment and install Airflow:

```bash
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install --upgrade pip setuptools wheel

export AIRFLOW_VERSION=2.10.5
export PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Initialize the database and create a user:

```bash
airflow db init

airflow users create \
    --username admin \
    --firstname Vu \
    --lastname Nguyen \
    --role Admin \
    --email admin@gmail.com \
    --password admin

airflow webserver --port 8080
```

### 3. Activate DAG from Source Code

```bash
source airflow-venv/bin/activate
pip install -r ODAP/requirements.txt

mkdir -p ~/airflow/dags
cp ~/ODAP/src/scheduler.py ~/airflow/dags/

airflow scheduler
```

Then access the UI at `http://localhost:8080`, enable the `data_pipeline` DAG, and click **Trigger DAG** to test run.
