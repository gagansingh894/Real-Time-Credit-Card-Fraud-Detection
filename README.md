# 🛡️ Credit Card Fraud Detection System

This project is an **end-to-end system** for detecting fraudulent credit card transactions. 
It integrates **data engineering, model training, and experiment tracking** into a production-ready architecture.

![Status](https://img.shields.io/badge/Status-In%20Progress-orange)  

# 🚀 How to Run

1. Clone the repository and move into the project root:
   ```bash
   git clone https://github.com/gagansingh894/Real-Time-Credit-Card-Fraud-Detection.git
   cd Real-Time-Credit-Card-Fraud-Detection
   ```
2. Start all services using Docker Compose (ensure you have docker compose installed)
   ```bash
   make setup
   ```
3. Wait for all containers (Airflow, Spark, Kafka, Cassandra, MLflow, etc.) to start.
4. Login to [Airflow UI](http://0.0.0.0:8083) using `admin` as ***username*** and ***password*** to trigger workflows

![airflow_dags.png](docs/airflow_dags.png)

**UI Links**
- Airflow UI: http://0.0.0.0:8083
- Kafka UI: http://0.0.0.0:8085
- Spark Master: http://0.0.0.0:8080
- MLFlow: http://0.0.0.0:5005


**⚠️ Warning: Resource Usage**

Running this system locally with Docker Compose is memory intensive.  
At least **10 GB of RAM** is required to run all services smoothly.

**⚠️ Cassandra Initialization**

The Cassandra cluster may take some time to become fully available after startup. The cassandra-init service runs initialization scripts (creditcard.cql) once the nodes respond.

However, in some cases (e.g., slow startup or schema agreement delays), you may need to manually re-run the initialization script:

```bash
   make cassandra-init
```

---
## 📌 Architecture
<p align="center">
  <img src="docs/architecture.jpg" alt="Architecture Diagram" width="600"/>
</p>

1. **File System** → Stores raw transaction data.  
2. **ETL Spark Job** → Reads raw data, transforms it, and persists it into Cassandra.  
3. **Cassandra** → Acts as the feature store for model training.  
4. **ML Training Spark Job** → Reads training data from Cassandra and trains models.  
5. **MLflow Tracking Server** → Logs trained models and experiment metadata.  
   - **MinIO** → Stores ML models and artifacts.  
   - **Postgres** → Stores MLflow experiment and run metadata.  
6. **Stream Processing Job** -> Consumes new transactions from kafka stream, transform, predict and persist 
7. **Kafka Producer** -> Generates transactions and publishes to topic
8. **Airflow** -> Orchestrates ETL, ML training, Spark Streaming and Kafka Producer
---

## 🛠️ Tech Stack
- **Python** for code
- **Apache Spark** for Batch + Stream processing  
- **Apache Cassandra** as Database and Feature store 
- **Apache Kafka** for Real-time transaction streaming 
- **MLflow** for Experiment tracking & model registry  
- **MinIO** for Artifact storage 
- **Postgres** for Metadata store  
- **Airflow** for orchestrating spark jobs and kafka producer
- **Docker Compose** for service orchestration
