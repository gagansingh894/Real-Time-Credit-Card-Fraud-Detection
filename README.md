# 🚀 Credit Card Fraud Detection System

This project is an **end-to-end system** for detecting fraudulent credit card transactions.  
It integrates **data engineering, model training, and experiment tracking** into a production-ready architecture.  
The system is being extended with **streaming ingestion (Kafka + Spark Streaming)** and a **UI for monitoring faulty transactions**.

---

## 📌 Architecture  
![Status](https://img.shields.io/badge/Status-In%20Progress-orange)  
![Project](https://img.shields.io/badge/Project-Development-red)

> ⚠️ **Note:** Project is currently in **active development**. Expect breaking changes and rapid iteration.

### Current Flow  
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
---

## 🔮 Planned Extensions

- **Fraud Monitoring UI** → Dashboard for monitoring and reviewing suspicious transactions.  

---

## 🛠️ Tech Stack
- **Apache Spark** (Batch + Streaming)  
- **Apache Cassandra** (Database + Feature store)  
- **Apache Kafka** (Real-time transaction streaming) 
- **MLflow** (Experiment tracking & model registry)  
- **MinIO** (Artifact storage)  
- **Postgres** (Metadata store)  
- **Docker Compose** (Service orchestration)  

---