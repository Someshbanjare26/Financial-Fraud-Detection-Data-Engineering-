# Real-Time Financial Fraud Detection Data Pipeline on Azure

## 🚀 Project Overview

This project is an end-to-end Azure Data Engineering solution designed to simulate and process financial transaction data for fraud analysis. The system combines both batch and streaming-style data ingestion using Azure cloud services and Databricks transformations to build a scalable modern data pipeline.

The project follows the Medallion Architecture (Bronze → Silver → Gold) and demonstrates real-world data engineering concepts such as cloud ingestion, orchestration, distributed processing, and analytics dashboarding.

---

# 🏗️ Architecture

## Data Flow

Kaggle Fraud Dataset (Batch Data)
            ↓
Azure Data Factory (ADF)
            ↓
Azure Data Lake Storage Gen2 (Bronze Layer)
            ↓
Azure Databricks (PySpark Transformations)
            ↓
Silver Layer (Processed Data)
            ↓
Gold Layer (Analytics Ready Data)
            ↓
Power BI Dashboard

Streaming Simulation:

Python Script → ADLS Bronze Streaming Layer → Databricks Processing → Gold Analytics

---

# 🎯 Objectives

- Build a scalable cloud-based data engineering pipeline
- Simulate near real-time financial transaction ingestion
- Process and transform transaction data using Databricks
- Implement Medallion Architecture
- Create analytics-ready datasets
- Visualize fraud insights using Power BI

---

# ☁️ Azure Services Used

| Service | Purpose |
|---|---|
| Azure Data Factory | Data orchestration & ingestion |
| Azure Data Lake Storage Gen2 | Centralized cloud storage |
| Azure Databricks | Data transformation using PySpark |
| Azure Resource Group | Resource management |
| Power BI | Dashboard & analytics |

---

# 🔥 Features

## ✅ Batch Data Ingestion
- Kaggle Credit Card Fraud Dataset ingestion using Azure Data Factory
- Raw data stored in Bronze Layer

## ✅ Streaming Simulation
- Python-based transaction generator
- Near real-time JSON upload to ADLS

## ✅ Medallion Architecture
- Bronze → Raw Data
- Silver → Cleaned & transformed data
- Gold → Analytics-ready datasets

## ✅ Databricks Transformations
- Duplicate handling
- Feature engineering
- Risk categorization
- Transaction classification

## ✅ Analytics & Visualization
- Fraud risk analysis
- High-value transaction detection
- Location-based analysis
- Device usage trends

---

# ⚙️ Data Engineering Workflow

## Bronze Layer
Stores raw ingested data:
- Batch CSV files
- Streaming JSON transaction data

## Silver Layer
Processed and cleaned data:
- Deduplication
- Risk level creation
- Transaction categorization

## Gold Layer
Business-ready analytics datasets:
- Fraud summary
- High-risk transactions
- Location analysis

---

# 📊 Power BI Dashboard

The dashboard includes:

- Fraud Risk Distribution
- High Value Transactions
- Device Usage Analysis
- City-wise Transaction Analysis
- Transaction Trend Monitoring
- KPI Summary Cards

---

# 🧠 Key Concepts Demonstrated

- Data Lake Architecture
- Batch + Streaming Integration
- Cloud Data Engineering
- Distributed Processing with Spark
- PySpark Transformations
- Orchestration using ADF
- Analytics Pipeline Design
- Dashboard Visualization

---

# 📈 Future Enhancements

- Real-time streaming with Azure Event Hub
- Machine Learning based fraud prediction
- Alerting system integration
- Incremental pipeline loading
- CI/CD deployment for pipelines

---

# 📸 Project Showcase

This repository contains:

- Azure Data Factory pipeline screenshots
- Databricks notebook execution screenshots
- Data Lake structure screenshots
- Power BI dashboard screenshots
- End-to-end architecture visuals

---

# 🏆 Learning Outcomes

Through this project, the following skills were demonstrated:

- Azure Cloud Services
- Data Pipeline Design
- Databricks & PySpark
- Data Lake Management
- Data Transformation Engineering
- Near Real-Time Data Processing
- Dashboard Analytics

---

# 📌 Dataset Used

Credit Card Fraud Detection Dataset:
https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

---

# 👨‍💻 Author

Somesh Banjare

Advanced Azure Data Engineering Project

---

# 📄 Short Project Description

Real-Time Financial Fraud Detection Data Pipeline on Azure is an advanced cloud-based data engineering project that processes both batch and streaming-style financial transaction data using Azure services. The project uses Azure Data Factory for orchestration, Azure Data Lake for storage, Databricks for PySpark transformations, and Power BI for analytics visualization. The system follows the Medallion Architecture (Bronze, Silver, Gold) to create scalable and analytics-ready data pipelines capable of near real-time fraud analysis.

