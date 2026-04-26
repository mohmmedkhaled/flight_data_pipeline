# Real-Time Flight Data Engineering Pipeline (GCP)

An end-to-end streaming data pipeline that ingests, processes, and stores real-time flight data from the OpenSky Network API using Google Cloud Platform (GCP) and Apache Beam.

## 🏗️ Architecture
The pipeline follows a modern data engineering pattern:
1. **Source:** OpenSky Network REST API (Streaming state vectors).
2. **Producer:** Python-based service that fetches data and publishes messages to **GCP Pub/Sub**.
3. **Ingestion:** **Cloud Pub/Sub** acts as the message broker for decoupling.
4. **Processing:** **Apache Beam** (running on **Dataflow**) performs real-time ETL:
   - Data parsing and cleaning.
   - Schema mapping and validation.
   - Handling missing values (NULLs).
5. **Sink:** **Cloud SQL (PostgreSQL)** for structured storage and analytical readiness.
6. **IaC:** Infrastructure provisioned using **Terraform**.



## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Stream Processing:** Apache Beam
* **Cloud Provider:** Google Cloud Platform (GCP)
* **Messaging:** Cloud Pub/Sub
* **Database:** Cloud SQL (PostgreSQL)
* **Infrastructure:** Terraform
* **Secret Management:** Python-dotenv & Service Accounts

## 🚀 Getting Started

### 1. Prerequisites
- Google Cloud Project with Billing Enabled.
- Terraform installed.
- Python 3.9+ installed.
- A GCP Service Account with `Pub/Sub Admin`, `Dataflow Worker`, and `Cloud SQL Editor` roles.

### 2. Infrastructure Setup
```bash
cd terraform
terraform init
terraform apply
