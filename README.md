# FlexEau BI — Data Engineering and Business Intelligence Platform

An individual final-year engineering project developed during my professional immersion at **FlexEau Afrique**.

The project focuses on designing and implementing a data engineering and business intelligence platform for integrating operational data, building a PostgreSQL data warehouse and preparing reliable analytical data for decision-support dashboards.

The platform combines data ingestion, staging, transformation, dimensional modelling, workflow orchestration and business intelligence in a containerized environment.

---

## Project Overview

Operational information is distributed across several business domains, including:

- Geography and operational sites
- Customers and subscribers
- Water meters
- Commercial activities
- Billing and payments
- Water production
- Energy consumption
- Exploitation activities
- Incidents and interventions
- Recovery and service interruption operations

The objective of the project is to centralize these data sources into an analytical environment that can be used for reporting and decision support.

The implemented data pipeline follows this architecture:

```text
                    ┌─────────────────────┐
                    │       MySQL         │
                    │ Operational Database│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    DLT + Python     │
                    │   Data Ingestion    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ PostgreSQL Staging  │
                    │    flex_staging     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Apache Spark     │
                    │ Data Transformation │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ PostgreSQL DWH      │
                    │      flex_dwh       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Power BI       │
                    │ Analytical Dashboards│
                    └─────────────────────┘

              Workflow orchestration: Apache Airflow
              Infrastructure: Docker Compose



# Project Objectives

The main objectives of the project are to:

Extract operational data from a MySQL database

Integrate data from several business domains

Load extracted data into a PostgreSQL staging database

Separate raw data storage from analytical processing

Transform and combine datasets using Apache Spark

Build analytical dimensions and fact tables

Implement a PostgreSQL data warehouse

Orchestrate the data pipeline with Apache Airflow

Containerize the main infrastructure components

Prepare structured data for Power BI dashboards

Facilitate the analysis of production, commercial and operational activities

# Workflow Orchestration with Apache Airflow

The complete pipeline is orchestrated using Apache Airflow.

The main DAG is:

flex_eau_pipeline

The DAG is configured with a daily schedule:

@daily

The workflow is defined in:

airflow/dags/flex_eau_pipeline.py

The DAG uses Airflow BashOperator tasks to execute the ingestion and Spark transformation scripts.

Pipeline Stages

The workflow contains three main stages.

Stage 1 — Ingestion

The ingestion task executes:

python ingestion/mysql_to_staging.py

This task loads data from MySQL into the PostgreSQL staging database.

Stage 2 — Dimension Processing

The DAG executes the dimension-building jobs:

dim_date
dim_geo
dim_forage
dim_usage
dim_abonne
dim_mode_paiement
dim_type_incident
dim_compteur

These tasks create or update the analytical dimensions in the data warehouse.

Stage 3 — Fact Processing

The DAG executes the fact-building jobs:

fact_production
fact_energie
fact_facturation
fact_paiement
fact_exploitation

These tasks use the staging data and the previously created dimensions to populate the data warehouse.

# Task Dependencies

The main dependency structure is:

MySQL Ingestion
       |
       ├── Date Dimension
       ├── Geographic Dimension
       ├── Borehole Dimension
       ├── Usage Dimension
       ├── Subscriber Dimension
       ├── Payment Mode Dimension
       ├── Incident Type Dimension
       └── Meter Dimension
                    |
                    ▼
             Fact Processing
                    |
       ┌────────────┼────────────┬──────────────┐
       ▼            ▼            ▼              ▼
 Production      Energy      Billing        Payments
                    |
                    ▼
              Exploitation

The DAG is configured with:

catchup=False

One retry

Daily scheduling

Airflow tags related to FlexEau, ETL and data warehousing

Airflow provides a centralized interface for:

Scheduling pipeline executions

Monitoring task states

Viewing task logs

Inspecting DAG runs

Managing task dependencies

Detecting failed executions


Dockerized Infrastructure

The project uses Docker Compose to run the main infrastructure components.

The docker-compose.yml file defines the following services.

PostgreSQL Staging
Service: postgres-staging
Database: flex_staging
Local port: 5433

This database stores data extracted from MySQL before transformation.

PostgreSQL Data Warehouse
Service: postgres-dwh
Database: flex_dwh
Local port: 5434

This database stores the analytical dimensions and fact tables.

Apache Spark Master
Service: spark-master
Spark version: 3.5.1
Spark port: 7077
Web interface: 8080

The Spark Master manages the Spark processing environment.

Apache Spark Worker
Service: spark-worker
Spark version: 3.5.1

The Spark Worker connects to the Spark Master and executes Spark workloads.

Airflow Metadata Database
Service: postgres-airflow
Database: airflow

This PostgreSQL service stores Airflow metadata.

Airflow Initialization Service

The airflow-init service is responsible for:

Initializing the Airflow database

Applying Airflow database migrations

Creating the initial administrator account

Airflow Scheduler

The scheduler is responsible for:

Scheduling DAG executions

Managing task execution

Monitoring dependencies

Coordinating the pipeline workflow

Airflow Webserver

The Airflow webserver provides the graphical interface for monitoring and managing the DAG.

The interface is exposed locally through:

http://localhost:8081
Repository Structure
Flex-Eau-Bi/
│
├── airflow/
│   ├── dags/
│   │   └── flex_eau_pipeline.py
│   ├── Dockerfile
│   └── logs/
│
├── ingestion/
│   ├── config.py
│   ├── mysql_to_staging.py
│   └── __pycache__/
│
├── spark_jobs/
│   ├── dimensions/
│   │   ├── dim_abonne.py
│   │   ├── dim_compteur.py
│   │   ├── dim_date.py
│   │   ├── dim_forage.py
│   │   ├── dim_geo.py
│   │   ├── dim_mode_paiement.py
│   │   ├── dim_type_incident.py
│   │   └── dim_usage.py
│   │
│   ├── facts/
│   │   ├── fact_energie.py
│   │   ├── fact_exploitation.py
│   │   ├── fact_facturation.py
│   │   ├── fact_paiement.py
│   │   └── fact_production.py
│   │
│   └── Dockerfile
│
├── jars/
│   └── postgresql-42.7.8.jar
│
├── docker-compose.yml
├── main.py
└── README.md
Technologies Used
Programming and Data Processing

Python

SQL

pandas

NumPy

DLT

SQLAlchemy

PyMySQL

Databases

MySQL

PostgreSQL

JDBC

Data Engineering

Data ingestion

ETL pipelines

Data staging

Data transformation

Data integration

Data warehousing

Dimensional modelling

Distributed Processing

Apache Spark

Spark Master

Spark Worker

Workflow Orchestration

Apache Airflow

DAGs

BashOperator

Task dependencies

Pipeline scheduling

Infrastructure

Docker

Docker Compose

Business Intelligence

Power BI

Analytical reporting

Decision-support dashboards

Running the Project
Requirements

Before running the project, install or configure:

Docker

Docker Compose

Python

A compatible MySQL source database

The expected source tables

Power BI Desktop for dashboard development or visualization

The project also requires the PostgreSQL JDBC driver included in the jars directory.
