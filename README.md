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
