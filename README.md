# FlexEau BI — Data Engineering and Business Intelligence Platform

An individual final-year engineering project developed during my professional immersion at **FlexEau Afrique**.

The project focuses on integrating operational data, building a data warehouse and preparing analytical data for business intelligence dashboards.

## Objective

The objective is to design an automated data pipeline capable of:

- Extracting data from MySQL
- Loading data into a PostgreSQL staging database
- Transforming data with Apache Spark
- Building dimensional and fact tables
- Orchestrating the workflow with Apache Airflow
- Preparing data for Power BI dashboards

## Data Pipeline

```text
MySQL
   |
   v
DLT + Python
   |
   v
PostgreSQL Staging
   |
   v
Apache Spark
   |
   v
PostgreSQL Data Warehouse
   |
   v
Power BI
