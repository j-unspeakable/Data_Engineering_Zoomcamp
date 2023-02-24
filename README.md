## CONTENT
1. [ Description ](#desc)
2. [ Week 1: Introduction & Prerequisites ](#week1)
3. [ Week 2: Workflow Orchestration ](#week2)
4. [ Week 3: Data Warehouse ](#week3)
5. [ Week 4: Analytics engineering ](#week4)
6. [ Week 5: Batch processing ](#week5)
7. [ Week 6: Streaming ](#week6)
8. [ Summary ](#summ)

<a name="desc"></a>
## Description: Data Engineering Zoomcamp
This repository contains tasks worked on during the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](http://datatalks.club/) anchored by Alexey Grigorev. 

<a name="week1"></a>
## Week 1: Introduction & Prerequisites
- ETL was carried out from the web to a local PostgreSQL database.
- SQL queries were written to get some insights into two datasets and this queries were ran on a PgAdmin container connected to a PostgreSQL container in a docker-compose file.

<a name="week2"></a>
## Week 2: Workflow Orchestration

- Workflows to move the Green NYC taxi dataset from the web to GCS and from GCS to Big Query for a few months were orchestrated using Prefect.
- Deployments were created and scheduled for execution using the Prefect Orion UI and in the Python Scripts
- Storage Blocks were created in the Prefect Orion UI to store secret credentials/configurations and interact with external systems.
- The Prefect Cloud was used to setup an Automation system that sends an E-mail notification when a workflow has been completed.

<a name="week3"></a>
## Week 3: Data Warehouse

- BigQuery as a Data Warehouse solution.
- Best practices in BigQuery (Cost reduction and query performance) was carried out using Partitioning and Clustering.
- SQL for machine Learning in BigQuery.
- BigQuery Machine Learning Deployment.

<a name="week4"></a>
## Week 4: Analytics engineering
- Introduction to Analytics Engineering
- Data Modelling concepts
- Data Transformation with DBT. 

>> In summary, models were developed with the Yellow taxi data (Years 2019 and 2020), Green taxi data (Years 2019 and 2020), and fhv data (Year 2019). Tests were carried out and an extensive documentation was developed. Deployment was also implemented with best practices and a dashboard was created with Google's [looker studio](https://lookerstudio.google.com/).