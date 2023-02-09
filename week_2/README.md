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
