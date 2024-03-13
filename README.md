# Core Scientific Data Trial - Gyan Jadal
## Data Trial Project containing Code, Reports and Documentation

This project has code for a system that polls data from different exchanges using rest api and ingests them into AWS S3. 
This is then processed using a data pipeline using the medallion architecture. The output of the pipeline is aggregated data that is consumed by tableau reports.

Following are the modules in the system
- Ingestion
- Pipeline Processing
- Reports

## Ingestion

- An AWS Eventbridge scheduler runs on a 1 min interval
- This invokes a handler inside an AWS Lambda function
- The scheduler is configured with the url for a specific exchange
- The handler invokes a rest api on the configured exchange
- The response is saved in AWS S3
- Code for this is located in the ingestion folder
- Deployment script is in ingestion\deploy folder

## Pipeline Processing

- This processing is implemented in medallion architecture using Databricks Live Tables (DLT).
- The bronze processor is triggered when new data arrives in AWS S3, reads and stores the incremental raw data in Bronze tables 
- The silver processor is triggered when new data is added to Bronze tables, incrementally reads, transforms and stores in Silver tables
- The gold processor is triggered when new data is added to Silver tables, incrementally adds data to dimensions and facts.
- This processor also recreates aggregate tables every run triggered when the fact table is appended with new data.

## Reporting

- Data from Aggregates is fed into Tableau.
- Here different charts are created to understand the order book.
- Report is located in reporting folder


