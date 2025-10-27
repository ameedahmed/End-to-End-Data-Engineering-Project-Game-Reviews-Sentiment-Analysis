# Steam Top Selling Games ETL Pipeline
## Motivation
This is an end to end data pipeline project with the aim to help video game players make informed decisions regarding their entertainment purchases on Steam. This is achieved by measuring the number of positive and negative reviews against each title along with a tag cloud for positive and negative reactions. Output is a Looker Studio dashboard that can be accessed via: https://lookerstudio.google.com/reporting/b45ae42c-b8ad-498e-9d8a-c09ffee35fb2 

## Architecture
The pipeline follows the medallion structure, where the bronze layer is responsible for data ingestion, silver layer for data manipulation & gold layer for data analysis and visualization.

1. The ingestion process is carried out using AWS lambda functions & Docker for ready-deployment. The data itself is fetched using Steam's REST API (https://partner.steamgames.com/doc/store/getreviews) & stored in S3 buckets for further processing.
2. Once the data has been ingested & stored, it is loaded into Athena & transformerd into tables using the AWS Glue Crawler. From thereon, dbt is utilized to perform joins between the source tables in the bronze layer for further usage. The results in each stage is stored in S3 buckets in respective folders, instead of Red Shift in order to minimize cost. 
3. The tables in the silver layers are then utilized by a machine learning model from hugging face for sentiment analysis of each user review, with the results once again being saved in S3 buckets
4. The bronze, silver and gold layers are orchestrated via Airflow with Docker to run on a monthly basis.
5. The results from the bucket are exported to Google Sheet (https://docs.google.com/spreadsheets/d/1X7qNkV9SVKgCUSweqZsnJOeuPXIdfOswTKhP_Xn-9EU/edit?usp=sharing) & visualized in Looker Studio as end-result.
A visual diagram is as follows:
<img width="1221" height="361" alt="image" src="https://github.com/user-attachments/assets/6267c0dc-c214-4c79-93eb-20504543df19" />

## Project Folder Structure
1. Within the [Docker Folder Files](https://github.com/ameedahmed/End-to-End-Data-Engineering-Project-Game-Reviews-Sentiment-Analysis/tree/main/docker_folder_files), you have a folder called **bronze_layer** which is responsible for fetching the top selling games on steam, their reviews, ids and the total number of reviews.
2. The second folder [airflow](https://github.com/ameedahmed/End-to-End-Data-Engineering-Project-Game-Reviews-Sentiment-Analysis/tree/main/docker_folder_files/airflow) contains the dbt files for the silver layer as well as the orchestration file (DAG) for lambda+dbt functions.
3. The third folder [gold_layer](https://github.com/ameedahmed/End-to-End-Data-Engineering-Project-Game-Reviews-Sentiment-Analysis/tree/main/docker_folder_files/gold_layer) is the implementation of [hugging face multi-lingual sentiment analysis model](https://huggingface.co/tabularisai/multilingual-sentiment-analysis) as well as a script for transferring data from S3 bucket to google sheet for visualization.
## Final Note
In order to implement this project, please remember to save your aws key and id in the AWS Secrets Manager, along with your json credentials for google drive.
