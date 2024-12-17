# Sparkify Data Pipeline Project with Airflow

## Table of Contents:
- Project Overview
- Data Modeling Diagrams
- Project Structure
- Project Setup
- DAG Structure

## 1. Project Overview
The Sparkify Data Pipeline Project with Airflow is the capstone project for The Data Engineering with AWS course on Udacity. This project is designed to build an ETL pipeline that extracts log and song data from S3, stages the data in Amazon Redshift, and transforms it into a star schema optimized for querying and analysis. There is also an Apache Airflow component that helps orchestrate the ETL process into your Redshift Datawarehouse.

## 2. Data Modeling Diagram:
  - Dataflow Diagram:
![Dataflow Diagram](5.Data_pipeline_project_w_airflow/project_screenshots/dataflowdiagram.png)

  - Sparkify ERD Diagram:
![Sparkify ERD](5.Data_pipeline_project_w_airflow/project_screenshots/ERD_diagram.png)


## 3. Project Structure:
  - sql_queries: SQL statements to create Redshift tables and insert the data from staging tables into corresponding fact and dimension tables.

  - airflow: contains the dags that orchestrate the ETL workflow to load data S3 into staging tables, and from the staging tables into the corresponding fact and dimension tables. This folder also contains the test dags for testing the redshift and aws connections.
  
  - plugins: contains the pyscripts setting up the tasks in our ETL dag workflow. These files are load_dimension.py, data_quality.py, load_fact.py, stage_redshift.py

  - project_screenshots: showcases the screenshots at different points of the project from beginning to completion aligning with project guideline requirements.


## 4. Project Setup:
  1. Pre-requisites:
    a. Create an IAM Role with the following permissions:
      - AmazonS3ReadOnlyAccess
      - AmazonRedshiftFullAccess
    
  2. Load data files into corresponding bucket S3 folders:
    - Make sure you have AWS CLI installed and configured with your AWS account.
      ```bash
        aws s3 sync /local/song_data s3://<bucket-name>/song_data
        aws s3 sync /local/log_data s3://<bucket-name>/log_data
      ```
    
  3. Update Amazon Redshift cluster settings with the following parameters:
      - Make publicly accessible
      - Associate IAM role you created in step 3.1.a with this new cluster.
      - Check VPC group settings and edit inbound rules to create a custom rule allowing your local pc i.p to have access to your Redshift cluster.
      - Before moving onto the Airflow, create your corresponding staging tables, fact tables and dimension tables in your Redshift datawarehouse using Query Editor. 

  4. Go to Airflow UI:
    - Go to your terminal and launch Airflow using the commands below:
      - 'airflow webserver' and 'airflow scheduler'
    a. Create your AWS and Redshift connection objects.
    b. Test your connection objects by either clicking the 'test' command or write
    your own test DAG to test these connections and ensure a successful run.
    c. Now you're ready to run your main DAG, as the tasks run look at the task logs to ensure everything is running smoothly or if anything needs to be debugged.

  5. Query your data tables:
    - Once your dag runs smoothly with no errors, go to Query editor and run some
    test simple test queries to ensure data properly loaded into the corresponding tables.

## 4. DAG Structure:
  The DAG (`final_project.py`) defines the following tasks:
  1. **Begin Execution**: Marks the start of the workflow.
  2. **Stage Events to Redshift**: Loads log data from S3 to the `staging_events` table in Redshift.
  3. **Stage Songs to Redshift**: Loads song data from S3 to the `staging_songs` table in Redshift.
  4. **Load Songplays Fact Table**: Transforms and loads data into the `songplays` fact table.
  5. **Load Dimension Tables**: Transforms and loads data into `users`, `songs`, `artists`, and `time` dimension tables.
  6. **Run Data Quality Checks**: Validates the integrity of the data.
  7. **End Execution**: Marks the end of the workflow.

## 5.  

