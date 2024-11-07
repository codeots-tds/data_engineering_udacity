# STEDI Human Balance Analytics Project

## Project Overview
This project aims to process, transform, and analyze data to create curated datasets for the STEDI Human Balance Analytics initiative. Using AWS Glue, Athena, and S3, the goal is to develop landing, trusted, and curated data zones to support downstream machine learning and data analysis. The project entails processing customer, accelerometer, and step trainer data. Your objective is to build an ETL pipeline using AWS Services (S3, Glue, Athena, Pyspark, and IAM).

## Project Structure and Deliverables
### 1. Landing Zone
- **Glue Jobs**:
  - `customer_landing_to_trusted.py`: Connects to the S3 bucket to process the customer landing data.
  - `accelerometer_landing_to_trusted.py`: Connects to the S3 bucket to process the accelerometer landing data.
  - `step_trainer_trusted.py`: Connects to the S3 bucket to process the step trainer data.

- **SQL DDL Scripts**:
  - `customer_landing.sql`, `accelerometer_landing.sql`, `step_trainer_landing.sql` to create tables in AWS Glue Data Catalog.

- **Athena Queries**:
  - Count and inspect the number of rows in each landing table:
    - `customer_landing` should have 956 rows.
    - `accelerometer_landing` should have 81,273 rows.
    - `step_trainer_landing` should have 28,680 rows.
  - Confirm that `customer_landing` includes rows where `shareWithResearchAsOfDate` may be blank.

### 2. Trusted Zone
- **Glue Job Configurations**:
  - The Glue jobs are configured to dynamically update the schema in the Data Catalog.
  - Ensure the option to "Create a table in the Data Catalog and update the schema on subsequent runs" is set to true.

- **Athena Queries**:
  - Validate the results with counts:
    - `customer_trusted` should have 482 rows with no blank `shareWithResearchAsOfDate`.
    - `accelerometer_trusted` should have 40,981 rows.
    - `step_trainer_trusted` should have 14,460 rows.

- **Transformations**:
  - Use a filter in `customer_landing_to_trusted.py` to drop rows where `shareWithResearchAsOfDate` is null.

### 3. Curated Zone
- **Glue Jobs**:
  - `customer_trusted_to_curated.py`: Inner joins `customer_trusted` with `accelerometer_trusted` by email, producing a `customer_curated` table.
  - `step_trainer_trusted.py`: Joins `step_trainer_landing` with `customer_curated` by `serialnumber`.
  - `machine_learning_curated.py`: Joins `step_trainer_trusted` with `accelerometer_trusted` by `timestamp` and `sensorreadingtime`.


## 4. Project Structure:
  - images directory: contains the necessary screenshots requested for the
  project. These screenshots ensure the data tables have populated data with the projected row counts.

  - pyscripts: contains the python/pyspark scripts containing the business 
  transformation logic needed to prepare the trusted and curated datasets. 
  
  - sql_scripts: contains the SQL DDL scripts necessary for creating the landing/staging tables for our stock data.

## 5. Final Steps
- **Verify**: Run and screenshot Athena queries to show counts and ensure data integrity.
- **Document**: Ensure all scripts, SQL DDLs, and screenshots are well-organized for submission.
- **Clean Up (Optional)**: Delete any S3 outputs and Glue jobs if they are no longer needed to avoid costs.

## 6. Conclusion
The project ensures data is ingested, processed, and joined to create `machine_learning_curated` for further analysis, meeting the requirements set forth for STEDI Human Balance Analytics.