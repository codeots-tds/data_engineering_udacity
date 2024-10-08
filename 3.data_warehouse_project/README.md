# Udacity Datawarehouse Project

## Table of Contents:
- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Datasets](#Datasets)
- [Project Structure](#ProjectStructure)
- [Project ERD](#ProjectERD)
- [ETL Pipeline](#ETLPipeline)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview:
This project is part of Udacity's Data Engineering Nanodegree program and focuses on building an AWS Redshift-based data warehouse. The goal is to design a cloud-based data warehouse solution to handle large-scale data from an imaginary music streaming company, Sparkify.

The project involves setting up ETL (Extract, Transform, Load) pipelines that move data from Amazon S3 to Amazon Redshift, transforming it into a star-schema format optimized for analytical queries.

## Features:
List the key features of your project:
- Design a data warehouse using AWS Redshift to handle large volumes of data.
- Create an ETL pipeline that extracts data from S3, stages it in Redshift, and loads it into fact and dimension tables for analytics
- Optimize the database schema for performance using a star schema structure.

## Installation:
Step-by-step instructions to install and run the project locally:
1. Clone the repository:
   ```bash
   git clone https://github.com/codeots-tds/data_engineering_udacity/tree/master/3.data_warehouse_project


## Datasets:
There are two main datasets for this project which should be stored in Amazon S3 bucket.
- Song Data: Metadata about songs and the artists involved.
    - Example: s3://udacity-dend/song_data

- Log Data: Logs of user activity on the Sparkify app based on a simulation of events.
    - Example: s3://udacity-dend/log_data

## Project Structure:
The project contains the files below:
- create_tables.py: Script to create staging, fact, and dimension tables in Redshift.
- etl.py: Script to extract data from S3, stage it in Redshift, and load it into the final tables.
- sql_queries.py: Contains all the SQL queries used in the project for table creation and data insertion.
- dwh.cfg: Configuration file that contains AWS credentials and other Redshift connection details.

## Project ERD:
![Project ERD](<images/erd sparkify.png>)

### Database Schema:
This data warehouse is designed using a star schema with one fact table and 4 dimensional tables.

### Fact Tables:
- songplays table:
    - songplay_id INT PRIMARY KEY - primary key(auto increment)
    - start_time TIMESTAMP - timestamp of the song playing
    - user_id INT - id of user
    - level VARCHAR - subscription level
    - song_id INT - song id number
    - artist_id INT - artist id number
    - session_id INT - session id number
    - location VARCHAR - location of user
    - user_agent VARCHAR - user agent info

### Dimensional Tables:
- users table:
    - user_id INT PRIMARY KEY -user id number
    - first_name VARCHAR -user first name
    - last_name VARCHAR -user last name
    - gender CHAR(1) -user gender
    - level VARCHAR -subscription level

- songs table:
    - song_id VARCHAR PRIMARY KEY, - unqiue song identifier
    - title VARCHAR NOT NULL - song title
    - artist_id INT NOT NULL - artist id number
    - year INT - year the song was released
    - duration FLOAT - duration of the song playtime in seconds

- artist table:
    - artist_id INT PRIMARY KEY - unique artist id
    - name VARCHAR NOT NULL - name of artist
    - location VARCHAR - artist's home town
    - latitude FLOAT - latitude of artists location
    - longitude FLOAT - longitude of the artist's location

- time table:
    - start_time TIMESTAMP PRIMARY KEY - timestamp of start of the song
    - hour INT NOT NULL - hour from timestamp
    - day INT NOT NULL - day from timestamp
    - week INT NOT NULL -week from timestamp
    - month INT NOT NULL -month from timestamp
    - year INT NOT NULL - year from timestamp
    - weekday INT NOT NULL - day from timestamp

### Staging Tables:
- staging_events table - staging table for events data 
    - event_id INT - unique event id
    - artist  VARCHAR - artist name
    - auth    VARCHAR - authorization
    - first_name  VARCHAR - artist first name
    - gender      CHAR(1) - artist gender
    - item_in_session INT - item session
    - last_name VARCHAR - artist last name
    - length  FLOAT - length of time
    - level VARCHAR - subscription level
    - location VARCHAR - location
    - method VARCHAR - method of 
    - page  VARCHAR - song page
    - registration  BIGINT - registration number 
    - session_id    INT - song session id 
    - song      VARCHAR - song name
    - status        INT - artist status 
    - ts       BIGINT - time seconds
    - user_agent    VARCHAR - user agent
    - user_id       INT - user id number

-  staging_songs table:
    num_songs        INT - number of songs
    artist_id        VARCHAR - artist id number
    artist_latitude  FLOAT - artist latitude
    artist_longitude FLOAT - artist longitude
    artist_location  VARCHAR - artist location
    artist_name      VARCHAR - artist name
    song_id          VARCHAR - artist song_id
    title            VARCHAR - title of song
    duration         FLOAT - duration of song
    year             INT - year of song

## ETL Pipeline:
Purpose:
- The goal of this ETL pipeline is to extract json data from S3 bucket, load it into the staging tables in Redshift, transform and load the data into the corresponding fact and dimension tables.

Steps:
1. Staging: Copy the raw data into the S3 bucket into staging tables in Redshift.
2. Transformation: Taking the requirement business logic and transforming the data appropriately.
3. Loading the transformed data into the corresponding fact and dimension tables.

### How to Run:
1. AWS IAM Role: Have the proper AWS IAM Role configured with access to S3 and Redshift.
2. AWS Redshift Cluster: Create a Redshift cluster using AWS and record the connection 
details(host, username, database, password).

### Steps:
1. Configure dwh.cfg with your redshift and aws credentials.
2. Create your database tables with your schema by running create_tables.py
3. Run the ETL pipeline to load the data into the corresponding tables.

## Expected Result:
After the ETL pipeline is executed, the data from the song and log datasets will be stored in the 
tables in the star schema format in your Redshift cluster. The data should then be ready for querying.
