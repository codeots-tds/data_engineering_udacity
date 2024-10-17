# Udacity Data Modeling with Apache Cassandra Project

## Table of Contents:
- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Datasets](#Datasets)
- [Project Structure](#ProjectStructure)
- [License](#license)
---

## Overview:
This project is a part of Udacity's Data Engineering with AWS Nanodegree program. The objective is to create a data model using Apache Cassandra. This database should be able to handle the queries related to user activity logs from a fictional music, streaming app called Sparkify. 


## Features:
- Data Preprocessing: Process the raw event data from multiple CSV files to create a cohesive, denormalized dataset.

- Data Modeling: Design and create appropriate Cassandra tables based on specific query needs.

- ETL Pipeline: Implement an ETL pipeline to load the processed data into the Cassandra tables.
    
- Testing: Execute queries to retrieve the necessary data from the Cassandra database to validate the data model and pipeline.
 

## Installation:
Step-by-step instructions to install and run the project locally:
1. Clone the repository:
   ```bash
   git clone https://github.com/codeots-tds/data_engineering_udacity/tree/master/1.data_modeling_with_apache_cassandra


## Datasets:
This project uses user event data from the Sparkify music streaming app. The dataset is stored in several CSV files which are parititoned by date in the 'event_data' folder. The goal is to process this data into one denormalized dataset which we will then use to load data into the Apache Cassandra tables.
    Filepath examples: 
    - event_data/2018-11-08-events.csv
    - event_data/2018-11-09-events.csv

## Project Structure:
The project contains the files below:
- event_data: the raw data directory of the partitioned csv files.
- Project_1B_Project.ipynb: Project file to conduct data preprocessing, and ETL processing.
- event_datafile_new.csv: Denormalized CSV file created by raw event_data.
- readme.md: Project documentation.

### Database Schema:
This data warehouse is designed using a star schema with one fact table and 4 dimensional tables.

    - songs_info table:
        - session_id int, 
        - itemInSession int, 
        - artist text, 
        - song text, 
        - song_length float, 
        - PRIMARY KEY(session_id, itemInSession)

    - users_info table:
        - song text, 
        - user_id int, 
        - first_name text, 
        - last_name text, 
        - PRIMARY KEY(song, user_id)


### License

This project is part of the Udacity Data Engineering Nanodegree and is intended for educational purposes only.