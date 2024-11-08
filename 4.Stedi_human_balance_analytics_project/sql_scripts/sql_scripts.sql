#Drop Tables
drop_customer_trusted = '''DROP TABLE IF EXISTS `stedi-database`.`customer_trusted`;'''
drop_accelerometer_trusted = '''DROP TABLE IF EXISTS `stedi-database`.`accelerometer_trusted`;'''
drop_step_trainer_trusted = '''DROP TABLE IF EXISTS `stedi-database`.`step_trainer_trusted`;'''
drop_customer_curated = 'DROP TABLE IF EXISTS `stedi-database`.`customer_curated`;'''
drop_machine_learning_curated = '''DROP TABLE IF EXISTS `stedi-database`.`machine_learning_curated`;'''

#Trusted Tables

#Create Table Queries
create_customer_trusted ='''CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`customer_trusted` (
    `customername` STRING,
    `email` STRING,
    `phone` STRING,
    `birthday` STRING,
    `serialnumber` STRING,
    `registrationdate` BIGINT,
    `lastupdatedate` BIGINT,
    `sharewithresearchasofdate` BIGINT,
    `sharewithpublicasofdate` BIGINT,
    `sharewithfriendsasofdate` BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-s3-bucket/customer_trusted/'
TBLPROPERTIES ('classification' = 'parquet');'''

create_accelerometer_trusted = '''CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`accelerometer_trusted` (
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-s3-bucket/accelerometer_trusted/'
TBLPROPERTIES ('classification' = 'parquet');'''

create_step_trainer_trusted = '''CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`step_trainer_trusted` (
  sensorreadingtime bigint,
  serialnumber string,
  distancefromobject int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-s3-bucket/step_trainer_trusted/'
TBLPROPERTIES ('classification' = 'parquet');
'''

machine_learning_curated = '''CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`machine_learning_curated` (
  timestamp bigint,
  serialnumber string,
  distancefromobject int,
  x double,
  y double,
  z double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://stedi-s3-bucket/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'parquet');'''

#Staging Table Test Queries:
accelerometer_landing = '''SELECT * FROM accelerometer_landing;'''

count_accelerometer_landing = '''SELECT COUNT(*) AS row_count FROM accelerometer_landing;'''

customer_landing = '''SELECT * FROM customer_landing;'''

count_customer_landing = '''SELECT COUNT(*) AS row_count FROM customer_landing;'''

step_trainer_landing = '''SELECT * FROM step_trainer_landing;'''

count_step_trainer_landing = '''SELECT COUNT(*) AS row_count FROM step_trainer_landing;'''

# Trusted Tables Test Queries
trusted_customer = '''SELECT * FROM customer_trusted LIMIT 10;'''
count_trusted_customer = '''SELECT COUNT(*) FROM customer_trusted;'''

trusted_accelerometer = '''select * from accelerometer_trusted;'''
count_trusted_accelerometer = '''SELECT COUNT(*) FROM accelerometer_trusted;'''

trusted_step_trainer = '''select * from step_trainer_trusted;'''
count_trusted_step_trainer = '''SELECT COUNT(*) FROM step_trainer_trusted;'''

#curated test queries

#customers_curated
customer_curated = '''select * from customer_curated;'''
count_customer_curated = '''SELECT COUNT(*) FROM customer_curated;'''

#machine learning curated
machine_learning_curated = '''select * from machine_learning_curated;'''
count_machine_learning_curated = '''SELECT COUNT(*) FROM machine_learning_curated;'''