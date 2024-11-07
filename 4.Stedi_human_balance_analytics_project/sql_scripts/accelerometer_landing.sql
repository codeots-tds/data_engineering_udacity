CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-s3-bucket/accelerometer_landing/'
TBLPROPERTIES ('classification'='json');
