CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`step_trainer_landing` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-s3-bucket/step_trainer_landing/'
TBLPROPERTIES ('classification'='json');
