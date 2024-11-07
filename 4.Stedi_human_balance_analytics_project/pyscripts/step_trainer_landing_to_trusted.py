import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

#intialize glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#load customer_trusted and step_trainer data into dynamic frames
step_trainer_data = glueContext.create_dynamic_frame.from_catalog(database = 'stedi-database', table_name = 'step_trainer_landing')
customer_trusted = glueContext.create_dynamic_frame.from_catalog(database = 'stedi-database', table_name = 'customer_trusted')

#Convert dynamic to dataframes
step_trainer_df = step_trainer_data.toDF()
customer_trusted_df = customer_trusted.toDF()

step_trainer_trusted_df = step_trainer_df.join(
    customer_trusted_df,
    step_trainer_df['serialnumber'] == customer_trusted_df['serialnumber']
    ).select(
        step_trainer_df['sensorreadingtime'],
        step_trainer_df['serialnumber'],
        step_trainer_df['distancefromobject']
)

#Convert dataframe to dynamicframe
step_trainer_trusted = DynamicFrame.fromDF(step_trainer_trusted_df, glueContext, 'step_trainer_trusted')

# Write the result to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://stedi-s3-bucket/step_trainer_trusted/",
                        "partitionKeys": [],  
                        "enableUpdateCatalog": True,
                        "updateBehavior": "UPDATE_IN_DATABASE"},
    format="parquet"
)

job.commit()