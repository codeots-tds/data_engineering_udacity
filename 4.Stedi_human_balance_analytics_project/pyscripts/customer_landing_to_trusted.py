"""
Python script that uses Spark to clean Customer data from 
website(landing zone) and only stores records of those who agreed to 
share data for research(Trusted Zones)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

#initalize Glue job 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Load the stock customer_landing data
customer_landing = glueContext.create_dynamic_frame.from_catalog(database="stedi-database", table_name="customer_landing")

#convert customer_landing dynamic frame to dataframe
customer_data_df = customer_landing.toDF()

# filter the data by 
customer_trusted_df = customer_data_df.filter(col("sharewithresearchasofdate").isNotNull())

#convert dataframe back to dynamic frame
customer_trusted = DynamicFrame.fromDF(customer_trusted_df, glueContext, "customer_trusted")


#Save dynamicframe back to S3-customer-data as customer_trusted dataset
glueContext.write_dynamic_frame.from_options(
    frame = customer_trusted,
    connection_type = "s3",
    connection_options = {"path": "s3://stedi-s3-bucket/customer_trusted/",
                          "partitionKeys": [],
                          "enableUpdateCatalog": True,
                          "updateBehavior": "UPDATE_IN_DATABASE"},
    format = "parquet"
)
job.commit()