import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

#intializing glue jobs
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Load stock accelerometer data and customer_trusted data
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(database='stedi-database', 
                                                                        table_name = 'accelerometer_landing')
customer_trusted = glueContext.create_dynamic_frame.from_catalog(database='stedi-database', 
                                                                        table_name= 'customer_trusted')

#Convert dynamic frames to dataframes
accelerometer_df = accelerometer_landing.toDF()
customer_trusted_df = customer_trusted.toDF()
#--------------------------------------------------------------------------------------
# Join accelerometer data with customer_trusted data on email/user column using pyspark
# accelerometer_trusted_df = accelerometer_df.join(
#     customer_trusted_df,
#     accelerometer_df['user'] == customer_trusted_df['email']
# ).select(
#     accelerometer_df['user'],
#     accelerometer_df['timestamp'],
#     accelerometer_df['x'],
#     accelerometer_df['y'],
#     accelerometer_df['z']
# )
#--------------------------------------------------------------------------------------
#Prepare temporary views on datasets for SQL
accelerometer_df.createOrReplaceTempView("accelerometer_landing")
customer_trusted_df.createOrReplaceTempView("customer_trusted")

#JOIN accelerometer data with customer_trusted data on email/user column using sql
accelerometer_trusted_sql = spark.sql('''
SELECT al.user, al.timestamp, al.x, al.y, al.z
FROM accelerometer_landing al
INNER JOIN customer_trusted ct ON al.user = ct.email
''')

#convert sql result to dynamic frame
accelerometer_trusted = DynamicFrame.fromDF(accelerometer_trusted_sql, glueContext, 'accelerometer_trusted')

# Saving resulting accelerometer_trusted into S3
glueContext.write_dynamic_frame.from_options(
    frame = accelerometer_trusted,
    connection_type = "s3",
    connection_options = {"path": "s3://stedi-s3-bucket/accelerometer_trusted/"},
    format = "parquet"
)


job.commit()