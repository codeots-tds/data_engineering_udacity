import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted_data
customer_trusted_data_node1730847475239 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-s3-bucket/customer_trusted/"], "recurse": True}, transformation_ctx="customer_trusted_data_node1730847475239")

# Script generated for node accelerometer_trusted_data
accelerometer_trusted_data_node1730848013174 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-s3-bucket/accelerometer_trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_data_node1730848013174")

# Script generated for node accelerometer and customer join
accelerometerandcustomerjoin_node1730848136864 = Join.apply(frame1=customer_trusted_data_node1730847475239, frame2=accelerometer_trusted_data_node1730848013174, keys1=["email"], keys2=["user"], transformation_ctx="accelerometerandcustomerjoin_node1730848136864")

#Covert dynamicframe to dataframe to drop duplicates
joined_df = accelerometerandcustomerjoin_node1730848136864.toDF()

# Apply deduplication based on the 'email' column
deduplicated_df = joined_df.dropDuplicates(['email'])

#convert dataframe back to dynamic frame
deduplicated_df_dynamicframe = DynamicFrame.fromDF(deduplicated_df, glueContext, "deduplicated_dynamic_frame")

# Script generated for node output_save_customer_trusted_to_s3
output_save_customer_trusted_to_s3_node1730848570665 = glueContext.write_dynamic_frame.from_options(frame=deduplicated_df_dynamicframe, connection_type="s3", format="glueparquet", connection_options={"path": "s3://stedi-s3-bucket/customer_curated/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="output_save_customer_trusted_to_s3_node1730848570665")

job.commit()