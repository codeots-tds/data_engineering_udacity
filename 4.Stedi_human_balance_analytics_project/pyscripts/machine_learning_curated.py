import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node step_trainer_trusted_data
step_trainer_trusted_data_node1730830315068 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://stedi-s3-bucket/step_trainer_trusted/"], "recurse": True}, 
    transformation_ctx="step_trainer_trusted_data_node1730830315068"
)

# Script generated for node accelerometer_trusted_data
accelerometer_trusted_data_node1730831025786 = glueContext.create_dynamic_frame.from_options(
    format_options={},  # Adjusted if needed
    connection_type="s3", 
    format="parquet",
    connection_options={"paths": ["s3://stedi-s3-bucket/accelerometer_trusted/"], "recurse": True}, 
    transformation_ctx="accelerometer_trusted_data_node1730831025786"
)


# Script generated for node Join step_trainer and accelerometer data
Joinstep_trainerandaccelerometerdata_node1730831631247 = Join.apply(
    frame1=accelerometer_trusted_data_node1730831025786, 
    frame2=step_trainer_trusted_data_node1730830315068, 
    keys1=["timestamp"], 
    keys2=["sensorreadingtime"], 
    transformation_ctx="Joinstep_trainerandaccelerometerdata_node1730831631247"
)

# Script generated for node Output_to_S3
Output_to_S3_node1730917182737 = glueContext.getSink(
    path="s3://stedi-s3-bucket/machine_learning_curated/", 
    connection_type="s3", 
    updateBehavior="UPDATE_IN_DATABASE", 
    partitionKeys=[], 
    enableUpdateCatalog=True, 
    transformation_ctx="Output_to_S3_node1730917182737"
)
Output_to_S3_node1730917182737.setCatalogInfo(catalogDatabase="stedi-database", catalogTableName="machine_learning_curated")
Output_to_S3_node1730917182737.setFormat("glueparquet", compression="snappy")
Output_to_S3_node1730917182737.writeFrame(Joinstep_trainerandaccelerometerdata_node1730831631247)

job.commit()