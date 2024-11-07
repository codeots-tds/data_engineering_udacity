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
step_trainer_trusted_data_node1730830315068 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-s3-bucket/step_trainer_trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_data_node1730830315068")

# Script generated for node accelerometer_trusted_data
accelerometer_trusted_data_node1730831025786 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-s3-bucket/accelerometer_landing/"], "recurse": True}, transformation_ctx="accelerometer_trusted_data_node1730831025786")

# Script generated for node customer_curated_data
customer_curated_data_node1730913373500 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-s3-bucket/customer_curated/"], "recurse": True}, transformation_ctx="customer_curated_data_node1730913373500")

# Script generated for node Join step_trainer and accelerometer data
Joinstep_trainerandaccelerometerdata_node1730831631247 = Join.apply(frame1=accelerometer_trusted_data_node1730831025786, frame2=step_trainer_trusted_data_node1730830315068, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Joinstep_trainerandaccelerometerdata_node1730831631247")

# Script generated for node Renamed keys for join_cus_curated_step_accel
Renamedkeysforjoin_cus_curated_step_accel_node1730917100842 = ApplyMapping.apply(frame=customer_curated_data_node1730913373500, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("registrationdate", "bigint", "registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("y", "double", "right_y", "double"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"), ("x", "double", "right_x", "double"), ("timestamp", "bigint", "timestamp", "long"), ("email", "string", "right_email", "string"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="Renamedkeysforjoin_cus_curated_step_accel_node1730917100842")

# Script generated for node join_cus_curated_step_accel
join_cus_curated_step_accel_node1730917025387 = Join.apply(frame1=Joinstep_trainerandaccelerometerdata_node1730831631247, frame2=Renamedkeysforjoin_cus_curated_step_accel_node1730917100842, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="join_cus_curated_step_accel_node1730917025387")

# Script generated for node Output_to_S3
# Output_to_S3_node1730917182737 = glueContext.write_dynamic_frame.from_options(frame=join_cus_curated_step_accel_node1730917025387, connection_type="s3", format="glueparquet", connection_options={"path": "s3://stedi-s3-bucket/machine_learning_curated/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Output_to_S3_node1730917182737")
Output_to_S3_node1730917182737 = glueContext.getSink(path="s3://stedi-s3-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Output_to_S3_node1730917182737")
Output_to_S3_node1730917182737.setCatalogInfo(catalogDatabase="stedi-database",catalogTableName="machine_learning_curated")
Output_to_S3_node1730917182737.setFormat("glueparquet", compression="snappy")
Output_to_S3_node1730917182737.writeFrame(join_cus_curated_step_accel_node1730917025387)

job.commit()