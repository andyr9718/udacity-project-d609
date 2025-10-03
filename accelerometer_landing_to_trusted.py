import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1759349880774 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1759349880774")

# Script generated for node Accelerometer Landing S3 Bucket
AccelerometerLandingS3Bucket_node1759505759627 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://andrew-robinson2/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLandingS3Bucket_node1759505759627")

# Script generated for node Join
Join_node1759349717579 = Join.apply(frame1=CustomerTrusted_node1759349880774, frame2=AccelerometerLandingS3Bucket_node1759505759627, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1759349717579")

# Script generated for node Drop Fields
DropFields_node1759349959497 = DropFields.apply(frame=Join_node1759349717579, paths=["email", "phone"], transformation_ctx="DropFields_node1759349959497")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1759349959497, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759349434628", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1759350096037 = glueContext.getSink(path="s3://andrew-robinson2/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1759350096037")
AccelerometerTrusted_node1759350096037.setCatalogInfo(catalogDatabase="dev",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1759350096037.setFormat("json")
AccelerometerTrusted_node1759350096037.writeFrame(DropFields_node1759349959497)
job.commit()