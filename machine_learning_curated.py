import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1759427489365 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1759427489365")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1759427431042 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1759427431042")

# Script generated for node Customer Curated
CustomerCurated_node1759427543830 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_curated", transformation_ctx="CustomerCurated_node1759427543830")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from accelerometer_trusted JOIN step_trainer_trusted ON accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime WHERE
    accelerometer_trusted.serialNumber IN (
        SELECT DISTINCT customer_curated.serialNumber
        FROM customer_curated
    );
'''
SQLQuery_node1759427562172 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":CustomerCurated_node1759427543830, "accelerometer_trusted":AccelerometerTrusted_node1759427431042, "step_trainer_trusted":StepTrainerTrusted_node1759427489365}, transformation_ctx = "SQLQuery_node1759427562172")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1759427562172, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759426488786", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1759427829227 = glueContext.getSink(path="s3://andrew-robinson2/machine-learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1759427829227")
AmazonS3_node1759427829227.setCatalogInfo(catalogDatabase="dev",catalogTableName="machine_learning_curated")
AmazonS3_node1759427829227.setFormat("json")
AmazonS3_node1759427829227.writeFrame(SQLQuery_node1759427562172)
job.commit()