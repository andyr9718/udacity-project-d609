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

# Script generated for node Customer Curated
CustomerCurated_node1759425974109 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_curated", transformation_ctx="CustomerCurated_node1759425974109")

# Script generated for node Step Trainer Landing S3 Bucket
StepTrainerLandingS3Bucket_node1759505928657 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://andrew-robinson2/step-trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLandingS3Bucket_node1759505928657")

# Script generated for node Join
SqlQuery0 = '''
SELECT
    *
FROM
    step_trainer_landing
WHERE
    serialNumber IN (
        SELECT DISTINCT serialNumber
        FROM customer_curated
    );
'''
Join_node1759426802685 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":CustomerCurated_node1759425974109, "step_trainer_landing":StepTrainerLandingS3Bucket_node1759505928657}, transformation_ctx = "Join_node1759426802685")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1759426802685, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759425895689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1759426060272 = glueContext.getSink(path="s3://andrew-robinson2/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1759426060272")
StepTrainerTrusted_node1759426060272.setCatalogInfo(catalogDatabase="dev",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1759426060272.setFormat("json")
StepTrainerTrusted_node1759426060272.writeFrame(Join_node1759426802685)
job.commit()