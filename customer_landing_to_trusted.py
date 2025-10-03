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

# Script generated for node Customer Landing S3 Bucket
CustomerLandingS3Bucket_node1759505581336 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://andrew-robinson2/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingS3Bucket_node1759505581336")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SharewithResearch_node1759346129451 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLandingS3Bucket_node1759505581336}, transformation_ctx = "SharewithResearch_node1759346129451")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1759346129451, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759346083080", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1759346197176 = glueContext.getSink(path="s3://andrew-robinson2/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1759346197176")
CustomerTrusted_node1759346197176.setCatalogInfo(catalogDatabase="dev",catalogTableName="customer_trusted")
CustomerTrusted_node1759346197176.setFormat("json")
CustomerTrusted_node1759346197176.writeFrame(SharewithResearch_node1759346129451)
job.commit()