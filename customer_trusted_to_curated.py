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

# Script generated for node Customer Trusted
CustomerTrusted_node1759349880774 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1759349880774")

# Script generated for node Accelerometer Landing to Trusted
AccelerometerLandingtoTrusted_node1759349440434 = glueContext.create_dynamic_frame.from_catalog(database="dev", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingtoTrusted_node1759349440434")

# Script generated for node Join
Join_node1759349717579 = Join.apply(frame1=AccelerometerLandingtoTrusted_node1759349440434, frame2=CustomerTrusted_node1759349880774, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1759349717579")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1759410868568 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1759349717579}, transformation_ctx = "DropFieldsandDuplicates_node1759410868568")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1759410868568, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759349434628", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1759350096037 = glueContext.getSink(path="s3://andrew-robinson2/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1759350096037")
CustomerCurated_node1759350096037.setCatalogInfo(catalogDatabase="dev",catalogTableName="customer_curated")
CustomerCurated_node1759350096037.setFormat("json")
CustomerCurated_node1759350096037.writeFrame(DropFieldsandDuplicates_node1759410868568)
job.commit()