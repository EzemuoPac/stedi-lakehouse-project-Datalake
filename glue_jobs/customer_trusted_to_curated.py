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

# Script generated for node CustomerTrustedNode
CustomerTrustedNode_node1766867677766 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1766867677766")

# Script generated for node AccelerometerTrustedNode
AccelerometerTrustedNode_node1766868141337 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1766868141337")

# Script generated for node JoinAndSelectCustomers
SqlQuery0 = '''
SELECT DISTINCT
    customer.serialNumber,
    customer.shareWithPublicAsOfDate,
    customer.birthDay,
    customer.registrationDate,
    customer.shareWithResearchAsOfDate,
    customer.customerName,
    customer.email,
    customer.lastUpdateDate,
    customer.phone,
    customer.shareWithFriendsAsOfDate
FROM customer
INNER JOIN accelerometer
ON customer.email = accelerometer.user
'''
JoinAndSelectCustomers_node1766868237171 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":CustomerTrustedNode_node1766867677766, "accelerometer":AccelerometerTrustedNode_node1766868141337}, transformation_ctx = "JoinAndSelectCustomers_node1766868237171")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinAndSelectCustomers_node1766868237171, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766867235344", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766868336004 = glueContext.getSink(path="s3://stedi-lake-house-po/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766868336004")
AmazonS3_node1766868336004.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1766868336004.setFormat("glueparquet", compression="snappy")
AmazonS3_node1766868336004.writeFrame(JoinAndSelectCustomers_node1766868237171)
job.commit()