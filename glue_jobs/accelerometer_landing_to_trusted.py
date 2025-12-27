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
CustomerTrustedNode_node1766866199133 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1766866199133")

# Script generated for node AccelerometerLandingNode
AccelerometerLandingNode_node1766866316663 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingNode_node1766866316663")

# Script generated for node JoinOnEmail
SqlQuery0 = '''
SELECT 
    accelerometer.user,
    accelerometer.timeStamp,
    accelerometer.x,
    accelerometer.y,
    accelerometer.z
FROM accelerometer
INNER JOIN customer
ON accelerometer.user = customer.email
'''
JoinOnEmail_node1766866417777 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":CustomerTrustedNode_node1766866199133, "accelerometer":AccelerometerLandingNode_node1766866316663}, transformation_ctx = "JoinOnEmail_node1766866417777")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinOnEmail_node1766866417777, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766866001544", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766866534472 = glueContext.getSink(path="s3://stedi-lake-house-po/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766866534472")
AmazonS3_node1766866534472.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1766866534472.setFormat("glueparquet", compression="snappy")
AmazonS3_node1766866534472.writeFrame(JoinOnEmail_node1766866417777)
job.commit()