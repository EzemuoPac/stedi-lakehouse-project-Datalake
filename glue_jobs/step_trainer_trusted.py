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

# Script generated for node StepTrainerLandingNode
StepTrainerLandingNode_node1766869021759 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingNode_node1766869021759")

# Script generated for node CustomerCuratedNode
CustomerCuratedNode_node1766869074486 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCuratedNode_node1766869074486")

# Script generated for node JoinOnSerialNumber
SqlQuery0 = '''
SELECT 
    step_trainer.sensorReadingTime,
    step_trainer.serialNumber,
    step_trainer.distanceFromObject
FROM step_trainer
INNER JOIN customer
ON step_trainer.serialNumber = customer.serialNumber
'''
JoinOnSerialNumber_node1766869158049 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":StepTrainerLandingNode_node1766869021759, "customer":CustomerCuratedNode_node1766869074486}, transformation_ctx = "JoinOnSerialNumber_node1766869158049")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinOnSerialNumber_node1766869158049, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766868979893", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766869281542 = glueContext.getSink(path="s3://stedi-lake-house-po/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766869281542")
AmazonS3_node1766869281542.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1766869281542.setFormat("glueparquet", compression="snappy")
AmazonS3_node1766869281542.writeFrame(JoinOnSerialNumber_node1766869158049)
job.commit()