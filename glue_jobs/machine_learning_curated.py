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

# Script generated for node StepTrainerTrustedNode
StepTrainerTrustedNode_node1766870203028 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedNode_node1766870203028")

# Script generated for node AccelerometerTrustedNode
AccelerometerTrustedNode_node1766870293579 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1766870293579")

# Script generated for node JoinOnTimestamp
SqlQuery0 = '''
SELECT 
    step_trainer.sensorReadingTime,
    step_trainer.serialNumber,
    step_trainer.distanceFromObject,
    accelerometer.user,
    accelerometer.x,
    accelerometer.y,
    accelerometer.z,
    accelerometer.timeStamp
FROM step_trainer
INNER JOIN accelerometer
ON step_trainer.sensorReadingTime = accelerometer.timeStamp
'''
JoinOnTimestamp_node1766870352410 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":StepTrainerTrustedNode_node1766870203028, "accelerometer":AccelerometerTrustedNode_node1766870293579}, transformation_ctx = "JoinOnTimestamp_node1766870352410")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinOnTimestamp_node1766870352410, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766870177552", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766870450306 = glueContext.getSink(path="s3://stedi-lake-house-po/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766870450306")
AmazonS3_node1766870450306.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1766870450306.setFormat("glueparquet", compression="snappy")
AmazonS3_node1766870450306.writeFrame(JoinOnTimestamp_node1766870352410)
job.commit()