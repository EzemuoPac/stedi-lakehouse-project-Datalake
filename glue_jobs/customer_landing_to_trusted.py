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

# Script generated for node CustomerLandingNode
CustomerLandingNode_node1766857589474 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLandingNode_node1766857589474")

# Script generated for node FilterPrivacy
SqlQuery0 = '''
SELECT *
   FROM myDataSource
   WHERE shareWithResearchAsOfDate IS NOT NULL
'''
FilterPrivacy_node1766857767022 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLandingNode_node1766857589474}, transformation_ctx = "FilterPrivacy_node1766857767022")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterPrivacy_node1766857767022, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766857107568", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766857947330 = glueContext.getSink(path="s3://stedi-lake-house-po/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766857947330")
AmazonS3_node1766857947330.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1766857947330.setFormat("glueparquet", compression="snappy")
AmazonS3_node1766857947330.writeFrame(FilterPrivacy_node1766857767022)
job.commit()