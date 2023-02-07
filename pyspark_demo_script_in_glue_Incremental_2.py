import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
empdf1 = glueContext.create_dynamic_frame.from_catalog(
        database = "metadata-db",
        table_name = "data_first_bucket_input",
        transformation_ctx = "s3_input_data_1"
        )
empdf2 = glueContext.create_dynamic_frame.from_catalog(
        database = "metadata-db",
        table_name = "data_second_bucket_data",
        transformation_ctx = "s3_input_data_2"
        )

empdf1.printSchema()
empdf2.printSchema()
sparkdf1 = empdf1.toDF()
sparkdf2 = empdf2.toDF()
sparkdf1.show()
sparkdf2.show()
print(sparkdf1.count())
print(sparkdf2.count())
job.commit()

#job = Job(glueContext)
#job.init(args['JOB_NAME'], args)
#job.commit()


## for connecting Eventbridgw with cloudwatch

CloudWatchEventsBuiltInTargetExecutionAccess
CloudWatchEventsInvocationAccess
