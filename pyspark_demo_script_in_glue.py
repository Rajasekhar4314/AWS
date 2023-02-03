
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

empDf = glueContext.create_dynamic_frame.from_catalog(
        database="b1-glue-catalog-db",
        table_name="s3fileb1_athena_input_data"
        )

empDf.printSchema()
sparkEmpDf = empDf.toDF()
sparkEmpDf.show()
df2=sparkEmpDf.select("employee_id")
df2.show()

# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# job.commit()


## For Glue servie permission(ADD Glue role)
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GetTables",
      "Effect": "Allow",
      "Action": [
        "glue:CreateTable",
        "glue:GetTable",
        "glue:GetTables",                
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchDeleteTable",
        "glue:GetTableVersion",
        "glue:GetTableVersions",
        "glue:DeleteTableVersion",
        "glue:BatchDeleteTableVersion",
        "glue:CreatePartition",
        "glue:BatchCreatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchDeletePartition"        
      ],
      "Resource": "*"
    }
  ]
}
