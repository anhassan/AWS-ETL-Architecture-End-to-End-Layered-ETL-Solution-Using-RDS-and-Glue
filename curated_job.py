import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
# Creating glue context from reading from glue catalog database
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql import SparkSession

# Creating the spark session for using spark transformations and actions
spark = SparkSession \
    .builder \
    .getOrCreate()


from pyspark.sql.functions import *

# Utility function to get a table from specified catalog database
def get_table(catalog_database_name,catalog_table):
    df = glueContext.create_dynamic_frame.from_catalog(
             database=catalog_database_name,
             table_name=catalog_table).toDF().where(lit(True))
    return df

# Utility function to persist data into the required layer
def persist_data(df,bucket_name,layer,dataset_name):
    df.write\
       .mode("overwrite")\
       .save("s3://{}/{}/{}".format(bucket_name,layer,dataset_name))
    print("Loaded {} rows into the {} layer of datalake".format(df.count(),layer))
    

# Getting the required tables from the raw layer
catalog_database_name = "enterprise_db"
raw_customers = get_table(catalog_database_name,"raw_enterprise_db_dbo_customers")
raw_orders = get_table(catalog_database_name,"raw_enterprise_db_dbo_orders")


# Specifying the logic for curation

threshold_amount = 60
join_cols = ["customerid"]
filter_cols = ["customername"]

curated_data = raw_orders.where(col("amount")<=threshold_amount)\
                                           .withColumnRenamed("recordtime","orderrecordtime")\
                                          .join(raw_customers,join_cols)\
                                          .drop(*filter_cols)


# Persisting data into the curated layer

bucket_name = "etl-pipeline-bucket"
layer, dataset_name = "curated","customersorders"
persist_data(curated_data,bucket_name,layer,dataset_name)

