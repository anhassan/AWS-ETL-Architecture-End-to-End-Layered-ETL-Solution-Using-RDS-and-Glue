
# Creating glue context from reading from glue catalog database
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


from pyspark.sql import SparkSession

# Creating the spark session for using spark transformations and actions
spark = SparkSession \
    .builder \
    .getOrCreate()


import boto3

# A utility function to get the watermark value for a table from S3
def get_cdc_watermark(bucket_name,table_name):
    try:
        return spark.read\
                             .text("s3://{}/cdc_marks/{}.txt".format(bucket_name,table_name))\
                             .collect()[0]["value"]
    except Exception as error:
        return None

# A utility function to persist watermark value for a table to S3
def persist_cdc_watermark(df,bucket_name,table_name):
    s3_client = boto3.client("s3")
    watermark_time = df.agg({"recordtime":"max"})\
                                     .collect()[0]["max(recordtime)"]
    s3_client.put_object(Body=bytes("{}".format(watermark_time),'utf-8'),Bucket=bucket_name,Key="cdc_marks/{}.txt".format(table_name))
    
# A utility function to get all the tables in the catalog database
def get_catalog_tables(catalog_database_name,region):
    glue_client = boto3.client("glue",region_name=region)
    response = glue_client.get_tables(DatabaseName=catalog_database_name)
    datalake_layers = ["raw","curated","semantic"]
    catalog_tables_list = [table_meta["Name"] for table_meta in response["TableList"] if not any(layer in table_meta["Name"] for layer in datalake_layers)]
    return catalog_tables_list


from pyspark.sql.functions import *

# Main function to ingest all the 
def ingest_rds_to_raw(catalog_database_name,bucket_name,region="us-west-2"):
    catalog_tables = get_catalog_tables(catalog_database_name,region)
    
    for catalog_table in catalog_tables:
        print("Reading Table : {} from RDS SQL Server Instance".format(catalog_table))
        df = glueContext.create_dynamic_frame.from_catalog(
             database=catalog_database_name,
             table_name=catalog_table).toDF().where(lit(True))
        
        print("Reading previous CDC Watermark for Table : {} ".format(catalog_table))
        watermark_time = get_cdc_watermark(bucket_name,catalog_table)
        if watermark_time is not None:
            df = df.where(col("recordtime")>watermark_time )
        
        if df.count() == 0 :
            print("No incremental data found for Table : {} therefore no ingestion takes place".format(catalog_table))
        else:
            print("Writing current CDC Watermark for Table : {} ".format(catalog_table))
            persist_cdc_watermark(df,bucket_name,catalog_table)
            
            print("Ingesting incremental data for Table : {} into raw zone ".format(catalog_table))
            df.write\
               .mode("append")\
               .save("s3://{}/raw/{}".format(bucket_name,catalog_table))
            print("{} number of rows ingested incrementally for Table : {}".format(df.count(),catalog_table))
            

# Parameters for Ingestion Job
catalog_database_name = "enterprise_db"
bucket_name = "etl-pipeline-bucket"


# Ingesting all the tables from catalog to S3 raw zone
ingest_rds_to_raw(catalog_database_name,bucket_name)

