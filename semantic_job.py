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


from pyspark.sql.functions import *

# Utility function to get a curated table from specified catalog database
def get_table(catalog_database_name,catalog_table):
    if "curated" in catalog_table:
        df = glueContext.create_dynamic_frame.from_catalog(
                 database=catalog_database_name,
                 table_name=catalog_table).toDF().where(lit(True))
        return df
    else:
        print ("Please enter a valid curated table. Table : {} does not belong to curated layer".format(catalog_table))

# Utility function to persist data into the semantic layer
def persist_data(df,bucket_name,layer,dataset_name):
    if layer == "semantic":
        df.write\
           .mode("overwrite")\
           .save("s3://{}/{}/{}".format(bucket_name,layer,dataset_name))
        print("Loaded {} rows into the {} layer of datalake".format(df.count(),layer))
    else:
        print("Please ensure the persistance layer is semantic. Layer used : {}".format(layer))
    

# Getting the required tables from the curated layer
catalog_database_name = "enterprise_db"
curated_data = get_table(catalog_database_name,"curated_customersorders")


# Specifying the logic for the semantic layer

semantic_data = curated_data.groupBy("customerregion")\
                                                .agg(sum("amount").alias("regional_sum"),\
                                                        min("amount").alias("regional_min"),\
                                                        max("amount").alias("regional_max"),\
                                                        avg("amount").alias("regional_avg"))


# Persisting data into the curated layer

bucket_name = "etl-pipeline-bucket"
layer, dataset_name = "semantic","customersordersaggregated"
persist_data(semantic_data,bucket_name,layer,dataset_name)

