import json
import boto3
import os
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    
    print("Lambda Function Got Triggered")
    
    # Defining parameters for the lambda job
    source = ""
    region = "us-west-2"
    meta_table_name = "pipelineconfiguration"
    
    # Reading configurations from the environment variables
    AWS_ACCESS_KEY_ID = os.environ["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = os.environ["aws_secret_access_key"]
    
    
    if event["detail-type"] == "Glue Job State Change":
        # Get job details from event bridge events
        glue_job_name = event["detail"]["jobName"]
        glue_job_status = event["detail"]["state"]
        
        # Log job details for reference
        print("Status of Glue Job : {} Changed".format(glue_job_name))
        print("Status of Job : {}".format(glue_job_status))
        
        if glue_job_status == "SUCCEEDED":
            source = glue_job_name
        else:
            raise Exception("Glue Job : {} Failed".format(glue_job_name))
    
    if event["detail-type"] == "Glue Crawler State Change":
        # Get crawler details from event bridge events
        glue_crawler_name = event["detail"]["crawlerName"]
        glue_crawler_status = event["detail"]["state"]
        
        # Log crawler details for reference
        print("Status of Crawler : {} Changed".format(glue_crawler_name))
        print("Status of Crawler : {}".format(glue_crawler_status))
        
        if glue_crawler_status == "Succeeded":
            source = glue_crawler_name
        else:
            raise Exception("Glue Crawler : {} Failed".format(glue_crawler_name))
            
    # Reading the ETL metadata information from dynamo db table
    dd_client = boto3.resource("dynamodb",region_name=region,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    dd_table = dd_client.Table(meta_table_name)
    response = dd_table.query(KeyConditionExpression=Key('Source').eq(source))
    target = response["Items"][0]["Target"]
    target_type = response["Items"][0]["TargetType"]
    
    # Taking required action according to the event type
    glue_client = boto3.client("glue",region_name=region,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    if target_type == "Crawler":
        glue_client.start_crawler(Name=target)
        print("Started Crawler : {}".format(target))
    if target_type == "Job":
        glue_client.start_job_run(JobName=target)
        print("Started Glue Job : {}".format(target))
