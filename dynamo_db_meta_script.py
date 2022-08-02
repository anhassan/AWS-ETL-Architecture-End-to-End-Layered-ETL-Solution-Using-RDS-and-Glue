
import boto3

# A utility function to create schema less dynamo db table with a sort key column
def create_dynamo_db_table(table_name,region):
    dd_client = boto3.client('dynamodb',region_name=region)
    dd_resource = boto3.resource('dynamodb',region_name=region)
    dd_tables = dd_client.list_tables()['TableNames']
    
    if table_name in dd_tables:
        print("Table : {} already exists".format(table_name))
    else:
        dd_resource.create_table(
            TableName = table_name,
            KeySchema =[
                {
                    'AttributeName' : 'Source',
                    'KeyType' : 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName' : 'Source',
                    'AttributeType' : 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits':1,
                'WriteCapacityUnits':1
            }
        )
        print("Table : {} created successfully".format(table_name))
        

# A utility function to populate given dynamo db table with given items using batch writer
def populate_dynamo_db_table(table_name,items,region):
    dd_resource = boto3.resource('dynamodb',region_name=region)
    dd_table = dd_resource.Table(table_name)
    
    with dd_table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
    print("Added {} items in Table : {}".format(len(items),table_name))
    


# Creating a dynamo db table if not exists for storing etl metadata
table_name = "pipelineconfiguration"
region = "us-west-2"
create_dynamo_db_table(table_name,region)


# Populating dynamo db table with etl metadata
sources = ["ingest_rds_raw_job","raw_layer_crawler","curated_job","curated_layer_crawler","semantic_job","semantic_layer_crawler"]
targets = ["raw_layer_crawler","curated_job","curated_layer_crawler","semantic_job","semantic_layer_crawler","None"]
target_types = ["Crawler","Job","Crawler","Job","Crawler","None"]

items = [{"Source":source,"Target" : targets[ind],"TargetType":target_types[ind]} for ind,source in enumerate(sources)]
populate_dynamo_db_table(table_name,items,region)

