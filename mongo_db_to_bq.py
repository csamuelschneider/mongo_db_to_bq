from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
import json
import mongo_db_to_bq.config as config

# MongoDB connection
uri = config.uri
mongo_client = MongoClient(uri)
db = mongo_client["sample_mflix"]
collec = db["users"]

#Setup BigQuery variables and GCP credential
credential_path = r"projeto_2025\apache-beam-curso-3127e2e5d648.json"
credentials = service_account.Credentials.from_service_account_file(credential_path)
project_id  = "apache-beam-curso"
dataset_id = "raw_sample_mflix_mongosb"
table_id = "comments"
table_path = f"{project_id}.{dataset_id}.{table_id}"
bq_client = bigquery.Client(credentials=credentials, project="apache-beam-curso")
table_ref = bq_client.dataset(dataset_id).table(table_id)
temp_table = "temp_table"
temp_table_id = f"{project_id}.{dataset_id}.{temp_table}"


# Fetch data (convert MongoDB documents to a list of dictionaries)
mongo_data = []
for doc in collec.find():
    doc_id = str(doc["_id"])  # Convert ObjectId to string
    doc.pop("_id")  # Remove _id from the document
    mongo_data.append({"id": doc_id, "data": json.dumps(doc)})  # Store as JSON string]

#BQ schema
# schema = [
#     bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
#     bigquery.SchemaField("data", "STRING", mode="REQUIRED"),
# ]

try:
    bq_client.get_table(table_ref)
except Exception:
    table = bigquery.Table(table_ref)  #table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)

#temp table create and load
create_temp_table = f"""
    CREATE OR REPLACE TEMP TABLE temp_table AS
    id STRING,
    data STRING
"""
create_temp_table_job = bq_client.query(create_temp_table)
json_data = [{"id": "xyz", "data": json.dumps({"name": "Louis", "age": 60})}]
job = bq_client.load_table_from_json(json_data, temp_table_id)
job.result()

# Load data into BigQuery

merge_query = f"""

MERGE `{project_id}.{dataset_id}.{table_id}` AS target
USING `{project_id}.{dataset_id}.{temp_table}` AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
  INSERT (id, data)
  VALUES (source.id, source.data)
WHEN MATCHED THEN
    UPDATE SET target.data = source.data;
"""

query_job = bq_client.query(merge_query)
query_job.result()
# job = bq_client.load_table_from_json(mongo_data, table_path)
# job.result() 


