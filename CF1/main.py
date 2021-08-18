import csv
import json
import ndjson
import sys
from google.cloud import storage
import io

storage_client = storage.Client()
bucket = storage_client.bucket('data_pipeline_sosv')
project = "infra-test-1-315920"
bucket_name = 'data_pipeline_sosv'
dataset = 'data_pipeline_sosv_dataset'
table = 'data_pipeline_sosv_table'
destination_blob_name = 'scaneer_ndjson'

def upload_blob(bucket_name, data, destination_blob_name):
  destination_blob_name = destination_blob_name+'.json'
  storage_client = storage.Client()
  bucket = storage_client.bucket('temp_data_pipeline_sosv')
  blob = bucket.blob('ndjson_data/'+destination_blob_name)
  blob.upload_from_string( data=data, content_type='application/json')
  print("FILE UPLOADED SUCCESFULLY",destination_blob_name)

def hello_gcs(event, context):
  blob = bucket.get_blob(event['name'])
  bd = blob.download_as_string().decode("utf-8") 
  reader = csv.DictReader(io.StringIO(bd))
  json_data = json.dumps(list(reader))
  json_data = json.loads(json_data)
  for i in range(0, len(json_data)):
    b = json_data[i]
    del b[""]

  output = ndjson.dumps(json_data)
  upload_blob(bucket_name, output, destination_blob_name)