from googleapiclient.discovery import build
import base64
import google.auth
import os


def run_job(data, context): 
     try :
          credentials, _ = google.auth.default()
          service = build('dataflow', 'v1b3', credentials=credentials, cache_discovery=False)
          PROJECT = os.environ["PROJECT"]
          STAGING_DATA_BUCKET = os.environ["STAGING_DATA_BUCKET"]
          TEMPLATE_PATH = os.environ["TEMPLATE_PATH"]

          template_path = "gs://{}".format(TEMPLATE_PATH)
          template_body = {
          "jobName" : "{}".format(data['name'].split('.')[0]),
          "parameters": {
               "temp_location": "gs://{}".format(TEMPLATE_PATH),
               "input_path": "gs://{0}/{1}".format(STAGING_DATA_BUCKET, data['name'])
               }
          }

          request = service.projects().templates().launch(projectId=PROJECT, gcsPath=template_path, body=template_body)
          response = request.execute()

          print('Response for file - {} :'.format(data['name']), response)
          
     except Exception as e:
          print('Error thrown for file {0} : '.format(data['name']), e)


# PROJECT = infra-test-1-315920
# STAGING_DATA_BUCKET = temp_data_pipeline_sosv
# TEMPLATE_PATH = data_pipeline_sosv/template/SOSV