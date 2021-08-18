import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import datetime
import json


PROJECT = 'infra-test-1-315920'
BUCKET = 'data_pipeline_sosv'
BQ_DATASET = 'DataFlowTest'
final_data = []


class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_path', type=str, 
        help='Path to file, which needs to be processed')

def process_data(data, cur_ts):
    # print(data)
 
    data['timestamp'] = cur_ts
    # print(json.dumps([transformed_data]))
    # print(type(json.dumps([transformed_data])))
    return ([data])

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    cur_ts = datetime.datetime.now()
    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    pipeline=beam.Pipeline(options=pipeline_options)

    (pipeline
    # | "Reading from file" >> beam.io.ReadFromText("gs://data_pipeline_sosv/ndjson_data/scaneer_ndjson.json")
    | "Reading from file" >> beam.io.ReadFromText(dataflow_options.input_path)
    | 'To Dict' >> beam.Map(lambda x : json.loads(x))
    | "Process data" >> beam.ParDo(process_data,cur_ts)
    | 'Write to BQ Table' >> beam.io.WriteToBigQuery(table = "scanner_data",
    dataset = 'DataFlowTest',
    project = 'infra-test-1-315920',
    schema=beam.io.gcp.bigquery.SCHEMA_AUTODETECT,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    pipeline.run().wait_until_finish()

if __name__ == '__main__':
  run()

# python3 data_pipeline_SOSV.py --runner DirectRunner --project infra-test-1-315920 --staging_location gs://data_pipeline_sosv/ --temp_location gs://data_pipeline_sosv/temp/ --region us-central1

# data_pipeline_SOSV.py --runner DataflowRunner --project infra-test-1-315920 --staging_location gs://data_pipeline_sosv/ --temp_location gs://data_pipeline_sosv/temp/ --region us-central1 --template_location gs://data_pipeline_sosv/template/SOSV