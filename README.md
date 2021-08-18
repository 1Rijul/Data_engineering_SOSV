# Data_engineering_SOSV

Creating a ETL pipeline using following services:
1. Google Cloud Storage
2. BigQuery
3. Cloud Functions
4. Cloud DataFlow

The cloud infrastruce provide below is secure, scalable, high-performance, efficient, elastic, highly available, fault-tolerant, and recoverable.

The Pipeline is deployed on Google cloud platform and the basic functionality is defined in the following architecture:


<img width="659" alt="Screen Shot 2021-08-18 at 6 10 28 PM" src="https://user-images.githubusercontent.com/21199829/129978851-e9fe2fda-5f8f-40f9-b1f8-d3d316d9e3e8.png">


Here **GCS** acts as a Data Lake.

A data lake is a system or repository of data stored in its natural/raw format, usually object blobs or files. A data lake is usually a single store of data including raw copies of source system data, sensor data, social data etc., and transformed data used for tasks such as reporting, visualization, advanced analytics and machine learning. A data lake can include structured data from relational databases (rows and columns), semi-structured data (CSV, logs, XML, JSON), unstructured data (emails, documents, PDFs) and binary data (images, audio, video). A data lake can be established "on premises" (within an organization's data centers) or "in the cloud" (using cloud services from vendors such as Amazon, Microsoft, or Google).

**BigQuery** acts as a sink, which is a fully-managed, serverless data warehouse that enables scalable analysis over petabytes of data. It is a Platform as a Service (PaaS) that supports querying using ANSI SQL. It also has built-in machine learning capabilities. 

To deploy the Dataflow pipeline, run the following command:

```
python data_pipeline_SOSV.py --runner DataflowRunner --project infra-test-1-315920 --staging_location gs://data_pipeline_sosv/ --temp_location gs://data_pipeline_sosv/temp/ --region us-central1 --template_location gs://data_pipeline_sosv/template/SOSV
```
