import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials, GcsBucket
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from dotenv import load_dotenv
from pyspark.sql.functions import col, concat, lit, to_timestamp, when
import urllib.request
#from google.cloud import storage, bigquery
import warnings
warnings.filterwarnings('ignore')

basedir=os.path.abspath(os.getcwd())
load_dotenv(os.path.join(basedir, '../.env'))


my_gcp_creds = GcpCredentials(
    service_account_info=os.environ.get("GCP_SERVICE_ACCOUNT_KEY_FILE_CONTENTS"), 
)
my_gcp_creds.save(name="my-gcp-creds-block", overwrite=True)


credentials_location = os.getenv('CREDENTIALS_LOCATION')

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()


@task
def extract_data():
    
    urllib.request.urlretrieve("https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv", "data.csv")
    
    schema = types.StructType([
        types.StructField('CRASH DATE', types.StringType(), True),
        types.StructField('CRASH TIME', types.StringType(), True),
        types.StructField('BOROUGH', types.StringType(), True),
        types.StructField('ZIP CODE', types.StringType(), True),
        types.StructField('LATITUDE', types.DoubleType(), True),
        types.StructField('LONGITUDE', types.DoubleType(), True),
        types.StructField('LOCATION', types.StringType(), True),
        types.StructField('ON STREET NAME', types.StringType(), True),
        types.StructField('CROSS STREET NAME', types.StringType(), True),
        types.StructField('OFF STREET NAME', types.StringType(), True), 
        types.StructField('NUMBER OF PERSONS INJURED', types.IntegerType(), True), 
        types.StructField('NUMBER OF PERSONS KILLED', types.IntegerType(), True), 
        types.StructField('NUMBER OF PEDESTRIANS INJURED', types.IntegerType(), True), 
        types.StructField('NUMBER OF PEDESTRIANS KILLED', types.IntegerType(), True), 
        types.StructField('NUMBER OF CYCLIST INJURED', types.IntegerType(), True), 
        types.StructField('NUMBER OF CYCLIST KILLED', types.IntegerType(), True), 
        types.StructField('NUMBER OF MOTORIST INJURED', types.IntegerType(), True), 
        types.StructField('NUMBER OF MOTORIST KILLED', types.IntegerType(), True), 
        types.StructField('CONTRIBUTING FACTOR VEHICLE 1', types.StringType(), True), 
        types.StructField('CONTRIBUTING FACTOR VEHICLE 2', types.StringType(), True), 
        types.StructField('CONTRIBUTING FACTOR VEHICLE 3', types.StringType(), True), 
        types.StructField('CONTRIBUTING FACTOR VEHICLE 4', types.StringType(), True), 
        types.StructField('CONTRIBUTING FACTOR VEHICLE 5', types.StringType(), True), 
        types.StructField('COLLISION_ID', types.IntegerType(), True), 
        types.StructField('VEHICLE TYPE CODE 1', types.StringType(), True), 
        types.StructField('VEHICLE TYPE CODE 2', types.StringType(), True), 
        types.StructField('VEHICLE TYPE CODE 3', types.StringType(), True), 
        types.StructField('VEHICLE TYPE CODE 4', types.StringType(), True), 
        types.StructField('VEHICLE TYPE CODE 5', types.StringType(), True)
    ])


    data = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('data.csv')


    #data = data.repartition(24)

    return data

@task()
def save_on_local(data, data_dir: str = 'data') -> Path:
    path = Path(data_dir)
    data.write.parquet(str(path), mode='overwrite')
    return path


@task()
def write_to_gcs(path: Path) -> None:
    gcp_credentials = GcpCredentials.load("my-gcp-creds-block")
    gcs_bucket = GcsBucket(
        bucket= os.getenv("GCP_BUCKET"),
        gcp_credentials=gcp_credentials
    )
    
    for file_name in os.listdir(path):
        file_path = path / file_name
        if file_path.suffix == '.parquet': 
            destination_path = f"data/{file_path.name}"
            gcs_bucket.upload_from_path(from_path=file_path, to_path=destination_path)

@task
def transform_data() :
    
    gcp_credentials = GcpCredentials.load("my-gcp-creds-block")
    gcs_bucket = GcsBucket(
        bucket= os.getenv("GCP_BUCKET"),
        gcp_credentials=gcp_credentials
    )
    gcs_path='data/'
    gcs_bucket.get_directory(from_path=gcs_path, local_path='./')
    data= spark.read.parquet(gcs_path)

    new_column_names = [col(c).alias(c.replace(' ', '_').lower()) for c in data.columns]
    data = data.select(*new_column_names)

    # Concatenate crash_date and crash_time into crash_datetime column
    data = data.withColumn("crash_datetime", concat(col("crash_date"), lit(" "), col("crash_time")))
    # Convert crash_datetime to timestamp
    data = data.withColumn("crash_datetime", to_timestamp("crash_datetime", "MM/dd/yyyy HH:mm"))
    data = data.drop('crash_date', 'crash_time')

    # Process zip_code column
    data = data.withColumn("zip_code", when(col("zip_code").isNull(), -1).otherwise(col("zip_code").cast("integer")))

    # return the transformed data  
    return data 


@task
def write_to_bigquery(
    data,
    project_id = os.getenv("PROJECT_ID"),
    dataset_id = os.getenv("DATASET_ID"),
    table_id = os.getenv("TABLE_ID"),
    temporary_bucket = os.getenv("GCP_BUCKET")) -> None:
    data.write \
      .format('bigquery') \
      .option('table', f'{project_id}.{dataset_id}.{table_id}') \
      .option('temporaryGcsBucket', temporary_bucket) \
      .save()
    

@flow()
def etl() -> None:
    
    data = extract_data()
    path = save_on_local(data)
    write_to_gcs(path)
    data = transform_data()
    write_to_bigquery(data)

if __name__ == '__main__':
    etl()
