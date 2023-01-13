import datetime
import os
from datetime import timedelta
import json
import logging
import boto3
from decimal import Decimal
import pandas as pd

from airflow.hooks.filesystem import FSHook
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.connection import Connection
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

args = {
    'owner': 'alena',
    'start_date': days_ago(1),
    'depends_on_past': False,
}
AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = 'http://localhost:4566'

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

boto3.setup_default_session(profile_name=AWS_PROFILE)

# s3 and dynamodb clients
s3_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)
dynamodb_client = boto3.client(
    "dynamodb", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)


def open_file():
    """
    Open files from the folder - you can change the path to files
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        df = pd.read_csv(file_name)
        df.to_csv(file_name, index=True)
        object_name = f'{file[7:]}'
        bucket = 'my-bucket'
        logger.info('Uploading file to S3 bucket in LocalStack...')
        upload_file(file_name, bucket, object_name)
        logger.info('Transforming and uploading file to S3 bucket in LocalStack...')
        create_dynamodb_table(object_name)
        add_dynamodb_table_item(object_name)


def open_transform_file():
    """
    Open and transform files
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        # transforming files using pyspark
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName('PySpark_Tutorial') \
            .getOrCreate()
        my_schema = StructType() \
            .add("departure", TimestampType(), True) \
            .add("return", TimestampType(), True) \
            .add("departure_id", StringType(), True) \
            .add("departure_name", StringType(), True) \
            .add("return_id", FloatType(), True) \
            .add("return_name", StringType(), True) \
            .add("distance (m)", FloatType(), True) \
            .add("duration (sec.)", FloatType(), True) \
            .add("avg_speed (km/h)", FloatType(), True) \
            .add("departure_latitude", FloatType(), True) \
            .add("departure_longitude", FloatType(), True) \
            .add("return_latitude", FloatType(), True) \
            .add("return_longitude", FloatType(), True) \
            .add("Air temperature (degC)", FloatType(), True)
        new_df = spark.read.format("csv") \
            .option("header", True) \
            .schema(my_schema) \
            .load(file_name)
        table1 = new_df.groupBy("departure_name").count().withColumnRenamed("count", "count_taken")
        table2 = new_df.groupBy("return_name").count().withColumnRenamed("count", "count_returned")
        result_table = table1.join(table2, table1["departure_name"] == table2["return_name"]).select('departure_name','count_taken','count_returned')
        result_table.toPandas().to_csv(f'{file[7:]}_transformed')
        new_file = os.path.join(path, f'{file[7:]}_transformed')
        object_name = f'{file[7:]}_transformed'
        bucket = 'my-bucket'
        upload_file(new_file, bucket, object_name)


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to a S3 bucket.
    """
    try:
        if object_name is None:
            object_name = os.path.basename(file_name)
        response = s3_client.upload_file(
            file_name, bucket, object_name)
    except ClientError:
        logger.exception('Could not upload file to S3 bucket.')
        raise
    else:
        return response


def create_dynamodb_table(table_name):
    """
    Create a DynamoDB table.
    """
    try:
        response = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            })

    except ClientError:
        logger.exception('Could not create the table.')
        raise
    else:
        return response


def add_dynamodb_table_item(table_name):
    """
    Add raw and transformed data to Dynamodb
    """
    df = pd.read_csv(s3_client.get_object(Bucket='my-bucket', Key=table_name).get("Body"))
    df.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
    df['departure'] = df['departure'].astype('datetime64[ns]')
    df['month_day'] = df['departure'].dt.to_period('D')
    df['avg_distance_by_month'] = df['distance (m)'].mean()
    df['avg_distance_by_day'] = df.groupby('month_day')['distance (m)'].transform('mean')
    df['avg_duration_by_month'] = df['duration (sec.)'].mean()
    df['avg_duration_by_day'] = df.groupby('month_day')['duration (sec.)'].transform('mean')
    df['avg_speed_by_month'] = df['avg_speed (km/h)'].mean()
    df['avg_speed_by_day'] = df.groupby('month_day')['avg_speed (km/h)'].transform('mean')
    df['avg_temperature_by_month'] = df['Air temperature (degC)'].mean()
    df['avg_temperature_by_day'] = df.groupby('month_day')['Air temperature (degC)'].transform('mean')
    df['departure'] = df['departure'].astype('str')
    df1 = df.filter(['id', 'departure', 'return', 'departure_id', 'departure_name', 'return_id', 'return_name',
                     'distance (m)', 'duration (sec.)', 'avg_speed (km/h)','departure_latitude', 'departure_longitude',
                     'return_latitude', 'return_longitude', 'Air temperature (degC)', 'avg_distance_by_month',
                     'avg_distance_by_day', 'avg_duration_by_month',
                     'avg_duration_by_day', 'avg_speed_by_month', 'avg_speed_by_day',
                     'avg_temperature_by_month', 'avg_temperature_by_day'], axis=1)
    data = json.loads(df1.to_json(orient="records"), parse_float=Decimal)
    my_dict = {'item': data, 'table': table_name}
    dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
    dynamotable = dynamodb_resource.Table(table_name)
    for record in my_dict['item']:
        dynamotable.put_item(Item=record)


with DAG(
    'localstack_dag',
    default_args=args,
    schedule_interval='@once',
    catchup=False
) as dag:
    task1 = PythonOperator(task_id='load_to_bucket_and_dynamodb',
                           python_callable=open_file,
                           provide_context=True)
    task2 = PythonOperator(task_id='transform_files_and_load_metrics_to_bucket',
                           python_callable=open_transform_file,
                           provide_context=True)

    task1>>task2