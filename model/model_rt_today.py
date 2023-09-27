import pandas as pd
from functools import reduce
import boto3
import os
from io import StringIO,BytesIO

s3 = boto3.client('s3')

obj = s3.get_object(Bucket='nyl-raw-data', Key='covid19Indiaraw/csv/ml_models/overwrite/nyl_rt_today.csv')
df_rt_today = pd.read_csv(obj['Body'])

#### Writing to S3

bucket = 'nyl-processed-data'
csv_buffer = StringIO()
filename = "model_rt_today.csv"
df_rt_today.to_csv(csv_buffer)
content = csv_buffer.getvalue()

k = "covid19indiaprocessed/csv/model_data_today/"+filename
s3.put_object(Bucket=bucket, Key=k, Body=content)
