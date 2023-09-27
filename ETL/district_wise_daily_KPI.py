import pandas as pd
import boto3
import os
from io import StringIO
# import s3fs

s3 = boto3.client('s3')
s3_bucket = os.getenv('s3_bucket','nyl-raw-data')

obj = s3.get_object(Bucket='nyl-processed-data', Key='covid19indiaprocessed/csv/district_wise_daily_etl/district_wise_daily_etl.csv')

df = pd.read_csv(obj['Body'])

#### Transformation for KPI
# Pivoting confirmed_daily,recovered_daily and deaceased_daily column to get corresponding values as per cities and dates

df = pd.melt(df, id_vars =['City_new','City_needed','Date'], value_vars=['Confirmed_daily','Recovered_daily','Deceased_daily'])

#### Writing to S3

bucket = 'nyl-processed-data'
csv_buffer = StringIO()
filename = "district_wise_daily_KPI.csv"
df.to_csv(csv_buffer)
content = csv_buffer.getvalue()

k = "covid19indiaprocessed/csv/district_wise_daily_kpi/"+filename
s3.put_object(Bucket=bucket, Key=k, Body=content)
