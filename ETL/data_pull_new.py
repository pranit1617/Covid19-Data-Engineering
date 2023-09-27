import os
import boto3
import requests

s3 = boto3.client('s3')
s3_bucket = os.getenv('s3_bucket', 'nyl-raw-data')

### District_wise_daily
res = requests.get('https://api.covid19india.org/csv/latest/districts.csv')
s3.put_object( Bucket='nyl-raw-data',
Body=res.text,
Key='covid19indiaraw/csv/district_wise_daily/district_wise_daily.csv' )
