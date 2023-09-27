import pandas as pd
from functools import reduce
import boto3
import os
from io import StringIO,BytesIO
import numpy as np
from datetime import datetime,timedelta

s3 = boto3.client('s3')
s3_bucket = os.getenv('s3_bucket','nyl-raw-data')

obj_1 = s3.get_object(Bucket='nyl-raw-data', Key='covid19Indiaraw/csv/ml_models/overwrite/nyl_doubling_rate.csv')
df_double = pd.read_csv(obj_1['Body'])

obj_2 = s3.get_object(Bucket='nyl-raw-data', Key='covid19Indiaraw/csv/ml_models/overwrite/nyl_rt.csv')
df_rt = pd.read_csv(obj_2['Body'])

obj_3 = s3.get_object(Bucket='nyl-raw-data', Key='covid19Indiaraw/csv/ml_models/overwrite/nyl_sir.csv')
df_sir = pd.read_csv(obj_3['Body'])

#### Creating Key for merging all 3 dataframes

df_double['Key'] = df_double['region'] + '_' + df_double['date']
df_rt['Key'] = df_rt['region'] + '_' + df_rt['date']
df_sir['Key'] = df_sir['region'] + '_' + df_sir['date']

### Renaming rt columns

df_rt.rename(columns ={
    'mean_x':'rt_mean_x',
    'lower_x':'rt_lower_x',
    'upper_x':'rt_upper_x',
    'mean':'rt_mean_y',
    'lower':'rt_lower_y',
    'upper':'rt_upper_y'}, inplace = True)

#### merging csvs

df_combined = reduce(lambda x,y: pd.merge(x,y, on='Key', how='outer'), [df_double, df_rt, df_sir])

#### Creating City_new column for cities needed to use and renaming cities as per original names

df_combined['City_new'] = df_combined['region']
df_combined.loc[df_combined['City_new'] == 'Bengaluru Urban','City_new'] = 'Bengaluru'
df_combined.loc[df_combined['City_new'] == 'Ernakulam','City_new'] = 'Cochin'
# df_combined.loc[df_combined['City_new'] == 'Gurugram','City_new'] = 'Gurgaon'
df_combined.loc[df_combined['City_new'] == 'Mysuru','City_new'] = 'Mysore'
df_combined.loc[df_combined['City_new'] == 'Thiruvananthapuram','City_new'] = 'Trivandrum'
df_combined.loc[df_combined['region'] == 'Telangana','City_new'] = 'Telangana'

#### Dropping dupicate columns

df_combined = df_combined.drop(columns=['region_x','region_y','date_x','date_y'])

#### 90_day_tagging
# Tagging 90 days for visualization in Tableau in which half dates are tagged for actual records and other half is tagged for predicted/forecast data

d = datetime.now() + timedelta(days=1)
df_combined['date']=pd.to_datetime(df_combined['date'])
df_combined['90_day_tagging_actual_and_prediction'] = d-df_combined['date']
df_combined['90_day_tagging_actual_and_prediction'] = df_combined['90_day_tagging_actual_and_prediction'].astype(str)
df_combined['90_day_tagging_actual_and_prediction'] = df_combined['90_day_tagging_actual_and_prediction'].str.split(" ", 0, expand=True)
df_combined['90_day_tagging_actual_and_prediction'] = df_combined['90_day_tagging_actual_and_prediction'].astype(int)
df_combined.loc[df_combined['90_day_tagging_actual_and_prediction']>45,'90_day_tagging_actual_and_prediction']=np.nan

#### Creating log columns for active and total confirmed cases

df_combined['active_log'] = np.log(df_combined['active'])
df_combined['totalconfirmed_log'] = np.log(df_combined['totalconfirmed'])

#### Reordering columns

cols_new = ['Key','region','date','City_new','doubling_time','rt_mean_x','rt_lower_x','rt_upper_x','rt_mean_y','rt_lower_y','rt_upper_y',
           'dailyconfirmed_mean','dailyconfirmed_lower','dailyconfirmed_upper','totalconfirmed_lower', 'totalconfirmed_mean',
            'totalconfirmed_upper','active_lower','active_mean','active_upper','recovered_lower','recovered_mean','recovered_upper',
            'death_lower','death_mean','death_upper','totalconfirmed','active','totalrecovered','totaldeceased','dailyconfirmed',
	   'dailyrecovered','dailydeceased','active_log','totalconfirmed_log','90_day_tagging_actual_and_prediction']
df_combined = df_combined.loc[:,cols_new]

#### Writing to S3

bucket = 'nyl-processed-data'
csv_buffer = StringIO()
filename = "combined_model_data.csv"
df_combined.to_csv(csv_buffer)
content = csv_buffer.getvalue()

k = "covid19indiaprocessed/csv/combined_model_data/"+filename
s3.put_object(Bucket=bucket, Key=k, Body=content)
