import pandas as pd
import numpy as np
import boto3
import os
from io import StringIO
from datetime import date,datetime,timedelta

s3 = boto3.client('s3')
s3_bucket = os.getenv('s3_bucket','nyl-raw-data')

obj = s3.get_object(Bucket='nyl-raw-data', Key='covid19indiaraw/csv/district_wise_daily/district_wise_daily.csv')

df = pd.read_csv(obj['Body'])

#### Active cases

df['Active'] = df['Confirmed'] - df['Recovered'] - df['Deceased']

#### Reordering columns

cols_new = ['Date','State','District','Confirmed','Recovered','Deceased','Active','Other','Tested']
df = df.loc[:,cols_new]

#### Renaming Cities 
# City_new column is used to store the cities needed to be worked on

df['City_new'] = df['District']
df['City_new'].loc[df['City_new'] == 'Bengaluru Urban'] = 'Bengaluru'
df['City_new'].loc[df['City_new'] == 'Ernakulam'] = 'Cochin'
df['City_new'].loc[df['City_new'] == 'Mysuru'] = 'Mysore'
df['City_new'].loc[df['City_new'] == 'Thiruvananthapuram'] = 'Trivandrum'
df['City_new'].loc[df['State'] == 'Telangana'] = 'Telangana'

#### Cities to be considered
# City_needed column is filtered as 'Yes' for cities needed to store in City_new and 'No' for the other cities

df['City_needed'] = 'No'
df['City_needed'].loc[df['City_new'] == 'Bengaluru'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Chennai'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Coimbatore'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Cochin'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Gurugram'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Kolkata'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Mysore'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Pune'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Telangana'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Trivandrum'] = 'Yes'
df['City_needed'].loc[df['City_new'] == 'Mumbai'] = 'Yes'
df['City_new'].loc[df['City_needed'] == 'No'] ='Others'

#### Dataframe for Daily Confirmed,Recovered,Deceased cases
# New columns are created for daily confirmed,recovered and deceased record and appended in new dataframe called main_df.
# The diff() function is used to achieve this result. for eg. confirmed cases for today = total confirmed casses today - total confirmed casses yesterday

main_df = pd.DataFrame()
for i in df['City_new'].unique():
    temp = df.loc[df['City_new']==i]
    temp = temp.sort_values(['Date'])
    del_confirmed = temp.loc[:,['Confirmed','Recovered','Deceased']]
    a = del_confirmed.diff()
    temp['Confirmed_daily'] = a['Confirmed']
    temp['Recovered_daily'] = a['Recovered']
    temp['Deceased_daily'] = a['Deceased']
    main_df=main_df.append(temp)

#### 90 day logging
# Tagging the latest 90 days from for visualization window of 90 days in Tableau

d = datetime.now() + timedelta(days=1)
main_df['Date']=pd.to_datetime(main_df['Date'])
main_df['90_day_tagging'] = d-main_df['Date']
main_df['90_day_tagging'] = main_df['90_day_tagging'].astype(str)
main_df['90_day_tagging'] = main_df['90_day_tagging'].str.split(" ", 0, expand=True)
main_df['90_day_tagging'] = main_df['90_day_tagging'].astype(int)
main_df.loc[main_df['90_day_tagging']>90,'90_day_tagging']=np.nan

#### Writing into S3

bucket = 'nyl-processed-data'
csv_buffer = StringIO()
filename = "district_wise_daily_etl.csv"
main_df.to_csv(csv_buffer)
content = csv_buffer.getvalue()

k = "covid19indiaprocessed/csv/district_wise_daily_etl/"+filename
s3.put_object(Bucket=bucket, Key=k, Body=content)
