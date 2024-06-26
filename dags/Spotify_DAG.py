# %%
import pandas  as pd
from pandas import json_normalize
from datetime import datetime
import datetime
import pandas as pd
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
import base64
import requests
from sqlalchemy.exc import IntegrityError
import os 
# %%
# Set credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET =os.getenv("CLIENT_SECRET")
REFRESH_TOKEN=os.getenv("REFRESH_TOKEN")
EMAIL = os.getenv("EMAIL")
REDIRECT_URI="https://spotify-refresh-token-generator.netlify.app"
SCOPES="user-read-recently-played"

# %%
def generate_access_token(client_id,client_secret,refresh_token):
        auth_client = client_id + ":" + client_secret
        auth_encode = 'Basic ' + base64.b64encode(auth_client.encode()).decode()
        headers = {
            'Authorization': auth_encode,
            }
        data = {
            'grant_type' : 'refresh_token',
            'refresh_token' : refresh_token
            }
        response = requests.post('https://accounts.spotify.com/api/token', data=data, headers=headers) #sends request off to spotify

        if(response.status_code == 200): #checks if request was valid
            print("The request to went through we got a status 200; Spotify token refreshed")
            response_json = response.json()
            new_expire = response_json['expires_in']
            print("the time left on new token is: "+ str(new_expire / 60) + "min") #says how long
            return response_json["access_token"]
        else:
            print("ERROR! The response we got was: "+ str(response))

# Define function to get recently payed songs
def recently_played_songs():
        input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=generate_access_token(CLIENT_ID,CLIENT_SECRET,REFRESH_TOKEN))
        }
        #Define time period to retrive data
        today=datetime.datetime.now()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp())*1000
        # Call API
        results = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp), headers = input_variables)
        results = results.json()
        df1=pd.json_normalize(results['items'])

        # Explode json to obtain required fields
        try:
            # Extract timestamp
            df1['timestamps']=df1["played_at"].str[0:10]
        except KeyError:
            df=pd.DataFrame()
            print("Json is empty. No songs extracted")
            return df
        else:
            # Select only required fields
            df1=df1[['track.name',
            'track.artists',
            'played_at',
            'timestamps']]
            # Explode nested json fields to obtain required fields
            df2=json_normalize(df1['track.artists'])
            df2=json_normalize(df2[0])
            # generate final df
            df=pd.merge(df1, df2, left_index=True, right_index=True)
            df=df[['track.name','name','played_at','timestamps']].rename(columns={'track.name':'song_name',
                                                            'name':'artist_name',
                                                            'played_at':'played_at',
                                                            'timestamps':'timestamp'})
            return df
# %%
# Check data quality before loading
def data_quality(load_df):
    # Checking wether the dataset is empty
    if load_df.empty:
        print('No songs extracted')
        return False
    # Checking that primary key is distinct
    if load_df['played_at'].is_unique:
        pass
    else:
        # Terminate program and raise exception
        raise Exception("Primary key exception. Data might contain duplicates")
    # Checking for nulls in dataframe
    if load_df.isnull().values.any():
        raise Exception("Null value found")

# %%
def spotify_etl():
    # Extract songs
    load_df=recently_played_songs()
    #Check data quality
    data_quality(load_df)
    #return df
    return load_df
# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023,9,24,15,00,00),
    'email': [EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=1)
}

# %%
dag= DAG('spotify_dag',
        default_args=default_args,
        description='Spotify ETL process',
        schedule_interval=dt.timedelta(minutes=120)
        )

# %%

def ETL():
    print("Started")
    df=spotify_etl()
    conn=BaseHook.get_connection('postgres_default')
    # Create database conection
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql('my_played_tracks',if_exists='append',con = engine,index=False)
        except IntegrityError:
            pass

# %%
with dag:
    create_table=PostgresOperator(
        task_id='create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """
    )
    run_etl=PythonOperator(
        task_id='spotify_etl_final',
        python_callable=ETL,
        dag=dag
    )

    create_table >> run_etl


