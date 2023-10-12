import datetime as dt
import pandas as pd
from datetime import timedelta
from elasticsearch import Elasticsearch
from airflow import DAG
import psycopg2 as db
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def load_data():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_Yosef_Feriyanto_Data_Raw_M3.csv',index=False)

def cleantornados():
    df=pd.read_csv('/opt/airflow/dags/P2M3_Yosef_Feriyanto_Data_Raw.csv')
    df['loss']=df['loss'].fillna(0)
    df['mag']=df['mag'].fillna(0)
    df.drop_duplicates(inplace=True)
    df['end_time'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)
    df.drop('datetime_utc', axis=1, inplace=True)
    df['start_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df.drop(['date', 'time'], axis=1, inplace=True)
    df['tornado_duration'] = df['end_time'] - df['start_time']
    df['start_location'] = df.apply(lambda row: [row['slon'], row['slat']], axis=1)
    df['end_location'] = df.apply(lambda row: [row['elon'], row['elat']], axis=1)
    df.drop(['slon', 'slat', 'elon', 'elat'], axis=1, inplace=True)
    df['mag'] = df['mag'].astype(int)
    column_mapping = {
        'om': 'tornado_id',
        'yr': 'year',
        'mo': 'month',
        'dy': 'day',
        'tz': 'time_zone',
        'st': 'state',
        'stf': 'state_fips',
        'mag': 'magnitude',
        'inj': 'injuries',
        'fat': 'fatalities',
        'slat': 'start_lat',
        'slon': 'start_lon',
        'elat': 'end_lat',
        'elon': 'end_lon',
        'len': 'length',
        'wid': 'width',
        'ns': 'state_affect',
        'sn': 'state_affect_bin',
        'f1': 'first_cnty_affect',
        'f2': 'second_cnty_affect',
        'f3': 'third_cnty_affect',
        'f4': 'fourth_cnty_affect',
        'fc': 'was_mag_est'}
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv('/opt/airflow/dags/P2M3_Yosef_Feriyanto_Data_Clean.csv', index=False)

def csvToJson():
    df=pd.read_csv('/opt/airflow/dags/P2M3_Yosef_Feriyanto_Data_Clean.csv')
    for i,r in df.iterrows():
        print(r['tornado_id'])
    df.to_json('/opt/airflow/dags/Tornados.json', orient='records')
    # Initialize Elasticsearch
    es = Elasticsearch(hosts=["elasticsearch:9200"])
    # Membaca data yang sudah dibersihkan
    df = pd.read_json('Tornados.json')
    # Mengindeks data ke Elasticsearch
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(res)

default_args = {
    'owner': 'Yosef',
    'start_date': dt.datetime(2023, 9, 26),
    'retries': 1,
    # 'retry_delay': dt.timedelta(seconds=5), # No delay
}


with DAG('M3YosefDAG',
         default_args=default_args,
         schedule_interval=timedelta(minutes=60),      # '0 * * * *',
         ) as dag:
    
    printStarting = BashOperator(task_id='starting',
                                 bash_command='echo "Its Happening! Its Happening!"')
    
    loadSQL = PythonOperator(task_id='fetch',
                             python_callable=load_data)

    cleanData = PythonOperator(task_id='clean',
                                 python_callable=cleantornados)
    
    migrateES = PythonOperator(task_id='convertCSVtoJson',
                                python_callable=csvToJson)
    
    printEnd = BashOperator(task_id='ending',
                            bash_command='echo "BOOM! *drop the mic*"')


printStarting >> loadSQL >> cleanData >> migrateES >> printEnd