'''
=================================================
Milestone 3

Nama  : Frederick Kurniawan Putra
Batch : FTDS-016-HCK

Program ini dibuat untuk untuk melakukan otomatisasi proses ekstraksi dari postgre,
proses transformasi agar data bersih dan sesuai dengan ketentuan,
dan juga proses load agar tidak 
=================================================
'''

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

from elasticsearch import Elasticsearch


def csv_to_psg():
    '''
    This function are used to store initial csv file to postgre.

    Step of this function:
    1. Create connection using python using sqlalchemy.
    2. Input all data csv into pandas dataframe
    3. create 'table_m3' table in postgre, and input all datas in dataframe to postgre table.

    '''
    
    db_name = 'airflow_m3'
    user_name = 'airflow_m3'
    password = 'airflow_m3'
    host = 'postgres'

    postgres_url = f'postgresql+psycopg2://{user_name}:{password}@{host}/{db_name}'
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/car_prices_new.csv')
    df.to_sql('table_m3', conn,index=False, if_exists='replace')

def psg_to_csv():
    '''
    This function are used to import data from postgre to raw csv.

    Step of this function:
    1. Create connection using python using sqlalchemy.
    2. query data from postgres and input it into pandas dataframe.
    3. input all data in pandas dataframe into csv.

    '''

    db_name = 'airflow_m3'
    user_name = 'airflow_m3'
    password = 'airflow_m3'
    host = 'postgres'

    postgres_url = f'postgresql+psycopg2://{user_name}:{password}@{host}/{db_name}'
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_frederick_kurniawan_data_raw.csv', sep=',',index=False)

def clean_csv():
    '''
    This function are used to clean data in raw csv to met data quality standard.

    Step of this function:
    1. Input data from raw csv into pandas dataframe.
    2. Perform several data cleaning codes.
    3. input all data in pandas dataframe into clean csv.

    '''


    df = pd.read_csv('/opt/airflow/dags/P2M3_frederick_kurniawan_data_raw.csv')

    #drop missing value
    df.dropna(inplace=True)

    #drop duplicates
    df.drop_duplicates(inplace=True)

    #lower case all value in make column make and body
    df['Make'] = df['Make'].str.lower()
    df['Body'] = df['Body'].str.lower()

    #remove 'tk' substring from all value in make column
    df['Make'] = df['Make'].str.replace(' tk','')

    #change mercedes and merceded-b
    df['Make'] = df['Make'].str.replace('mercedes','mercedes-benz')
    df['Make'] = df['Make'].str.replace('mercedes-b','mercedes-benz')

    #change vw to volkswagen
    df['Make'] = df['Make'].str.replace('vw','volkswagen')

    #change chev truck to chevrolet
    df['Make'] = df['Make'].str.replace('chev truck','chevrolet')

    #remove ' truck' to 'ford'
    df['Make'] = df['Make'].str.replace(' truck','')

    #change 'dot' to dodge
    df['Make'] = df['Make'].str.replace('dot','dodge')

    #drop entries with 'sedan' or 'Sedan' value in transmission column
    df.drop(df[df['Transmission'] == 'Sedan'].index,inplace=True)
    df.drop(df[df['Transmission'] == 'sedan'].index,inplace=True)

    #drop entries with '-' value in interior color
    df.drop(df[df['Interior'] == '—'].index,inplace=True)

    #change date column to datetime datatype
    df['Saledate'] = pd.to_datetime(df['Saledate'], utc=True)

    #change year column to object
    df['Year'] = df['Year'].apply(lambda x: str(x))


    #lowercase all table columns
    df.columns = [x.lower() for x in df.columns]

    #add transaction_id column to dataframe
    df['transaction_id'] = df['vin'] + '_' + df['odometer'].astype(str) + '_' + df['sellingprice'].astype(str) + '_' +  df['mmr'].astype(str)

    #save to csv
    df.to_csv('/opt/airflow/dags/P2M3_frederick_kurniawan_data_clean.csv', sep=',',index=False)


def upload_to_elasticsearch():
    '''
    This function are used to export clean datas in csv into elastisearch.

    Step of this function:
    1. Define Elasticsearch and its port to a variable.
    2. Import clean data from csv to pandas dataframe.
    3. Input all datas in dataframe to elasticseach with index="table_m3".

    '''
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/P2M3_frederick_kurniawan_data_clean.csv')

    for i, r in df.iterrows():
        doc = r.to_dict()
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f'Response from elasticsearch: {res}')


default_args = {
'owner': 'frederick',
'start_date': datetime(2024, 6, 22),
# 'retries': 1,
# 'retry_delay': dt.timedelta(minutes=5),
}
    
with DAG('Table_M3',
        default_args=default_args,
        description='Milestone_3',
        schedule_interval='30 6 * * *',      # '0 * * * *',
        ) as dag:

    load_data_pg = PythonOperator(task_id='csv_psg',
                                 python_callable=csv_to_psg)
    
    get_data_csv = PythonOperator(task_id='psg_csv',
                                   python_callable=psg_to_csv)
    
    clean_data_csv = PythonOperator(task_id='csv_clean',
                                   python_callable=clean_csv)
    
    upload_data_csv = PythonOperator(task_id='clean_es',
                                   python_callable=upload_to_elasticsearch)
    

load_data_pg >> get_data_csv>>clean_data_csv>>upload_data_csv