#python functions

import pandas as pd
import gzip
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from configparser import ConfigParser
import gc
import sys
import time
from datetime import datetime, timedelta

config_path=r'/opt/airflow/config/'
raw_files_path=r'/opt/airflow/raw_files/'


def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    
    return db

def get_engine():
    dbschema='staging'
    param=config(filename=config_path+r'database.ini', section='postgresql')
    db_uri='postgresql+psycopg2://'+param['user']+':'+param['password']+'@'+param['host']+'/'+param['database']
    engine=create_engine(db_uri,connect_args={'options': '-csearch_path={}'.format(dbschema)})
    return engine

def drop_nan_rows(df,cols):
    df.columns=map(str.lower,df.columns)
    df.dropna(subset=cols,inplace=True)
    return df

def remove_NUL_chars(df):
    df.columns=map(str.lower,df.columns)
    for col in df.columns:
        if col in ['price','overall']:
            pass
        elif col in ['unixreviewtime']:
            pass
        else:
            df[col]=df[col].astype('str')
            df[col]=df[col].apply(lambda x: x.replace("\x00", "\uFFFD"))
    return df

def fix_col_datatypes(df):
    df.columns=map(str.lower,df.columns)
    for col in df.columns:
        if col in ['price','overall']:
            df[col]=df[col].astype('float64')
        elif col in ['unixreviewtime']:
            df[col]=df[col].apply(datetime.fromtimestamp)
        else:
            df[col]=df[col].astype('str')
    return df

def process_file(file,t_name,not_null_cols,offset,limit):
    f_name=raw_files_path+file
    f=gzip.open(f_name)
    
    t_cnt=0
    cnt=0
    dic={}
    while (cnt<offset):
        _=f.readline()
        cnt=cnt+1
    print(time.strftime('%H:%M:%S'),'-',offset,'records skipped')
    cnt=0
    for l in f:
        d=eval(l)
        if 'categories' in d.keys():
            d['category']=d['categories'][0][0]
        dic[cnt]=d
        cnt=cnt+1
        t_cnt=t_cnt+1
        if cnt%1000000==0:
            print(time.strftime('%H:%M:%S'),'- Moving Data to DataFrame from file')
            df=pd.DataFrame.from_dict(dic, orient='index')
            dic={}
            gc.collect()
            df=drop_nan_rows(df,not_null_cols)
            df=remove_NUL_chars(df)
            df=fix_col_datatypes(df)
            print(time.strftime('%H:%M:%S'),'- 1 Mil records processed to DataFrame from file')
            df.to_sql(t_name,con=get_engine(),index=False,if_exists='append')
            print(time.strftime('%H:%M:%S'),'-',t_cnt,' - records processed to DB')
            cnt=0
            del(df)
            gc.collect()
        if t_cnt==limit:
            break
    df=pd.DataFrame.from_dict(dic, orient='index')    
    if df.shape[0] != 0:
        df=pd.DataFrame.from_dict(dic, orient='index')
        df=drop_nan_rows(df,not_null_cols)
        df=fix_col_datatypes(df)
        df.to_sql(t_name,con=get_engine(),index=False,if_exists='append')
        del(df)
    print(time.strftime('%H:%M:%S'),'-',t_cnt,' - records processed to DB')
    print(time.strftime('%H:%M:%S'),'Whole file processed to DB')   

#if (__name__=='__main__'):
#
#    arg=sys.argv[1:]
#    print(time.strftime('%H:%M:%S'))
#    if len(arg)==5:
#        process_file(arg[0],arg[1],eval(arg[2]),int(arg[3]),int(arg[4]))
  

##################################
#---------DAG SCHEDULING---------#
##################################

from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    'JET_process_reviews_products',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['test@test.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='downloads the gz files then loads the files to staging schema and then process to curated schema dim and fact tables',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 7),
    catchup=False,
    tags=['JET'],
) as dag:

    
    download_metadata = BashOperator(
        task_id='download_metadata',
        bash_command='curl -o /opt/airflow/raw_files/metadata.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz',
        #bash_command='curl -o /opt/airflow/raw_files/metadata.jpg https://en.wikipedia.org/wiki/Main_Page#/media/File:Daisy_Pearce.3.jpg',
    )
    
    download_reviews = BashOperator(
        task_id='download_reviews',
        bash_command='curl -o /opt/airflow/raw_files/reviews.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz',
    )
    
    load_staging_products = PythonOperator(
        task_id='load_staging_products',
        python_callable= process_file,
        op_kwargs = {"file" : "metadata.gz","t_name" : "products","not_null_cols":['asin','title','categories','price'],"offset" : 0,"limit" : 50000},
    )
    
    load_staging_reviews = PythonOperator(
        task_id='load_staging_reviews',
        python_callable= process_file,
        op_kwargs = {"file" : "reviews.gz","t_name" : "reviews","not_null_cols":['asin','reviewerid','reviewername','reviewtext','summary','overall','unixreviewtime'],"offset" : 0,"limit" : 50000},
    )
    
    drop_staging_products_table= PostgresOperator(
        task_id="drop_staging_products_table",
        postgres_conn_id="postgres_main",
        sql="sql/drop_staging_products_table.sql",
    )
    
    drop_staging_reviews_table= PostgresOperator(
        task_id="drop_staging_reviews_table",
        postgres_conn_id="postgres_main",
        sql="sql/drop_staging_reviews_table.sql",
    )
    
    load_dim_reviewer= PostgresOperator(
        task_id="load_dim_reviewer",
        postgres_conn_id="postgres_main",
        sql="sql/dim_reviewer.sql",
    )
    
    load_fact_product_reviews= PostgresOperator(
        task_id="load_fact_product_reviews",
        postgres_conn_id="postgres_main",
        sql="sql/fact_product_reviews.sql",
    )
    
    load_dim_product= PostgresOperator(
        task_id="load_dim_product",
        postgres_conn_id="postgres_main",
        sql="sql/dim_product.sql",
    )
    

    download_metadata >> drop_staging_products_table >> load_staging_products >> load_dim_product
    download_reviews >> drop_staging_reviews_table >> load_staging_reviews >> load_dim_reviewer >> load_fact_product_reviews

    
    
