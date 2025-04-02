import pandas as pd
from airflow import DAG
from datetime import datetime
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task


# Parameterize File Paths
input_file_path = Variable.get("input_file_path", default_var='/home/nitish/ETL/data/input_data.csv')
output_file_path = Variable.get("output_file_path",default_var='/home/nitish/ETL/data/output_data.csv')

#ETL task definations


@task
def extract_data():
    try:
        df = pd.read_csv(input_file_path)
        with open(output_file_path, 'w') as f:
            df.to_csv(f,index=False)
    except Exception as e:
        logging.error(f"Error in extracting data: {e}")
        raise
@task
def transform_data():
    df = pd.read_csv(output_file_path)
    df = df.dropna()
    df.columns = [col.lower() for col in df.columns]
    df['age'] = df['age'].astype(int)
    logging.info(f"Tranformed Data: {df.head()}")
    df.to_csv(output_file_path, index=False)

@task
def load_data():
    try:
        df = pd.read_csv(output_file_path)

        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        for _,row in df.iterrows():
            pg_hook.run(
            "INSERT into transformed_data (name,age,city) values(%s,%s,%s)",
            parameters=(row['name'],row['age'],row['city'])
            )
        logging.info(f"Data loaded successfully into PostgreSQL")

    except Exception as e:
        logging.error(f"Error in loading data: {e}")
        raise
    
    
#Define DAG
default_args = {
    'owner': 'nitish',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with DAG(
    'simple_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    description='A simple ETL pipleine for processing CSV data , transforming it and loading into PostgreSQL'
) as dag:
    extract_task = extract_data()
    transform_task = transform_data()
    load_task = load_data()

# extract_task =  PythonOperator(
#     task_id='extract_data',
#     python_callable=extract_data,
#     dag=dag
# )

# transform_task =  PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     dag=dag
# )

# load_task = PythonOperator(
#     task_id ='load_data',
#     python_callable=load_data,
#     dag=dag
# )

extract_task >> transform_task>>load_task