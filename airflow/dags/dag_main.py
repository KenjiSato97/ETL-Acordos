from datetime import timedelta, datetime
from airflow import DAG
from airflow.decorators import dag  
from airflow.operators.python import PythonOperator  
#from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup 

from tasks.bronze import google_sheet_to_minio_etl
from tasks.silver import transform_and_load_silver
from tasks.gold import transform_and_load_gold  

default_args = {
    'owner': 'Kenji',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsavel pelo ETL do case Acordos Internacionais na Área de Energia. Dados vindos da API do google sheets',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def main_dag():
    google_sheets = ['Geral']  # Altere para o nome das abas da sua planilha
    bucket_name = 'bronze'
    bucket2_name = 'silver'
    bucket3_name = 'gold'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '18HOS_6TuNxBlMaVITJgllR0W9Y2eB609KnsFdNs-neI'  # ID da planilha do google sheets

    with TaskGroup("group_task_sheets", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as group_task_sheets:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheets_name}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
            )
    with TaskGroup("slv_task_acordos", tooltip="Tasks de transformação e carga da camada silver") as slv_task_acordos:
        silver = {
            'slv_acordos': 'Geral',
        }
        for slv_name, sheet_name in silver.items():
            PythonOperator(
                task_id=f'{slv_name}_{sheet_name}',  # O nome da tarefa será único e descritivo, baseado na chave e valor do dicionário
                python_callable=transform_and_load_silver,  # Ajuste esta função se necessário para outras dimensões
                op_args=[bucket_name,
                         f"{sheet_name}/brz_{sheet_name}.parquet",
                         endpoint_url,
                         access_key,
                         secret_key]
            )

    with TaskGroup("gld_task_acordos", tooltip="Tasks de transformação e carga da camada gold") as gld_task_acordos:
        gold = {
            'gld_acordos': 'slv_acordos',
            'gld_hier': 'slv_acordos',
            'gld_pais': 'slv_acordos',
            'gld_org': 'slv_acordos',
        }
        for gld_name, slv_name in gold.items():
            PythonOperator(
                task_id=f'{gld_name}_{slv_name}',  # O nome da tarefa será único e descritivo, baseado na chave e valor do dicionário
                python_callable=transform_and_load_gold,  # Ajuste esta função se necessário para outras dimensões
                op_args=[bucket2_name,
                         f"{bucket2_name}/{slv_name}.parquet",
                         endpoint_url,
                         access_key,
                         secret_key]
            )

    group_task_sheets >> slv_task_acordos >> gld_task_acordos

main_dag_instance = main_dag()