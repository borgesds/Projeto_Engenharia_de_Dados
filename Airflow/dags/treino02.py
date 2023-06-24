from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd


# Argumentos defaults
default_args = {
    'owner': 'Borges',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 21),
    'email': ['borges@gmail.com', 'borgesan@gmail.com'],
    'email_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definirt a DEG
dag = DAG(
    'treino-eng_dados_02',
    description='Dados do Titanic e calcular a idade media',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    max_active_runs=1,
)

get_data = DummyOperator(
    task_id='get_data',
    dag=dag
)

"""get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/Downloads/train.csv',
    dag=dag
)"""


def calculate_mean_age():
    df = pd.read_csv('~/Downloads/train.csv')
    print(df.columns)
    med = df.Age.mean()
    return med


def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calula-idade-media')
    print(f'A idade media no Titanic era {value} anos')


task_idade_media = PythonOperator(
    task_id='calula-idade-media',
    python_callable=calculate_mean_age,
    dag=dag
)

task_print_idade = PythonOperator(
    task_id='mostra-idade',
    python_callable=print_age,
    provide_context=True,
    dag=dag
)

get_data >> task_idade_media >> task_print_idade
