from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


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
    'treino-eng_dados_01',
    description='Basico de Bash Operators e Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=20),
    max_active_runs=1,
)

# Adicionar tarefas
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)


def excute():
    print('Hello Airflow World')


hello_python = PythonOperator(
    task_id='Hello_Python',
    python_callable=excute,
    dag=dag
)

finally_hello = DummyOperator(
    task_id='finally_hello',
    dag=dag
)

hello_bash >> hello_python >> finally_hello
