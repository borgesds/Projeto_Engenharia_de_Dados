from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "andreborges",
    "start_date": datetime(2023, 6, 17),
}

dag = DAG(
    "dag_teste_curso_airflow",
    default_args=default_args,
    schedule_interval="15 10 * * 1-5",
    max_active_runs=1,
)

# DummyOpertor serve só para visualização
primeira_task = DummyOperator(
    task_id="primeira_task",
    dag=dag
)


def execute():
    curso_teste_airflow = Variable.get("teste_airflow")
    print(f'Nossa variavel é {curso_teste_airflow}')


task_python_operator = PythonOperator(
    task_id='task_python_operator',
    python_callable=execute,
    dag=dag
)


ultima_task = DummyOperator(
    task_id="ultima_task",
    dag=dag
)


ultima_task_final = DummyOperator(
    task_id="ultima_task_final",
    dag=dag
)


primeira_task >> task_python_operator >> ultima_task >> ultima_task_final
