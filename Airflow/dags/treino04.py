from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
import pandas as pd


engine = create_engine('mysql+pymysql://root:essaeasenha@localhost:3306/dbairflow')

# Argumentos defaults
default_args = {
    'owner': 'Borges',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 25, 13, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definirt a DEG
dag = DAG(
    'treino-eng_dados_04',
    description='Paralelismo',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_preprocessing = BashOperator(
    task_id='Start_preprocessing',
    bash_command='echo "Start Preprocessing!!!!!"',
    dag=dag
)


def aplicar_filtro():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER',
            'NT_FG', 'NT_CE_D1', 'NT_CE_D2', 'NT_CE_D3',
            'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']

    enade = pd.read_csv('/home/borges/airflow/data/microdados2021_arq_amostra.csv',
                        sep=',', decimal='.', usecols=cols)

    enade = enade.loc[
        (enade.NU_IDADE >= 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]

    enade.to_csv('/home/borges/airflow/data/enade_filtrado.csv', index=False)


task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplicar_filtro,
    dag=dag
)


# Idade centralizada na media
def constroi_idade_centralizada():
    idade = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                        usecols=['NU_IDADE'])

    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()

    idade[['idadecent']].to_csv('/home/borges/airflow/data/idadecent.csv',
                                index=False)


# Idade centralizada ao quadrado
def constroi_idade_cent_quad():
    idadecent = pd.read_csv('/home/borges/airflow/data/idadecent.csv')

    idadecent['idade2'] = idadecent.idadecent ** 2

    idadecent[['idade2']].to_csv('/home/borges/airflow/data/idadequadrado.csv',
                                 index=False)


task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

task_idade_quad = PythonOperator(
    task_id='constroi_idade_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)


def constroi_est_civil():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                         usecols=['QE_I01'])

    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Viuvo',
        'C': 'Separado',
        'D': 'Casado',
        'E': 'Outro'
    })

    filtro[['estcivil']].to_csv('/home/borges/airflow/data/estcivil.csv',
                                index=False)


task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)


def constroi_cor():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                         usecols=['QE_I02'])

    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': pd.NA,
        ' ': pd.NA
    })

    filtro[['cor']].to_csv('/home/borges/airflow/data/cor.csv',
                           index=False)


task_cor = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)


def constroi_escopai():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                         usecols=['QE_I04'])

    filtro['escopai'] = filtro.QE_I04.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
    })

    filtro[['escopai']].to_csv('/home/borges/airflow/data/escopai.csv',
                               index=False)


task_escopai = PythonOperator(
    task_id='constroi_escopai',
    python_callable=constroi_escopai,
    dag=dag
)


def constroi_escomae():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                         usecols=['QE_I05'])

    filtro['escomae'] = filtro.QE_I05.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
    })

    filtro[['escomae']].to_csv('/home/borges/airflow/data/escomae.csv',
                               index=False)


task_escomae = PythonOperator(
    task_id='constroi_escomae',
    python_callable=constroi_escomae,
    dag=dag
)


def constroi_renda():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv',
                         usecols=['QE_I08'])

    filtro['renda'] = filtro.QE_I08.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
        'G': 6,
    })

    filtro[['renda']].to_csv('/home/borges/airflow/data/renda.csv',
                             index=False)


task_renda = PythonOperator(
    task_id='constroi_renda',
    python_callable=constroi_renda,
    dag=dag
)


# Task de JOIN
def join_data():
    filtro = pd.read_csv('/home/borges/airflow/data/enade_filtrado.csv')
    idadecent = pd.read_csv('/home/borges/airflow/data/idadecent.csv')
    idadequadrado = pd.read_csv('/home/borges/airflow/data/idadequadrado.csv')
    estcivil = pd.read_csv('/home/borges/airflow/data/estcivil.csv')
    cor = pd.read_csv('/home/borges/airflow/data/cor.csv')
    escopai = pd.read_csv('/home/borges/airflow/data/escopai.csv')
    escomae = pd.read_csv('/home/borges/airflow/data/escomae.csv')
    renda = pd.read_csv('/home/borges/airflow/data/renda.csv')

    final = pd.concat([
        filtro, idadecent, idadequadrado, estcivil,
        cor, escopai, escomae, renda
    ], axis=1)

    final.to_csv('/home/borges/airflow/data/enade_tratado.csv', index=False)

    print(final)


task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)


def data_dw():
    # csv
    df_insert = pd.read_csv('/home/borges/airflow/data/enade_tratado.csv')

    # criar e carregar dados no mysql
    metadata = MetaData(bind=engine)
    table = Table('enade_tratado', metadata,
                  Column('CO_GRUPO', String(150)),
                  Column('NT_GER', String(150)),
                  Column('NT_FG', Integer),
                  Column('NT_CE_D1', String(150)),
                  Column('NT_CE_D2', String(150)),
                  Column('NT_CE_D3', String(150)),
                  Column('TP_SEXO', String(5)),
                  Column('NU_IDADE', Integer),
                  Column('QE_I01', String(5)),
                  Column('QE_I02', String(5)),
                  Column('QE_I04', String(5)),
                  Column('QE_I05', String(5)),
                  Column('QE_I08', String(5)),
                  Column('idadecent', String(255)),
                  Column('idade2', String(255)),
                  Column('estcivil', String(55)),
                  Column('cor', String(25)),
                  Column('escopai', Integer),
                  Column('escomae',  Integer),
                  Column('renda',  Integer),
                  )

    if not table.exists():
        table.create()

    df_insert.to_sql('enade_tratado',
                     con=engine, if_exists='append',
                     index=False)


task_dw_insert = PythonOperator(
    task_id='insert_data_dw',
    python_callable=data_dw,
    dag=dag
)

task_final = DummyOperator(
    task_id="final_ok",
    dag=dag
)


start_preprocessing >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor,
                       task_escopai, task_escomae, task_renda]

task_idade_quad.set_upstream(task_idade_cent)

task_join.set_upstream([
    task_est_civil, task_cor, task_idade_quad,
    task_escopai, task_escomae, task_renda
])

task_dw_insert.set_upstream(task_join)

task_final.set_upstream(task_dw_insert)
