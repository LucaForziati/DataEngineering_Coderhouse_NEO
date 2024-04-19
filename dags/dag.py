# importar librerias
from datetime import datetime, timedelta
from funciones_dag import consulta_api_nasa, convertir_dataframe, cargar_redshift

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# argumentos del DAG
default_args={
    # propietario
    'owner': 'LukeFo',
    # cantidad de reintentos por si falla
    'retries':5,
    # frecuencia con la cual va a ejecutar cada intento
    'retry_delay': timedelta(minutes=3)
}



api_dag = DAG(
    # argumentos definidos previamente.
    default_args=default_args,
    # definir el ID del DAG
    dag_id='NEO_coderhouse',
    # descripción de lo que realiza el sistema
    description= 'Sistema que permite almacenar los ultimos objetos que han pasado cerca de la tierra, registrado por la Nasa',
    # fecha y hora que va a iniciar el DAG
    start_date=datetime(2024,4,9,17),
    # Frecuencia con la cual se debe ejecutar el DAG. En este caso, es cada dia.
    schedule_interval='@daily'
    )

task1 = PythonOperator(
    task_id='Descargar_datos_crudos',
    python_callable=consulta_api_nasa,
    dag=api_dag
)

task2 = PythonOperator(
    task_id='Convertir_dataframe',
    python_callable=convertir_dataframe,
    dag=api_dag
)

task3 = PythonOperator(
    task_id='Cargar_datos_redshift',
    python_callable=cargar_redshift,
    dag=api_dag
)

task1 >> task2 >> task3




