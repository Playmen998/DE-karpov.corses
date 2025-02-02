from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from e_lavushkin_plugins.e_lavrushkin import RamLocOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'e-lavrushkin',
    'poke_interval': 600
}

with DAG("e-lavrushkin-plg",
         schedule_interval= '@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-lavrushkin']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    RamLocOperator_d = RamLocOperator(
        task_id='RamLocOperator',
        table_name='public.e_lavrushkin_ram_location'

    )
    #1

    dummy >> RamLocOperator_d