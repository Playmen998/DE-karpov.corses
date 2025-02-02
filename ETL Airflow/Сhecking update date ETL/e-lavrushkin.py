from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'e-lavrushkin',
    'poke_interval': 600
}

with DAG("e-lavrushkin",
         schedule_interval= '0 10 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         start_date=datetime(2022 , 1, 1),
         end_date=datetime(2022, 6, 14), 
         tags=['e-lavrushkin']
         ) as dag:
    start = DummyOperator(task_id='DummyOperator')

    echo_ds = BashOperator(
        task_id='BashOperator',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def function():
        logging.info(str(datetime.today()))


    print_args = PythonOperator(
        task_id='PythonOperator',
        python_callable=function,
    )

    def get_data_logs_articles_func():
        today_date = datetime.today().weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today_date}')
        query_res = cursor.fetchall()
        logging.info(str(query_res[0][0]))

    get_data_logs = PythonOperator(
        task_id = 'get_data_logs',
        python_callable = get_data_logs_articles_func
    )

    def get_data_xcom_articles_func(**kwargs):
        today_date = datetime.today().weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today_date}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=str(query_res[0][0]), key="task-1")


    get_data_xcom = PythonOperator(
        task_id='get_data_xcom',
        python_callable=get_data_xcom_articles_func,
        provide_context=True
    )

    start >> [echo_ds, print_args]
    get_data_logs >> get_data_xcom