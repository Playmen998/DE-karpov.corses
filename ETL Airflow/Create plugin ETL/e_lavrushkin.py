import requests
import operator
import logging
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RickMortyHook(HttpHook):

    def __init__(self, result: list, table_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.result = result
        self.table_name = table_name

    def insert_row(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        logging.warning('Initialise pg_hook')

        pg_hook.run(f'''
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id int4 NOT NULL PRIMARY KEY,
                        "name" varchar NULL,
                        "type" varchar NULL,
                        dimension varchar NULL,
                        resident_cnt int4 NULL
                    )
                    DISTRIBUTED BY (id)
                ''')
        logging.warning('TABLE CREATED')

        pg_hook.run(f'TRUNCATE TABLE {self.table_name}', False)
        logging.warning('TABLE TRUNCATED')

        for data in self.result:
            pg_hook.run(f'''
                INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt)
                VALUES {tuple(data)}
            ''')
        logging.warning('INSERT VALUES')


class RamLocOperator(BaseOperator):

    def __init__(self, table_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name

    def execute(self, context):

        r = requests.get('https://rickandmortyapi.com/api/location')
        js = r.json().get('results')
        result_list = []
        for i in range(len(js)):
          name = js[i].get('name')
          name_type = js[i].get('type')
          name_dimension = js[i].get('dimension')
          resident_cnt = len(js[i].get('residents'))
          result_list.append([i+1, name, name_type, name_dimension, resident_cnt])
        result_list_sort = sorted(result_list, key=lambda x: x[4], reverse=True)[:3]
        #result_list_sort = [[i] + lst for i, lst  in zip(range(1, 4),result_list_sort)]
        logging.info(f'TEST plugin: {result_list_sort}')

        posgreshook = RickMortyHook(result=result_list_sort, table_name=self.table_name)
        posgreshook.insert_row()