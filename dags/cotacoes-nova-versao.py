"""
DAG teste taskflow para extrair cotações do BCB
"""

from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging

from io import StringIO

@dag(
    start_date=datetime(2024, 11, 1),
    schedule="@daily",
    catchup=True,
    doc_md=__doc__,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["bcb", "nova-versao"],
)
def fin_cotacoes_bcb_taskflow():
    
    @task
    def extract(**context) -> str:
        #YYYYMMDD
        ds_nodash = context["ds_nodash"]

        base_url = "https://www4.bcb.gov.br/Download/fechamento/"
        full_url = base_url + ds_nodash + ".csv"
        logging.warning(full_url)

        try:
            response = requests.get(full_url)
            if response.status_code == 200:
                csv_data = response.content.decode("utf-8")
                return csv_data
        except Exception as e:
            logging.error(e)
    
    @task
    def transform(cotacoes: str) -> pd.DataFrame:
        csv_stringIO = StringIO(cotacoes)

        column_names = [
            "DT_FECHAMENTO",
            "COD_MOEDA",
            "TIPO_MOEDA",
            "DESC_MOEDA",
            "TAXA_COMPRA",
            "TAXA_VENDA",
            "PARIDADE_COMPRA",
            "PARIDADE_VENDA"
        ]

        data_types = {
            "DT_FECHAMENTO": str,
            "COD_MOEDA": str,
            "TIPO_MOEDA": str,
            "DESC_MOEDA": str,
            "TAXA_COMPRA": float,
            "TAXA_VENDA": float,
            "PARIDADE_COMPRA": float,
            "PARIDADE_VENDA": float
        }

        parse_dates = ["DT_FECHAMENTO"]

        df = pd.read_csv(
            csv_stringIO,
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            header=None,
            names=column_names,
            dtype=data_types,
            parse_dates=parse_dates,
            dayfirst=True
        )

        df['data_processamento'] = pd.to_datetime(datetime.now())

        return df

    create_table_ddl = """
        CREATE TABLE IF NOT EXISTS cotacoes_taskflow (
            dt_fechamento DATE,
            cod_moeda TEXT,
            tipo_moeda TEXT,
            desc_moeda TEXT,
            taxa_compra REAL,
            taxa_venda REAL,
            paridade_compra REAL,
            paridade_venda REAL,
            data_processamento TIMESTAMP,
            CONSTRAINT table_tf_pk PRIMARY KEY (dt_fechamento, cod_moeda)
        )
    """

    @task()
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_astro", schema="astro")
        postgres_hook.run(create_table_ddl)
    
    @task
    def load(cotacoes_df: pd.DataFrame) -> None:
        table_name = "cotacoes_taskflow"

        postgres_hook = PostgresHook(postgres_conn_id="postgres_astro", schema="astro")

        rows = list(cotacoes_df.itertuples(index=False))

        postgres_hook.insert_rows(
            table_name,
            rows,
            replace=True,
            replace_index=["DT_FECHAMENTO", "COD_MOEDA"],
            target_fields=["DT_FECHAMENTO", "COD_MOEDA", "TIPO_MOEDA", "DESC_MOEDA", "TAXA_COMPRA", "TAXA_VENDA", "PARIDADE_COMPRA", "PARIDADE_VENDA", "data_processamento"]
        )
    
    create_table() >> load(transform(extract()))

fin_cotacoes_bcb_taskflow()

