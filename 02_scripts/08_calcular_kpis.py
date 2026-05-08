# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 08_calcular_kpis.py
# OBJETIVO: Gerar KPIs tratando datas no formato brasileiro (DD/MM/AAAA).
# ==============================================================================
from google.cloud import bigquery
import os
import re

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def executar():
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    for pasta in pastas:
        dataset_id = gerar_nome_dataset(pasta)
        try:
            # O PARSE_DATE converte '19/04/2026' para o formato que o BigQuery entende
            sql = f"""
            CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_kpis` AS
            SELECT 
                SAFE.PARSE_DATE('%d/%m/%Y', CAST(data AS STRING)) as data, 
                SUM(CAST(valor_total AS FLOAT64)) as faturamento, 
                SUM(CAST(quantidade AS FLOAT64)) as qtd_total
            FROM `{client.project}.{dataset_id}.tb_vendas_fato` 
            GROUP BY 1
            """
            client.query(sql).result()
            print(f">>> [OK] KPIs gerados: {dataset_id}")
        except Exception as e:
            print(f"!!! ERRO KPIs {dataset_id}: {e}")
            continue

if __name__ == "__main__":
    executar()