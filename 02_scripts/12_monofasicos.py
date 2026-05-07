# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 12_monofasicos.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Gerar análise de impostos monofásicos para todos os clientes ativos.
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
        sql = f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_analise_monofasicos` AS
        SELECT 
            item, 
            SUM(quantidade) as qtd_total, 
            SUM(valor_total) as faturamento_bruto
        FROM `{client.project}.{dataset_id}.tb_vendas_fato`
        GROUP BY 1
        """
        try:
            client.query(sql).result()
            print(f">>> [OK] Monofásicos gerados: {dataset_id}")
        except Exception as e:
            print(f"Erro Script 12 em {dataset_id}: {e}")

if __name__ == "__main__":
    executar()