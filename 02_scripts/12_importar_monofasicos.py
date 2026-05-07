# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 12_monofasicos.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# FINALIDADE: Identificar produtos com PIS/COFINS Monofásico.
#             - Ajustado para colunas: 'produto'/'item' e 'ncm'
# ==========================================================

import pandas as pd
from google.cloud import bigquery
import os
import re

# CONFIGURAÇÃO
PROJECT_ID = "v2-gastrobi-lab"
TABELA_RESULTADO = "tb_analise_monofasicos"

client = bigquery.Client(project=PROJECT_ID)

def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

def analisar_monofasicos_cliente(dataset_alvo):
    try:
        # Pega as colunas reais da tabela
        table_ref = client.get_table(f"{PROJECT_ID}.{dataset_alvo}.tb_vendas_fato")
        colunas_reais = [field.name for field in table_ref.schema]

        # Lógica Flexível para nomes de colunas
        col_ncm = next((c for c in colunas_reais if c.lower() in ['ncm', 'ncm_simulado']), None)
        col_item = next((c for c in colunas_reais if c.lower() in ['item', 'produto', 'nome_item']), None)
        col_valor = next((c for c in colunas_reais if 'valor_total' in c.lower()), 'valor_total')
        col_qtd = next((c for c in colunas_reais if 'quant' in c.lower() or 'qtd' in c.lower()), 'quantidade')

        if not col_ncm or not col_item:
            print(f"--- Erro: Colunas essenciais não identificadas em {dataset_alvo} ---")
            return

        # SQL FINAL com nomes dinâmicos
        sql = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_alvo}.{TABELA_RESULTADO}` AS
        SELECT 
            {col_item} as item,
            CAST({col_ncm} AS STRING) as ncm,
            SUM({col_qtd}) as qtd_total,
            SUM({col_valor}) as faturamento_bruto,
            ROUND(SUM({col_valor}) * 0.0925, 2) as potencial_recuperacao_estimado
        FROM `{PROJECT_ID}.{dataset_alvo}.tb_vendas_fato`
        WHERE 
            SUBSTR(CAST({col_ncm} AS STRING), 1, 4) IN ('2202', '2203', '2106')
        GROUP BY 1, 2
        ORDER BY faturamento_bruto DESC
        """
        
        query_job = client.query(sql)
        query_job.result()
        
        print(f">>> Sucesso: Análise de Monofásicos gerada para {dataset_alvo}!")

    except Exception as e:
        print(f"--- Aviso: Erro em {dataset_alvo}: {e} ---")

def detectar_e_processar():
    PASTA_BASE = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
    for pasta in os.listdir(PASTA_BASE):
        if pasta.lower().endswith("_ativo"):
            dataset = gerar_nome_dataset(pasta)
            analisar_monofasicos_cliente(dataset)

if __name__ == "__main__":
    detectar_e_processar()