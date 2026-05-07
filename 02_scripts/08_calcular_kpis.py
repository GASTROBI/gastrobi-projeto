# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 08_calcular_kpis.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# FINALIDADE: Calcular KPIs no BigQuery de forma dinâmica.
#             - Protocolo de Segurança: APENAS LEITURA na tb_vendas_fato.
#             - SQL BLINDADO: Evita erros de nomes de colunas de texto.
# ==========================================================

import os
import re
from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

client = bigquery.Client(project=PROJECT_ID)

def gerar_nome_dataset(nome_pasta):
    """Gera o nome do dataset baseado na pasta do cliente."""
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

def calcular_kpis_cliente(dataset_alvo):
    """Executa o SQL de criação da tabela de KPIs preservando a segurança dos dados."""
    
    # SQL BLINDADO: Focamos nos campos numéricos que já validamos (valor_total e custo_total)
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_alvo}.tb_kpis` AS
    WITH base AS (
      SELECT * FROM `{PROJECT_ID}.{dataset_alvo}.tb_vendas_fato`
    )
    -- KPI 1: FATURAMENTO TOTAL
    SELECT 
        CURRENT_DATE() as data_kpi, 
        'FATURAMENTO_TOTAL' as indicador, 
        ROUND(SUM(valor_total), 2) as valor, 
        'R$' as unidade
    FROM base

    UNION ALL

    -- KPI 2: TICKET MÉDIO
    SELECT 
        CURRENT_DATE(), 
        'TICKET_MEDIO', 
        ROUND(SUM(valor_total) / NULLIF(COUNT(*), 0), 2), 
        'R$'
    FROM base

    UNION ALL

    -- KPI 3: TOTAL DE VENDAS (QTD)
    SELECT 
        CURRENT_DATE(), 
        'TOTAL_VENDAS', 
        CAST(COUNT(*) AS FLOAT64), 
        'UN'
    FROM base

    UNION ALL

    -- KPI 6: PONTO DE EQUILÍBRIO (Baseado em R$ 15.000 fixo)
    -- Protocolo de Segurança: Não altera a tabela original.
    SELECT 
        CURRENT_DATE(), 
        'PONTO_EQUILIBRIO',
        ROUND(
            15000 / 
            NULLIF(( (SUM(valor_total) - SUM(custo_total)) / NULLIF(SUM(valor_total), 0) ), 0)
        , 2),
        'R$'
    FROM base
    """
    
    query_job = client.query(sql)
    query_job.result()
    print(f">>> KPIs calculados com sucesso para o dataset: {dataset_alvo}")

def detectar_e_processar():
    """Busca o cliente ativo para rodar o cálculo manual se necessário."""
    for pasta in os.listdir(PASTA_CLIENTES):
        if pasta.lower().endswith("_ativo"):
            dataset = gerar_nome_dataset(pasta)
            calcular_kpis_cliente(dataset)
            return

if __name__ == "__main__":
    detectar_e_processar()