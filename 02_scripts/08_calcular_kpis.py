# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 08_calcular_kpis.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Calcular KPIs no BigQuery FREE TIER
# sem INSERT / DELETE / UPDATE
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

# ==========================================================
# CLIENTE ATIVO
# ==========================================================
def detectar_cliente():

    for pasta in os.listdir(PASTA_CLIENTES):

        caminho = os.path.join(PASTA_CLIENTES, pasta)

        if os.path.isdir(caminho):
            if pasta.lower().endswith("_ativo"):
                return pasta

    return None

# ==========================================================
# DATASET
# ==========================================================
def gerar_dataset(nome):

    nome = nome.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "")
    nome = re.sub(r"[^a-z0-9_]", "", nome)

    return nome

# ==========================================================
# EXECUTA SQL
# ==========================================================
def rodar(sql):

    job = client.query(sql)
    job.result()

# ==========================================================
# CRIAR TABELA KPI
# ==========================================================
def criar_kpis(dataset):

    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset}.tb_kpis` AS

    -- KPI 1 FATURAMENTO
    SELECT
    CURRENT_DATE() data_kpi,
    'FATURAMENTO_TOTAL' indicador,
    ROUND(SUM(valor_liquido),2) valor,
    'R$' unidade

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`

    UNION ALL

    -- KPI 2 TICKET MÉDIO
    SELECT
    CURRENT_DATE(),
    'TICKET_MEDIO',
    ROUND(SUM(valor_liquido)/COUNT(*),2),
    'R$'

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`

    UNION ALL

    -- KPI 3 TOTAL VENDAS
    SELECT
    CURRENT_DATE(),
    'TOTAL_VENDAS',
    COUNT(*),
    'UN'

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`

    UNION ALL

    -- KPI 4 VENDAS IFOOD
    SELECT
    CURRENT_DATE(),
    'VENDAS_IFOOD',
    ROUND(SUM(valor_liquido),2),
    'R$'

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`
    WHERE UPPER(canal_venda) = 'IFOOD'

    UNION ALL

    -- KPI 5 VENDAS BALCÃO
    SELECT
    CURRENT_DATE(),
    'VENDAS_BALCAO',
    ROUND(SUM(valor_liquido),2),
    'R$'

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`
    WHERE UPPER(canal_venda) = 'BALCÃO'

    UNION ALL

    -- KPI 6 PONTO DE EQUILÍBRIO
    SELECT
    CURRENT_DATE(),
    'PONTO_EQUILIBRIO',

    ROUND(
        15000 /
        (
            (SUM(valor_liquido) - SUM(custo_total))
            / NULLIF(SUM(valor_liquido),0)
        )
    ,2),

    'R$'

    FROM `{PROJECT_ID}.{dataset}.tb_vendas_fato`
    """

    rodar(sql)

# ==========================================================
# EXECUTAR
# ==========================================================
def executar():

    try:

        cliente = detectar_cliente()

        if not cliente:
            print("Nenhum cliente ativo.")
            return

        dataset = gerar_dataset(cliente)

        criar_kpis(dataset)

        print("KPIs criados com sucesso.")

    except Exception as erro:
        print("Erro geral:", erro)

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":

    executar()