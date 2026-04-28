# ==========================================================
# GASTROBI V1
# SCRIPT ÚNICO - CRIAR 3 TABELAS PADRÃO NO BIGQUERY
# tb_vendas_fato
# tb_produtos_dim
# tb_kpis
# ==========================================================

from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES (AJUSTAR)
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"
DATASET_ID = "modelo_gastrobi"

# ==========================================================
# CLIENTE BIGQUERY
# ==========================================================
client = bigquery.Client(project=PROJECT_ID)

dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

# ==========================================================
# CRIAR DATASET SE NÃO EXISTIR
# ==========================================================
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "US"

try:
    client.get_dataset(dataset_ref)
    print("Dataset já existe.")
except:
    client.create_dataset(dataset)
    print("Dataset criado com sucesso.")

# ==========================================================
# FUNÇÃO CRIAR TABELA
# ==========================================================
def criar_tabela(nome_tabela, schema):

    table_id = f"{dataset_ref}.{nome_tabela}"
    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
        print(f"Tabela {nome_tabela} já existe.")
    except:
        client.create_table(table)
        print(f"Tabela {nome_tabela} criada com sucesso.")

# ==========================================================
# 1. TABELA FATO - VENDAS
# ==========================================================
schema_vendas = [

    bigquery.SchemaField("data", "DATE"),
    bigquery.SchemaField("produto_original", "STRING"),
    bigquery.SchemaField("nome_produto_normalizado", "STRING"),
    bigquery.SchemaField("categoria", "STRING"),
    bigquery.SchemaField("qtd", "INT64"),
    bigquery.SchemaField("valor_unitario", "NUMERIC"),
    bigquery.SchemaField("faturamento_bruto", "NUMERIC"),
    bigquery.SchemaField("desconto", "NUMERIC"),
    bigquery.SchemaField("valor_liquido", "NUMERIC"),
    bigquery.SchemaField("forma_pagamento", "STRING"),
    bigquery.SchemaField("canal_venda", "STRING"),
    bigquery.SchemaField("ncm", "STRING"),
    bigquery.SchemaField("tributacao", "STRING"),
    bigquery.SchemaField("custo_unitario", "NUMERIC"),
    bigquery.SchemaField("custo_total", "NUMERIC"),
    bigquery.SchemaField("valor_gorjeta_padrao", "NUMERIC"),
    bigquery.SchemaField("arquivo_origem", "STRING"),
    bigquery.SchemaField("data_importacao", "TIMESTAMP"),
    bigquery.SchemaField("cliente", "STRING"),

]

# ==========================================================
# 2. TABELA DIMENSÃO PRODUTOS
# ==========================================================
schema_produtos = [

    bigquery.SchemaField("nome_produto_normalizado", "STRING"),
    bigquery.SchemaField("produto_original", "STRING"),
    bigquery.SchemaField("categoria", "STRING"),
    bigquery.SchemaField("subcategoria", "STRING"),
    bigquery.SchemaField("ncm", "STRING"),
    bigquery.SchemaField("tributacao", "STRING"),
    bigquery.SchemaField("monofasico", "BOOL"),
    bigquery.SchemaField("custo_unitario", "NUMERIC"),
    bigquery.SchemaField("preco_venda", "NUMERIC"),
    bigquery.SchemaField("valor_gorjeta_padrao", "NUMERIC"),
    bigquery.SchemaField("ativo", "BOOL"),
    bigquery.SchemaField("data_atualizacao", "TIMESTAMP"),

]

# ==========================================================
# 3. TABELA KPI
# ==========================================================
schema_kpis = [

    bigquery.SchemaField("data_ref", "DATE"),
    bigquery.SchemaField("kpi_nome", "STRING"),
    bigquery.SchemaField("valor", "NUMERIC"),
    bigquery.SchemaField("meta", "NUMERIC"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("observacao", "STRING"),
    bigquery.SchemaField("data_calculo", "TIMESTAMP"),
    bigquery.SchemaField("cliente", "STRING"),

]

# ==========================================================
# CRIAR TABELAS
# ==========================================================
criar_tabela("tb_vendas_fato", schema_vendas)
criar_tabela("tb_produtos_dim", schema_produtos)
criar_tabela("tb_kpis", schema_kpis)

print("Processo finalizado com sucesso.")