# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Corrigir erro pyarrow / bytestring ao importar vendas
# para BigQuery convertendo tipos incompatíveis.
# ==========================================================

import os
import re
import pandas as pd
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
        if pasta.endswith("_ativo"):
            return pasta

    return None

# ==========================================================
# DATASET
# ==========================================================
def nome_dataset(cliente):

    nome = cliente.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "")
    nome = re.sub(r"[^a-z0-9_]", "", nome)

    return nome

# ==========================================================
# CARREGAR PICKLES
# ==========================================================
def carregar():

    produtos = pd.read_pickle("produtos_tratado.pkl")
    vendas = pd.read_pickle("vendas_tratado.pkl")

    return produtos, vendas

# ==========================================================
# CONVERTER PRODUTOS
# ==========================================================
def preparar_produtos(df):

    for col in df.columns:

        if "data" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        elif df[col].dtype == "object":
            df[col] = df[col].astype(str)

    numeros = [
        "custo_unitario",
        "preco_venda",
        "valor_gorjeta_padrao"
    ]

    for col in numeros:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

# ==========================================================
# CONVERTER VENDAS
# ==========================================================
def preparar_vendas(df):

    # remove colunas técnicas do merge
    remover = [
        "produto_key",
        "nome_produto_normalizado"
    ]

    for col in remover:
        if col in df.columns:
            df = df.drop(columns=col)

    for col in df.columns:

        if "data" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        elif df[col].dtype == "object":
            df[col] = df[col].astype(str)

    numeros = [
        "quantidade",
        "valor_total",
        "custo_unitario",
        "custo_total",
        "valor_gorjeta",
        "preco_venda"
    ]

    for col in numeros:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df

# ==========================================================
# IMPORTAR
# ==========================================================
def subir(df, destino):

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df,
        destino,
        job_config=job_config
    )

    job.result()

# ==========================================================
# MAIN
# ==========================================================
def main():

    try:

        cliente = detectar_cliente()

        if not cliente:
            print("Cliente ativo não encontrado.")
            return

        dataset = nome_dataset(cliente)

        produtos, vendas = carregar()

        produtos = preparar_produtos(produtos)
        vendas = preparar_vendas(vendas)

        tabela_produtos = f"{PROJECT_ID}.{dataset}.tb_produto_dim"
        tabela_vendas = f"{PROJECT_ID}.{dataset}.tb_vendas_fato"

        subir(produtos, tabela_produtos)
        print("Produtos importados com sucesso.")

        subir(vendas, tabela_vendas)
        print("Vendas importadas com sucesso.")

        print("Importação concluída com sucesso.")

    except Exception as erro:

        print("Erro geral:", erro)

# ==========================================================
# EXECUTAR
# ==========================================================
if __name__ == "__main__":
    main()