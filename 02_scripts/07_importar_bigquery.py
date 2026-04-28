# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Importar planilha operacional para BigQuery
# usando CSV temporário (mais estável que pyarrow)
# ==========================================================

import os
import re
import pandas as pd
from datetime import datetime
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
def detectar_cliente_ativo():

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
# LIMPAR COLUNAS
# ==========================================================
def limpar_colunas(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    return df

# ==========================================================
# IMPORTAR VIA CSV
# ==========================================================
def importar_csv(df, tabela):

    arquivo_temp = "temp_upload.csv"

    df.to_csv(arquivo_temp, index=False, encoding="utf-8-sig")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition="WRITE_TRUNCATE"
    )

    with open(arquivo_temp, "rb") as arquivo:
        job = client.load_table_from_file(
            arquivo,
            tabela,
            job_config=job_config
        )

    job.result()

    os.remove(arquivo_temp)

# ==========================================================
# VENDAS
# ==========================================================
def importar_vendas(arquivo, dataset):

    df = pd.read_excel(arquivo, sheet_name="Vendas")
    df = limpar_colunas(df)
    df = df.dropna(how="all")

    novo = pd.DataFrame()

    novo["data"] = pd.to_datetime(df["data"]).dt.strftime("%Y-%m-%d")
    novo["produto_original"] = df["produto"]
    novo["nome_produto_normalizado"] = df["produto"]
    novo["categoria"] = ""
    novo["qtd"] = df["quantidade"]
    novo["valor_unitario"] = df["valor_total"] / df["quantidade"]
    novo["faturamento_bruto"] = df["valor_total"]
    novo["desconto"] = 0
    novo["valor_liquido"] = df["valor_total"]
    novo["forma_pagamento"] = df["forma_pagamento"]
    novo["canal_venda"] = df["canal_venda"]
    novo["ncm"] = ""
    novo["tributacao"] = ""
    novo["custo_unitario"] = 0
    novo["custo_total"] = 0
    novo["valor_gorjeta_padrao"] = 0
    novo["arquivo_origem"] = "Planilha_Operacional_Restaurante_Teste.xlsx"
    novo["data_importacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    novo["cliente"] = dataset

    tabela = f"{PROJECT_ID}.{dataset}.tb_vendas_fato"

    importar_csv(novo, tabela)

    print("Vendas importadas com sucesso.")

# ==========================================================
# PRODUTOS
# ==========================================================
def importar_produtos(arquivo, dataset):

    df = pd.read_excel(arquivo, sheet_name="Produtos")
    df = limpar_colunas(df)
    df = df.dropna(how="all")

    novo = pd.DataFrame()

    novo["nome_produto_normalizado"] = df["produto"]
    novo["produto_original"] = df["produto"]
    novo["categoria"] = df["categoria"]
    novo["subcategoria"] = ""
    novo["ncm"] = ""
    novo["tributacao"] = ""
    novo["monofasico"] = "false"
    novo["custo_unitario"] = 0
    novo["preco_venda"] = df["preco_venda"]
    novo["valor_gorjeta_padrao"] = 0
    novo["ativo"] = "true"
    novo["data_atualizacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    tabela = f"{PROJECT_ID}.{dataset}.tb_produtos_dim"

    importar_csv(novo, tabela)

    print("Produtos importados com sucesso.")

# ==========================================================
# EXECUTAR
# ==========================================================
def executar():

    try:

        cliente = detectar_cliente_ativo()

        if not cliente:
            print("Nenhum cliente ativo.")
            return

        dataset = gerar_dataset(cliente)

        pasta_cliente = os.path.join(PASTA_CLIENTES, cliente)

        arquivo = os.path.join(
            pasta_cliente,
            "Planilha_Operacional_Restaurante_Teste.xlsx"
        )

        importar_vendas(arquivo, dataset)
        importar_produtos(arquivo, dataset)

        print("Importação concluída com sucesso.")

    except Exception as erro:
        print("Erro geral:", erro)

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":

    executar()