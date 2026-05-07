# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-07
# FINALIDADE:
# Importar dados tratados para o BigQuery.
# Inteligente: Detecta se há produtos e vendas ou apenas vendas (CSV).
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
# DETECTAR CLIENTE ATIVO
# ==========================================================
def detectar_cliente():
    try:
        for pasta in os.listdir(PASTA_CLIENTES):
            if pasta.lower().strip().endswith("_ativo"):
                return pasta
    except:
        return None
    return None

# ==========================================================
# GERAR NOME DO DATASET (PADRONIZADO)
# ==========================================================
def nome_dataset(cliente):
    nome = cliente.lower()
    nome = re.sub(r"^\d+_", "", nome) # Remove 01_, 02_
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

# ==========================================================
# PREPARAR DADOS PARA O GOOGLE (TIPOS DE DADOS)
# ==========================================================
def preparar_dataframe(df, colunas_numericas):
    # Converte colunas de data
    for col in df.columns:
        if "data" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif df[col].dtype == "object":
            df[col] = df[col].astype(str)

    # Converte colunas numéricas
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Remove colunas técnicas que não devem ir para o BigQuery
    remover = ["produto_key", "nome_produto_normalizado"]
    for col in remover:
        if col in df.columns:
            df = df.drop(columns=col)
            
    return df

# ==========================================================
# SUBIR PARA O BIGQUERY
# ==========================================================
def subir(df, tabela_id):
    # WRITE_TRUNCATE limpa a tabela antes de subir (Ideal para o Dashboard atual)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        job = client.load_table_from_dataframe(df, tabela_id, job_config=job_config)
        job.result()
        print(f"Sucesso: Dados enviados para {tabela_id}")
    except Exception as e:
        print(f"Erro ao subir para {tabela_id}: {e}")

# ==========================================================
# MAIN
# ==========================================================
def main():
    try:
        cliente = detectar_cliente()
        if not cliente:
            print("Nenhum cliente ativo encontrado.")
            return

        dataset = nome_dataset(cliente)
        print(f"Dataset alvo: {dataset}")

        # 1. IMPORTAR VENDAS (Sempre existe)
        if os.path.exists("vendas_tratado.pkl"):
            vendas = pd.read_pickle("vendas_tratado.pkl")
            cols_vendas = ["quantidade", "valor_total", "custo_unitario", "custo_total", "valor_gorjeta"]
            vendas = preparar_dataframe(vendas, cols_vendas)
            
            tabela_vendas = f"{PROJECT_ID}.{dataset}.tb_vendas_fato"
            subir(vendas, tabela_vendas)
        else:
            print("Arquivo de vendas tratado não encontrado.")

        # 2. IMPORTAR PRODUTOS (Pode não existir em cargas via CSV)
        if os.path.exists("produtos_tratado.pkl"):
            produtos = pd.read_pickle("produtos_tratado.pkl")
            cols_prod = ["custo_unitario", "preco_venda", "valor_gorjeta_padrao"]
            produtos = preparar_dataframe(produtos, cols_prod)
            
            tabela_produtos = f"{PROJECT_ID}.{dataset}.tb_produtos_dim"
            subir(produtos, tabela_produtos)
        else:
            print("Info: Sem arquivo de produtos para importar (comum em fluxos CSV).")

    except Exception as erro:
        print("Erro geral no script 07:", erro)

if __name__ == "__main__":
    main()