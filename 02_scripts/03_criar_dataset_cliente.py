# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 03_criar_dataset_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-07
# FINALIDADE:
# Detectar o cliente ativo nas pastas e criar o dataset 
# correspondente no BigQuery de forma automática.
# ==========================================================

import os
import re
from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# ==========================================================
# FUNÇÃO PARA DETECTAR O CLIENTE ATIVO (Igual ao Script 02)
# ==========================================================
def detectar_cliente_ativo():
    try:
        pastas = os.listdir(PASTA_CLIENTES)
        for pasta in pastas:
            caminho = os.path.join(PASTA_CLIENTES, pasta)
            if os.path.isdir(caminho):
                if pasta.lower().strip().endswith("_ativo"):
                    return pasta
        return None
    except Exception as erro:
        print("Erro ao acessar pasta de clientes:", erro)
        return None

# ==========================================================
# FUNÇÃO PARA LIMPAR E GERAR O NOME DO DATASET
# ==========================================================
def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    # Remove prefixos como 01_, 02_
    nome = re.sub(r"^\d+_", "", nome)
    # Remove a palavra cliente e o sufixo _ativo
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    # Mantém apenas letras, números e underline
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

# ==========================================================
# FUNÇÃO PRINCIPAL: CRIAR DATASET NO BIGQUERY
# ==========================================================
def executar_criacao():
    try:
        # 1. Detecta quem é o cliente na pasta
        pasta_cliente = detectar_cliente_ativo()
        
        if not pasta_cliente:
            print("Nenhum cliente ativo encontrado nas pastas.")
            return

        # 2. Transforma o nome da pasta em nome de Dataset
        dataset_nome = gerar_nome_dataset(pasta_cliente)
        
        client = bigquery.Client(project=PROJECT_ID)
        dataset_id = f"{PROJECT_ID}.{dataset_nome}"

        # 3. Tenta criar no BigQuery
        try:
            client.get_dataset(dataset_id)
            print(f"Dataset já existe no BigQuery: {dataset_nome}")
        except:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            client.create_dataset(dataset)
            print(f"Dataset criado com sucesso no BigQuery: {dataset_nome}")
            
    except Exception as erro:
        print("Erro ao processar criação de dataset:", erro)

# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":
    executar_criacao()