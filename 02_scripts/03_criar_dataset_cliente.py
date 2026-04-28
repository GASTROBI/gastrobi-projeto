# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 03_criar_dataset_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Receber o nome do cliente ativo identificado pelo Script 02,
# padronizar o nome e criar automaticamente o dataset no BigQuery.
#
# EXEMPLO:
# 01_restaurante_teste_ativo
# vira
# restaurante_teste
# ==========================================================

import re
from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"

# ==========================================================
# FUNÇÃO LIMPAR NOME DO DATASET
# ==========================================================
def gerar_nome_dataset(nome_cliente):

    nome = nome_cliente.lower()

    # remove prefixo numérico inicial
    nome = re.sub(r"^\d+_", "", nome)

    # remove final _ativo
    nome = nome.replace("_ativo", "")

    # mantém somente letras, números e underline
    nome = re.sub(r"[^a-z0-9_]", "", nome)

    return nome


# ==========================================================
# FUNÇÃO CRIAR DATASET
# ==========================================================
def criar_dataset(nome_cliente):

    try:

        client = bigquery.Client(project=PROJECT_ID)

        dataset_nome = gerar_nome_dataset(nome_cliente)

        dataset_id = f"{PROJECT_ID}.{dataset_nome}"

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"

        try:
            client.get_dataset(dataset_id)
            print(f"Dataset já existe: {dataset_nome}")

        except:
            client.create_dataset(dataset)
            print(f"Dataset criado com sucesso: {dataset_nome}")

        return dataset_nome

    except Exception as erro:
        print("Erro ao criar dataset:", erro)
        return None


# ==========================================================
# TESTE LOCAL
# ==========================================================
if __name__ == "__main__":

    cliente = "01_restaurante_teste_ativo"

    criar_dataset(cliente)