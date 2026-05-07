# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 03_criar_dataset_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Criar datasets no BigQuery para todos os clientes ativos.
# ==============================================================================

import os
import re
from google.cloud import bigquery

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def executar():
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    for pasta in pastas:
        dataset_id = gerar_nome_dataset(pasta)
        full_id = f"{client.project}.{dataset_id}"
        try:
            client.get_dataset(full_id)
            print(f"Dataset já existe: {dataset_id}")
        except:
            dataset = bigquery.Dataset(full_id)
            dataset.location = "US"
            client.create_dataset(dataset)
            print(f"Dataset criado: {dataset_id}")

if __name__ == "__main__":
    executar()