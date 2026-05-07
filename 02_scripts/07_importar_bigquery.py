# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Enviar dados ao BigQuery resetando a tabela para evitar erro de schema.
# ==============================================================================

import pandas as pd
import os
import re
from google.cloud import bigquery

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def executar():
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    for pasta in pastas:
        dataset_id = gerar_nome_dataset(pasta)
        caminho_csv = os.path.join(PASTA_CLIENTES, pasta, "02_processed", "vendas_final.csv")
        if os.path.exists(caminho_csv):
            try:
                df = pd.read_csv(caminho_csv)
                tabela_id = f"{client.project}.{dataset_id}.tb_vendas_fato"
                client.delete_table(tabela_id, not_found_ok=True)
                df.to_gbq(destination_table=f"{dataset_id}.tb_vendas_fato", if_exists='replace', progress_bar=False)
                print(f">>> [OK] {pasta} no BigQuery.")
            except Exception as e:
                print(f"Erro ao subir {pasta}: {e}")

if __name__ == "__main__":
    executar()