# ------------------------------------------------------------
# PROJETO: GASTROBI
# SCRIPT: 11_importar_gastos.py
# OBJETIVO: Importar aba "Gastos" para BigQuery (via CSV)
# AUTOR: Sergio Santos | Consultoria Estratégica + ChatGPT
# DATA: 30/04/2026
# VERSÃO: 2.1 (Project ID corrigido)
# ------------------------------------------------------------

import pandas as pd
from google.cloud import bigquery
import os

# CONFIGURAÇÃO CORRETA
PROJECT_ID = "v2-gastrobi-lab"
DATASET = "restaurante_teste"
TABELA = "tb_gastos"

CAMINHO_PLANILHA = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes\01_restaurante_teste_ativo\Planilha_Operacional_Restaurante_Teste.xlsx"
CAMINHO_CSV = r"G:\Drives compartilhados\V2_GASTROBI\temp_gastos.csv"

def importar_gastos():
    try:
        print("Iniciando importação de gastos...")

        df = pd.read_excel(CAMINHO_PLANILHA, sheet_name="Gastos")

        print(f"Linhas lidas: {len(df)}")

        df.columns = df.columns.str.strip().str.lower()
        print(f"Colunas encontradas: {list(df.columns)}")

        colunas_necessarias = ["data", "categoria", "valor"]
        for col in colunas_necessarias:
            if col not in df.columns:
                raise Exception(f"Coluna obrigatória não encontrada: {col}")

        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        df = df.dropna(subset=["data", "valor"])
        print(f"Linhas após limpeza: {len(df)}")

        df.to_csv(CAMINHO_CSV, index=False, encoding="utf-8-sig")
        print("Arquivo CSV temporário criado.")

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET}.{TABELA}"

        print(f"Enviando para: {table_id}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        with open(CAMINHO_CSV, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        print("Aguardando BigQuery finalizar...")
        job.result()

        print("Gastos importados com sucesso.")

        os.remove(CAMINHO_CSV)
        print("Arquivo temporário removido.")

    except Exception as e:
        print(f"Erro geral: {e}")

if __name__ == "__main__":
    importar_gastos()