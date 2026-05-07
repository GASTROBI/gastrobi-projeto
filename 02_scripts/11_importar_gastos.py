# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 11_importar_gastos.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# FINALIDADE: Importar gastos operacionais (CSV, TXT ou XLSX)
#             - Protocolo de Segurança: Se não houver arquivo, pula.
#             - Inteligência: Detecta o formato do arquivo sozinho.
# ==========================================================

import pandas as pd
from google.cloud import bigquery
import os
import re

# CONFIGURAÇÃO
PROJECT_ID = "v2-gastrobi-lab"
TABELA = "tb_gastos"

client = bigquery.Client(project=PROJECT_ID)

def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

def importar_gastos_cliente(caminho_cliente, nome_pasta):
    """Detecta arquivos de gastos (multiformato) e importa para o BigQuery."""
    dataset = gerar_nome_dataset(nome_pasta)
    table_id = f"{PROJECT_ID}.{dataset}.{TABELA}"
    
    # Busca arquivos que contenham 'gasto' no nome e sejam CSV, TXT ou XLSX
    extensoes_suportadas = (".csv", ".txt", ".xlsx")
    arquivos_gastos = [f for f in os.listdir(caminho_cliente) 
                       if 'gasto' in f.lower() and f.endswith(extensoes_suportadas) and "~$" not in f]
    
    if not arquivos_gastos:
        print(f"--- Aviso: Nenhum arquivo de GASTOS encontrado para {dataset}. Pulando etapa ---")
        return

    try:
        caminho_file = os.path.join(caminho_cliente, arquivos_gastos[0])
        extensao = os.path.splitext(arquivos_gastos[0])[1].lower()
        
        print(f"Detectado arquivo {extensao}: {arquivos_gastos[0]}")

        # Lógica de leitura baseada na extensão
        if extensao == ".xlsx":
            df = pd.read_excel(caminho_file) # Tenta ler a primeira aba por padrão
        else:
            # Para CSV ou TXT, tenta detectar o separador automaticamente
            df = pd.read_csv(caminho_file, sep=None, engine='python', encoding='utf-8-sig')

        # Normalização Padrão GastroBI
        df.columns = df.columns.str.strip().str.lower()
        
        colunas_necessarias = ["data", "categoria", "valor"]
        for col in colunas_necessarias:
            if col not in df.columns:
                print(f"Erro: Coluna '{col}' não encontrada no arquivo de {dataset}")
                return

        # Limpeza rápida
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.dropna(subset=["data", "valor"])

        # Envio para o BigQuery (via DataFrame direto para simplificar o multiformato)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f">>> Sucesso: {len(df)} linhas de gastos importadas para {dataset}.")

    except Exception as e:
        print(f"Falha ao processar gastos de {dataset}: {e}")

if __name__ == "__main__":
    PASTA_BASE = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
    for pasta in os.listdir(PASTA_BASE):
        if pasta.lower().endswith("_ativo"):
            importar_gastos_cliente(os.path.join(PASTA_BASE, pasta), pasta)