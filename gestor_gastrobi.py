# ==============================================================================
# PROJETO: GASTROBI - SISTEMA DE INGESTÃO AUTOMATIZADA
# SCRIPT: gestor_gastrobi.py
# ==============================================================================

import pandas as pd
import sys
import os
from google.cloud import bigquery

# CONFIGURAÇÕES FIXAS
PROJECT_ID = "v2-gastrobi-lab"
CAMINHO_BASE = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
ARQUIVO_MAPA = r"G:\Drives compartilhados\V2_GASTROBI\mapeamento_colunas.csv"
DATASET_DESTINO = "lab_testes"

client = bigquery.Client(project=PROJECT_ID)

def processar_cliente(dataset_id):
    # 1. Verifica se a pasta existe
    pasta_raw = os.path.join(CAMINHO_BASE, dataset_id, "01_entrada_raw")
    if not os.path.exists(pasta_raw):
        return # Pula silenciosamente se não existir

    arquivos = [f for f in os.listdir(pasta_raw) if f.endswith('.csv')]
    if not arquivos: return
    caminho_csv = os.path.join(pasta_raw, arquivos[0])

    # 2. Leitura Robusta (Tenta detectar separador automaticamente)
    try:
        # Tenta com ; e depois com ,
        try:
            df = pd.read_csv(caminho_csv, sep=';', encoding='utf-8-sig')
            if len(df.columns) < 2: raise ValueError
        except:
            df = pd.read_csv(caminho_csv, sep=',', encoding='utf-8-sig')
            
        df.columns = df.columns.str.strip()
        # DELETA COLUNAS "Unnamed" automatico
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    except Exception as e:
        print(f"ERRO ao ler {dataset_id}: {e}")
        return

    # 3. Mapeamento
    mapa = pd.read_csv(ARQUIVO_MAPA, sep=None, engine='python')
    config = mapa[mapa['dataset_id'] == dataset_id].iloc[0]
    col_origem = str(config['coluna_original']).strip()
    col_destino = str(config['coluna_destino']).strip()

    if col_origem not in df.columns:
        print(f"ERRO: Cliente {dataset_id} não tem a coluna '{col_origem}'. Enxergou: {list(df.columns)}")
        return

    # 4. Processamento
    df = df.rename(columns={col_origem: col_destino})
    
    # 5. BigQuery
    table_raw = f"{PROJECT_ID}.{dataset_id}.fato_vendas"
    client.load_table_from_dataframe(df, table_raw, write_disposition="WRITE_TRUNCATE").result()
    
    df_dim = client.query(f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.dim_produtos`").to_dataframe()
    df_final = pd.merge(df, df_dim, on=col_destino, how='left')
    client.load_table_from_dataframe(df_final, f"{PROJECT_ID}.{DATASET_DESTINO}.fato_vendas_limpa", write_disposition="WRITE_TRUNCATE").result()
    print(f"SUCESSO: {dataset_id} processado.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        processar_cliente(sys.argv[1])
    else:
        mapa = pd.read_csv(ARQUIVO_MAPA, sep=None, engine='python')
        for cliente in mapa['dataset_id'].unique():
            processar_cliente(cliente)