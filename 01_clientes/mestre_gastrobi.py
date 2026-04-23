import pandas as pd
import os
import sys
from google.cloud import bigquery

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
# O script assume que está na pasta G:\Drives compartilhados\V2_GASTROBI\
PASTA_RAIZ = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_MAPA = os.path.join(PASTA_RAIZ, "mapeamento_colunas.csv")
PASTA_CLIENTES = os.path.join(PASTA_RAIZ, "01_clientes")
DATASET_DESTINO = "lab_testes"

client = bigquery.Client(project=PROJECT_ID)

def rodar_processamento(dataset_id):
    print(f"--- Iniciando processamento de: {dataset_id} ---")
    
    # 1. Leitura do Mapa
    if not os.path.exists(ARQUIVO_MAPA):
        print(f"ERRO: Não achei o mapeamento_colunas.csv em {ARQUIVO_MAPA}")
        return

    mapa = pd.read_csv(ARQUIVO_MAPA)
    config = mapa[mapa['dataset_id'] == dataset_id]
    
    if config.empty:
        print(f"ERRO: Cliente {dataset_id} não cadastrado no mapeamento_colunas.csv")
        return
    config = config.iloc[0]

    # 2. Localiza arquivo
    pasta_raw = os.path.join(PASTA_CLIENTES, dataset_id, "01_entrada_raw")
    if not os.path.exists(pasta_raw):
        print(f"ERRO: Pasta {pasta_raw} não existe.")
        return
        
    arquivos = [f for f in os.listdir(pasta_raw) if f.endswith('.csv')]
    if not arquivos:
        print(f"ERRO: Nenhum arquivo CSV encontrado em {pasta_raw}")
        return
    
    # 3. Processa
    caminho_csv = os.path.join(pasta_raw, arquivos[0])
    df = pd.read_csv(caminho_csv)
    
    col_origem = config['coluna_original']
    if col_origem not in df.columns:
        print(f"ERRO: A coluna '{col_origem}' (que consta no mapa) não existe no CSV do cliente.")
        print(f"Colunas encontradas no arquivo: {list(df.columns)}")
        return

    df = df.rename(columns={col_origem: config['coluna_destino']})
    
    # 4. Envio para BigQuery
    table_raw = f"{PROJECT_ID}.{dataset_id}.fato_vendas"
    client.load_table_from_dataframe(df, table_raw, write_disposition="WRITE_TRUNCATE").result()
    
    df_dim = client.query(f"SELECT * FROM `{PROJECT_ID}.{dataset_id}.dim_produtos`").to_dataframe()
    df_final = pd.merge(df, df_dim, on=config['coluna_destino'], how='left')
    client.load_table_from_dataframe(df_final, f"{PROJECT_ID}.{DATASET_DESTINO}.fato_vendas_limpa", write_disposition="WRITE_TRUNCATE").result()
    
    print(f"SUCESSO: {dataset_id} enviado com sucesso.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        rodar_processamento(sys.argv[1])
    else:
        print("Digite o nome da pasta do cliente no terminal.")