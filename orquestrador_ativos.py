# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: ORQUESTRADOR DE ATIVOS
# OBJETIVO: Sincronização de arquivos raw para BigQuery
# ==============================================================================

import pandas as pd
import os
from google.cloud import bigquery

# Inicializa o cliente do BigQuery
project_id = "v2-gastrobi-lab"
client = bigquery.Client(project=project_id)

# Caminho base dos clientes
base_path = r'G:\Drives compartilhados\V2_GASTROBI\01_clientes'

# Dicionário que ensina ao código como ler cada formato
leitores = {
    '.csv': pd.read_csv,
    '.json': pd.read_json,
    '.xlsx': pd.read_excel,
    '.txt': lambda x: pd.read_csv(x, sep='\t')
}

def carregar_no_bigquery(df, dataset_id, table_name):
    """Tenta carregar. Se a estrutura do arquivo for diferente da tabela, ele reporta e pula."""
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        # Apenas tenta carregar. Se falhar, o 'except' abaixo trata.
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"     [ENTREGUE] Dados enviados para o BigQuery na tabela {table_name}.")
    except Exception as e:
        # Aqui é onde ele não quebra nada. Ele só avisa que não subiu.
        print(f"     [AVISO] Arquivo incompatível com a tabela {table_name}. (Não foi carregado).")

print("--- Iniciando Orquestração Inteligente ---")

for cliente in os.listdir(base_path):
    caminho_cliente = os.path.join(base_path, cliente)
    caminho_raw = os.path.join(caminho_cliente, '01_entrada_raw')
    
    if os.path.isdir(caminho_cliente) and os.path.exists(caminho_raw):
        dataset_id = cliente.lower().replace(" ", "_")
        print(f"\n[CLIENTE: {cliente}]")
        
        for arquivo in os.listdir(caminho_raw):
            caminho_completo = os.path.join(caminho_raw, arquivo)
            extensao = os.path.splitext(arquivo)[1].lower()
            
            if extensao in leitores:
                try:
                    df = leitores[extensao](caminho_completo)
                    carregar_no_bigquery(df, dataset_id, "fato_vendas")
                    print(f"     [SUCESSO] {arquivo} lido.")
                except Exception as e:
                    print(f"     [ERRO] Falha na leitura do arquivo {arquivo}: {e}")
            else:
                print(f"  -> [IGNORADO] Formato não suportado: {arquivo}")

print("\n--- Orquestração Finalizada ---")