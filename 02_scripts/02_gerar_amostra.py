# ==============================================================================
# PROJETO: V2_GASTROBI
# SCRIPT: 02_gerar_amostra_DEBUG.py
# OBJETIVO: Popular o laboratório com 30 linhas de teste e validar a carga
# ==============================================================================

from google.cloud import bigquery
import pandas as pd

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
DATASET_ORIGEM = "08_espetaria_gauchaii_ativo"
DATASET_DESTINO = "lab_testes"
TABELA_ORIGEM = "fato_vendas"
TABELA_DESTINO = "fato_vendas"

client = bigquery.Client(project=PROJECT_ID)

def gerar_amostra():
    print(f"--- Iniciando extração de amostra ---")
    
    # Query de extração
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ORIGEM}.{TABELA_ORIGEM}`
        LIMIT 30
    """
    
    try:
        # Pega os dados
        df = client.query(query).to_dataframe()
        
        # DEBUG: Imprimir se veio algo
        print(f"Linhas encontradas na produção: {len(df)}")
        
        if len(df) == 0:
            print("ERRO: A query retornou zero linhas. Verifique se a tabela de origem tem dados.")
            return

        # Envia para o laboratório
        destino_ref = f"{PROJECT_ID}.{DATASET_DESTINO}.{TABELA_DESTINO}"
        
        # Configuração de carga para substituir caso já exista a tabela no lab
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = client.load_table_from_dataframe(df, destino_ref, job_config=job_config)
        job.result()
        
        print(f"SUCESSO: 30 linhas carregadas com sucesso em {destino_ref}.")
        
    except Exception as e:
        print(f"ERRO CRÍTICO: {e}")

if __name__ == "__main__":
    gerar_amostra()