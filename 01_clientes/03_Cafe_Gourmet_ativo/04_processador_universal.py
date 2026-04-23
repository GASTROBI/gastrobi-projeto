# ==============================================================================
# PROJETO: V2_GASTROBI
# SCRIPT: 04_processador_universal.py
# OBJETIVO: Processamento de limpeza e carga Silver
# MODO DE USAR: Altere a linha 'DATASET_ORIGEM' para o cliente desejado.
# ==============================================================================

import pandas as pd
from google.cloud import bigquery

# --- CONFIGURAÇÃO ---
DATASET_ORIGEM = "08_espetaria_gauchaii_ativo"
# --------------------

PROJECT_ID = "v2-gastrobi-lab"
DATASET_DESTINO = "lab_testes"

client = bigquery.Client(project=PROJECT_ID)

def processar_e_salvar():
    print(f"--- Processando: {DATASET_ORIGEM} ---")
    
    # 1. Puxar dados
    query_fato = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ORIGEM}.fato_vendas`"
    query_dim = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ORIGEM}.dim_produtos`"
    
    df_fato = client.query(query_fato).to_dataframe()
    df_dim = client.query(query_dim).to_dataframe()
    
    # 2. Limpeza preventiva
    colunas_para_remover = ['ncm', 'tributacao', 'custo_unitario', 'valor_gorjeta_padrao']
    for col in colunas_para_remover:
        if col in df_fato.columns:
            df_fato = df_fato.drop(columns=[col])
            
    # 3. Cruzamento
    df_final = pd.merge(df_fato, df_dim, on='nome_produto_normalizado', how='left')
    
    # 4. Tratamento de nulos
    for col in ['custo_unitario', 'valor_gorjeta_padrao']:
        if col in df_final.columns: df_final[col] = df_final[col].fillna(0)
    for col in ['ncm', 'tributacao']:
        if col in df_final.columns: df_final[col] = df_final[col].fillna("NÃO_CADASTRADO")
            
    # 5. Salvar
    table_destino = f"{PROJECT_ID}.{DATASET_DESTINO}.fato_vendas_limpa"
    job = client.load_table_from_dataframe(df_final, table_destino, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    
    print(f"Sucesso! Dados processados com sucesso.")

if __name__ == "__main__":
    processar_e_salvar()