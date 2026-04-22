# ==============================================================================
# PROJETO: V2_GASTROBI
# SCRIPT: 04_limpeza_cruzamento.py
# OBJETIVO: Cruzamento limpo (sem _y) e preenchimento de nulos
# ==============================================================================

import pandas as pd
from google.cloud import bigquery

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
DATASET_ORIGEM = "02_pizzaria_verace_ativo"
DATASET_DESTINO = "lab_testes"

client = bigquery.Client(project=PROJECT_ID)

def processar_e_salvar():
    print("--- Iniciando Cruzamento Limpo e Sem Duplicatas ---")
    
    # 1. Puxar as tabelas
    query_fato = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ORIGEM}.fato_vendas`"
    query_dim = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ORIGEM}.dim_produtos`"
    
    df_fato = client.query(query_fato).to_dataframe()
    df_dim = client.query(query_dim).to_dataframe()
    
    # 2. LIMPEZA PREVENTIVA: Remover colunas da FATO que vêm da DIM
    # Isso impede que apareça o _y no final
    colunas_para_remover = ['ncm', 'tributacao', 'custo_unitario', 'valor_gorjeta_padrao']
    for col in colunas_para_remover:
        if col in df_fato.columns:
            df_fato = df_fato.drop(columns=[col])
            
    # 3. Cruzamento
    df_final = pd.merge(df_fato, df_dim, on='nome_produto_normalizado', how='left')
    
    # 4. Tratamento de nulos (Preenchimento)
    # Números viram 0, Texto vira "NÃO_CADASTRADO"
    colunas_numericas = ['custo_unitario', 'valor_gorjeta_padrao']
    colunas_texto = ['ncm', 'tributacao']
    
    for col in colunas_numericas:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna(0)
            
    for col in colunas_texto:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna("NÃO_CADASTRADO")
            
    # 5. Salvar no BigQuery
    table_destino = f"{PROJECT_ID}.{DATASET_DESTINO}.fato_vendas_limpa"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    job = client.load_table_from_dataframe(df_final, table_destino, job_config=job_config)
    job.result()
    
    print(f"Sucesso! Tabela limpa salva em: {table_destino}")
    print(f"Total de linhas processadas: {len(df_final)}")

if __name__ == "__main__":
    processar_e_salvar()