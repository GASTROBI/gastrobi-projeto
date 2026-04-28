import pandas as pd
from google.cloud import bigquery
import os

# 1. DEFINIÇÃO DOS CAMINHOS
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))
pasta_raiz = os.path.dirname(diretorio_do_script)
caminho_vendas = os.path.join(pasta_raiz, "01_Clientes", "vendas.xlsx.xlsx")

print(f"🔎 Iniciando carga de Vendas com saneamento...")

try:
    # 2. Lendo o Excel
    df = pd.read_excel(caminho_vendas)
    
    # 🌟 MÁGICA: Limpando os nomes das colunas (tira parênteses, espaços e acentos)
    # Isso transforma "NCM (Simulado)" em "NCM_Simulado"
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_') for c in df.columns]
    
    print(f"✅ Colunas saneadas: {list(df.columns)}")

    # 3. Conectando ao BigQuery
    client = bigquery.Client(project="gastrobi-core-profissional")
    tabela_destino = "gastrobi-core-profissional.raw.vendas_brutas"
    
    print("⏳ Enviando vendas saneadas para a nuvem...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    job = client.load_table_from_dataframe(df, tabela_destino, job_config=job_config)
    job.result()
    
    print(f"🚀 SUCESSO! {len(df)} linhas de vendas agora estão no BigQuery.")

except Exception as e:
    print(f"❌ Erro: {e}")