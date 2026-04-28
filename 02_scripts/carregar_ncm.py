import pandas as pd
from google.cloud import bigquery
import os

# 1. DEFINIÇÃO DOS CAMINHOS (A base de tudo)
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))
pasta_raiz = os.path.dirname(diretorio_do_script)

# Aqui definimos a pasta e o arquivo com o nome exato que o Windows criou
caminho_clientes = os.path.join(pasta_raiz, "01_Clientes")
arquivo_alvo = os.path.join(caminho_clientes, "ncm.xlsx.xlsx")

print(f"🔎 Iniciando carga profissional...")
print(f"📂 Verificando arquivo em: {arquivo_alvo}")

# 2. PROCESSO DE CARGA
try:
    # Lendo o arquivo Excel
    df = pd.read_excel(arquivo_alvo)
    
    # Conectando ao BigQuery
    client = bigquery.Client(project="gastrobi-core-profissional")
    tabela_destino = "gastrobi-core-profissional.raw.ncm_mestre"
    
    print("⏳ Enviando dados para a nuvem...")
    job = client.load_table_from_dataframe(df, tabela_destino)
    job.result()  # Aguarda a conclusão
    
    print(f"✅ SUCESSO TOTAL! {len(df)} linhas de NCM carregadas no BigQuery.")

except FileNotFoundError:
    print(f"❌ ERRO: O arquivo 'ncm.xlsx.xlsx' não foi encontrado na pasta 01_Clientes.")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
    # teste
 


