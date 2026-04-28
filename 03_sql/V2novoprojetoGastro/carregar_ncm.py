import pandas as pd
from google.cloud import bigquery
import os

# 1. LOCALIZADOR INTELIGENTE DE PASTAS
# Este comando descobre onde o script está rodando (na G: ou C:)
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))
# Sobe um nível para sair de '03_Codigos' e chegar na raiz do projeto
pasta_raiz = os.path.dirname(diretorio_do_script)

# Define o caminho para a pasta de clientes e o arquivo
caminho_clientes = os.path.join(pasta_raiz, "01_Clientes")
arquivo_ncm = os.path.join(caminho_clientes, "ncm.xlsx.xlsx")

print(f"📡 Iniciando GastroBI Core...")
print(f"📂 Verificando base de dados em: {arquivo_ncm}")

# 2. PROCESSO DE CARGA PARA O BIGQUERY
try:
    # Verifica se o arquivo existe antes de tentar ler
    if not os.path.exists(arquivo_ncm):
        raise FileNotFoundError(f"O arquivo {arquivo_ncm} não foi encontrado.")

    # Lê o Excel
    df = pd.read_excel(arquivo_ncm)
    
    # Configura o Cliente BigQuery
    client = bigquery.Client(project="gastrobi-core-profissional")
    tabela_destino = "gastrobi-core-profissional.raw.ncm_mestre"
    
    # Envia para a nuvem substituindo a anterior (Truncate)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, tabela_destino, job_config=job_config)
    job.result()
    
    print(f"✅ SUCESSO! O backup no Drive está ativo e {len(df)} linhas foram enviadas ao BigQuery.")

except Exception as e:
    print(f"❌ Erro de Percurso: {e}")
 


