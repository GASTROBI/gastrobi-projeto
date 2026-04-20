# GastroBI_Core - Automação de Infraestrutura
# Objetivo: Criar as 3 camadas de dados (Raw, Staging, Analytics) no BigQuery.

from google.cloud import bigquery

client = bigquery.Client(project="gastrobi-core-profissional")

# Lista dos nomes dos conjuntos de dados que vamos criar
datasets_para_criar = ["raw", "staging", "analytics"]

print("🚀 Iniciando criação da infraestrutura GastroBI...")

for nome in datasets_para_criar:
    dataset_id = f"{client.project}.{nome}"
    
    # Criamos um objeto Dataset com a localização em São Paulo (Brasil)
    ds = bigquery.Dataset(dataset_id)
    ds.location = "southamerica-east1" 
    
    try:
        client.create_dataset(ds, exists_ok=True) # Cria se não existir
        print(f"✅ Dataset '{nome}' está pronto e operante em São Paulo.")
    except Exception as e:
        print(f"❌ Erro ao criar {nome}: {e}")

print("\n🎯 Infraestrutura de dados concluída com sucesso!")