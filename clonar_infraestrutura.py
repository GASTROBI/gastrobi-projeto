# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: ARQUITETO DE INFRAESTRUTURA (CLONAGEM AUTOMÁTICA)
# DATA: 21/04/2026
# VERSÃO: 2.1.0
# OBJETIVO: Varre pastas de clientes e provisiona datasets automaticamente
# ==============================================================================

import os
from google.cloud import bigquery

project_id = "v2-gastrobi-lab"
source_dataset_id = "dados_laboratorio"
base_path = r'G:\Drives compartilhados\V2_GASTROBI\01_clientes'
client = bigquery.Client(project=project_id)

def clonar_tabela(tabela_nome, destino_dataset, copiar_dados=False):
    source_table_ref = f"{project_id}.{source_dataset_id}.{tabela_nome}"
    dest_table_ref = f"{project_id}.{destino_dataset}.{tabela_nome}"
    
    # Busca o schema da tabela mestre
    table = client.get_table(source_table_ref)
    
    # Define a nova tabela no cliente
    new_table = bigquery.Table(dest_table_ref, schema=table.schema)
    
    try:
        client.create_table(new_table)
        print(f"    -> Estrutura {tabela_nome} criada em {destino_dataset}")
        
        # Copia dados apenas se for a dimensão
        if copiar_dados:
            job = client.copy_table(source_table_ref, dest_table_ref)
            job.result()
            print(f"    -> Dados da tabela {tabela_nome} copiados.")
            
    except Exception as e:
        print(f"    -> Aviso: Tabela {tabela_nome} já existe ou erro: {e}")

# EXECUÇÃO AUTOMÁTICA
print("--- Iniciando Varredura de Infraestrutura ---")

for cliente_pasta in os.listdir(base_path):
    # Processa apenas se for uma pasta
    if os.path.isdir(os.path.join(base_path, cliente_pasta)):
        dataset_id = cliente_pasta.lower().replace(" ", "_")
        
        # Verifica se o dataset já existe no BQ
        try:
            client.get_dataset(f"{project_id}.{dataset_id}")
            print(f"Infraestrutura ok para: {dataset_id}")
        except:
            print(f"Provisionando novo cliente: {dataset_id}")
            # Cria o dataset
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"
            client.create_dataset(dataset)
            
            # Clona as tabelas
            clonar_tabela("dim_produtos", dataset_id, copiar_dados=True)
            clonar_tabela("fato_vendas", dataset_id, copiar_dados=False)