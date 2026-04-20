# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: ARQUITETO DE INFRAESTRUTURA (CLONAGEM)
# DATA: 19/04/2026
# VERSÃO: 1.0.0
# OBJETIVO: Clonar tabelas do Dataset Mestre para os Datasets de Clientes
# ==============================================================================

from google.cloud import bigquery

project_id = "v2-gastrobi-lab"
source_dataset_id = "dados_laboratorio"
client = bigquery.Client(project=project_id)

# Lista de datasets de clientes (poderíamos automatizar para ler todos, 
# mas vamos começar com estes 5 que você já criou)
datasets_clientes = [
    "01_restaurante_sergio_ativo",
    "02_pizzaria_verace_ativo",
    "03_cafe_gourmet_ativo",
    "04_churrascaria_premium_ativo",
    "05_churrascaria_top_pausado"
]

def clonar_tabela(tabela_nome, destino_dataset, copiar_dados=False):
    source_table_ref = f"{project_id}.{source_dataset_id}.{tabela_nome}"
    dest_table_ref = f"{project_id}.{destino_dataset}.{tabela_nome}"
    
    # 1. Busca a tabela mestre
    table = client.get_table(source_table_ref)
    
    # 2. Define a nova tabela (clonando o schema)
    new_table = bigquery.Table(dest_table_ref, schema=table.schema)
    
    try:
        client.create_table(new_table)
        print(f"Estrutura da tabela {tabela_nome} criada em {destino_dataset}")
        
        # 3. Se precisar copiar os dados (dim_produtos)
        if copiar_dados:
            job = client.copy_table(source_table_ref, dest_table_ref)
            job.result()
            print(f"Dados da tabela {tabela_nome} copiados para {destino_dataset}")
            
    except Exception as e:
        print(f"Tabela {tabela_nome} já existe em {destino_dataset} ou erro: {e}")

# EXECUÇÃO
for dataset in datasets_clientes:
    print(f"--- Provisionando infraestrutura para: {dataset} ---")
    clonar_tabela("dim_produtos", dataset, copiar_dados=True)  # Copia schema + dados
    clonar_tabela("fato_vendas", dataset, copiar_dados=False)  # Copia apenas schema (tabela vazia)