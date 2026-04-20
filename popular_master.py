# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: POPULADOR DA TABELA MESTRE
# DATA: 19/04/2026
# VERSÃO: 1.0.0
# OBJETIVO: Inserir dados na dim_produtos via Python (Bypass de restrição SQL)
# AUTOR: Sergio Santos / Gemini Collaborator
# ==============================================================================

import pandas as pd
from google.cloud import bigquery

def popular_dim_produtos():
    project_id = "v2-gastrobi-lab"
    dataset_id = "dados_laboratorio"
    tabela_id = "dim_produtos"
    full_table_id = f"{project_id}.{dataset_id}.{tabela_id}"
    
    # Criando o dado mestre (O seu "De-Para")
    dados = {
        'nome_produto_normalizado': ['Coca-Cola', 'Pizza Calabresa'],
        'ncm': ['22021000', '19059090'],
        'tributacao': ['Monofasico', 'Normal'],
        'custo_unitario': [3.50, 22.00],
        'valor_gorjeta_padrao': [0.0, 0.0]
    }
    
    df = pd.DataFrame(dados)
    
    # Upload para o BigQuery
    client = bigquery.Client(project=project_id)
    job = client.load_table_from_dataframe(df, full_table_id)
    job.result()
    
    print(f"Sucesso! {job.output_rows} produtos cadastrados na tabela {full_table_id}")

if __name__ == "__main__":
    popular_dim_produtos()