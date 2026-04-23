# ==============================================================================
# SCRIPT: 00_auditoria_inicial.py
# OBJETIVO: Verificar se as tabelas essenciais existem no dataset do cliente
# ==============================================================================

from google.cloud import bigquery

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
DATASET_CLIENTE = "03_cafe_gourmet_ativo"

client = bigquery.Client(project=PROJECT_ID)

def verificar_base():
    print(f"--- Iniciando auditoria no dataset: {DATASET_CLIENTE} ---")
    
    tabelas_necessarias = ["fato_vendas", "dim_produtos"]
    
    for tabela in tabelas_necessarias:
        tabela_ref = f"{PROJECT_ID}.{DATASET_CLIENTE}.{tabela}"
        try:
            # Tenta pegar a tabela
            tbl = client.get_table(tabela_ref)
            # Verifica se tem linhas
            print(f"Tabela {tabela} encontrada! Linhas estimadas: {tbl.num_rows}")
        except Exception:
            print(f"ALERTA: Tabela {tabela} NÃO encontrada ou vazia no dataset.")

if __name__ == "__main__":
    verificar_base()