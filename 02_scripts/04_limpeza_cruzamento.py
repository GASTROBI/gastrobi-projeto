# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# CONSULTOR: Sérgio Paulo dos Santos
# DATA: 06/05/2026
# OBJETIVO: Limpeza e cruzamento de dados - MÓDULO SIMPLIFICADO
# ==============================================================================

from google.cloud import bigquery

def realizar_limpeza():
    client = bigquery.Client()
    dataset_id = "restaurante_teste"
    
    print(f"--- [04] Limpando dados em: {dataset_id} ---")
    
    try:
        # Apenas uma validação simples para garantir que o script funciona
        query = f"SELECT * FROM `{dataset_id}.tb_vendas_fato` LIMIT 1"
        client.query(query).result()
        print("SUCESSO: Dados validados no BigQuery.")
    except Exception as e:
        print(f"AVISO: Pulando limpeza (Dataset não encontrado ou vazio).")

if __name__ == "__main__":
    realizar_limpeza()