# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/08_calcular_kpis.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 12/05/2026
# VERSÃO: 2.1 - Processamento de Inteligência Multicliente
# OBJETIVO: Calcular faturamento e indicadores mesmo com custos zerados.
# ==============================================================================

from google.cloud import bigquery
import os

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
PASTA_RAIZ = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
client = bigquery.Client(project=PROJECT_ID)

def calcular_kpis():
    print("="*80)
    print(f"{'ENGINE DE INTELIGÊNCIA GASTROBI V2 - CÁLCULO DE KPIs':^80}")
    print("="*80)
    
    pastas_ativas = [p for p in os.listdir(PASTA_RAIZ) if "_ativo" in p.lower()]
    
    for pasta in pastas_ativas:
        partes = pasta.lower().split("_")
        if "cliente" in partes:
            idx = partes.index("cliente")
            id_bruto = partes[idx+1:-1]
            dataset_id = "_".join(id_bruto)
        else:
            dataset_id = pasta.split("_")[2].lower() if len(partes) > 2 else pasta.lower()

        print(f">>> Processando KPIs: {dataset_id.upper():<25}", end="\r")
        
        # Query que consolida Vendas e Produtos (mesmo com custo 0)
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_id}.tb_kpis` AS
        SELECT 
            CURRENT_DATE() as data_processamento,
            COUNT(*) as total_vendas,
            SUM(SAFE_CAST(valor_total AS FLOAT64)) as faturamento_total,
            AVG(SAFE_CAST(valor_total AS FLOAT64)) as ticket_medio
        FROM `{PROJECT_ID}.{dataset_id}.tb_vendas_fato`
        """
        
        try:
            client.query(query).result()
            print(f">>> {dataset_id.upper():<25} | ✔️ KPIs Gerados com sucesso.")
        except Exception as e:
            print(f">>> {dataset_id.upper():<25} | ❌ Erro: Dataset ou Tabela não encontrados.")

    print("\n" + "="*80)

if __name__ == "__main__":
    calcular_kpis()