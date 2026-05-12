# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 12/05/2026
# VERSÃO: 2.3 - Sincronização de Datasets e Carga Flexível
# OBJETIVO: Garantir o mapeamento correto dos nomes dos clientes no BigQuery.
# ==============================================================================

import pandas as pd
import os
from google.cloud import bigquery

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
PASTA_RAIZ = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
client = bigquery.Client(project=PROJECT_ID)

def importar_dados():
    print("="*80)
    print(f"{'CARGA BIGQUERY GASTROBI V2 - VALIDAÇÃO DE DATASETS':^80}")
    print("="*80)
    
    if not os.path.exists(PASTA_RAIZ):
        print(f"❌ ERRO: Caminho raiz não acessível.")
        return

    pastas_ativas = [p for p in os.listdir(PASTA_RAIZ) if "_ativo" in p.lower()]
    
    for pasta in pastas_ativas:
        # Lógica de extração do ID: tenta pegar o nome central da pasta
        # Ex: "03_cliente_cafe_gourmet_ativo" -> "cafe_gourmet"
        partes = pasta.lower().split("_")
        if "cliente" in partes:
            idx = partes.index("cliente")
            id_bruto = partes[idx+1:-1]
            dataset_id = "_".join(id_bruto)
        else:
            dataset_id = pasta.split("_")[2].lower() if len(partes) > 2 else pasta.lower()
        
        caminho_cliente = os.path.join(PASTA_RAIZ, pasta)
        subpastas = os.listdir(caminho_cliente)
        pasta_raw_nome = next((f for f in subpastas if "01_entrada_raw" in f.lower()), None)
        
        if not pasta_raw_nome: continue
            
        caminho_raw = os.path.join(caminho_cliente, pasta_raw_nome)
        arq_vendas = os.path.join(caminho_raw, "vendas_final.csv")
        
        if os.path.exists(arq_vendas):
            try:
                df_vendas = pd.read_csv(arq_vendas)
                table_id = f"{PROJECT_ID}.{dataset_id}.tb_vendas_fato"

                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    write_disposition="WRITE_APPEND",
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
                    ],
                )

                job = client.load_table_from_dataframe(df_vendas, table_id, job_config=job_config)
                job.result() 
                print(f">>> {dataset_id.upper():<25} | ✔️ Carga realizada com sucesso.")

                # AUTO-POPULAR PRODUTOS
                col_item = next((c for c in df_vendas.columns if "item" in c.lower() or "produto" in c.lower()), df_vendas.columns[0])
                itens_unicos = df_vendas[col_item].unique()
                df_p_auto = pd.DataFrame({"nome_produto": itens_unicos})
                
                table_prod = f"{PROJECT_ID}.{dataset_id}.tb_produtos_dim"
                client.load_table_from_dataframe(df_p_auto, table_prod, job_config=job_config).result()
                
            except Exception as e:
                print(f">>> {dataset_id.upper():<25} | ❌ Erro: {str(e)[:80]}")
        else:
            print(f">>> {dataset_id.upper():<25} | ⚠️ Aguardando vendas_final.csv.")

    print("\n" + "="*80)

if __name__ == "__main__":
    importar_dados()