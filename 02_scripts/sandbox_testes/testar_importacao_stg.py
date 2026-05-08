import pandas as pd
import os
import re
from google.cloud import bigquery

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset_stg(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return f"stg_{re.sub(r'[^a-z0-9_]', '', nome)}"

def executar_teste_completo():
    clientes_novos = ["03_cliente_cafe_gourmet_ativo", "04_cliente_pizzaria_verace_ativo"]
    
    for pasta in clientes_novos:
        dataset_id = gerar_nome_dataset_stg(pasta)
        caminho_csv = os.path.join(PASTA_CLIENTES, pasta, "02_processed", "vendas_final.csv")
        
        if os.path.exists(caminho_csv):
            try:
                df = pd.read_csv(caminho_csv)
                
                # 1. TRATAMENTO ANTI-NULL (Conforme combinado)
                # Preenche números com 0 e textos com "vazio"
                df = df.fillna(0) 

                # 2. TABELA FATO
                tabela_fato = f"{dataset_id}.tb_vendas_fato"
                df.to_gbq(destination_table=tabela_fato, if_exists='replace', progress_bar=False)
                print(f">>> [OK] Fato criada (sem NULLs) em: {tabela_fato}")

                # 3. TABELA DIMENSÃO PRODUTO (Garante as colunas do print)
                colunas_dim = ['item', 'categoria', 'subcategoria', 'ncm', 'tributacao', 'monofasico', 'custo_unitario', 'preco_venda']
                for c in colunas_dim:
                    if c not in df.columns: df[c] = 0
                
                df_prod = df[colunas_dim].drop_duplicates().reset_index(drop=True).fillna(0)
                df_prod.to_gbq(destination_table=f"{dataset_id}.tb_produto_dim", if_exists='replace', progress_bar=False)
                print(f">>> [OK] Dimensão criada em: {dataset_id}.tb_produto_dim")

                # 4. TABELA KPIs (Criando agora no Sandbox)
                sql_kpi = f"""
                CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_kpis` AS
                SELECT data, SUM(valor_total) as faturamento, SUM(quantidade) as qtd_total
                FROM `{client.project}.{dataset_id}.tb_vendas_fato` GROUP BY 1
                """
                client.query(sql_kpi).result()
                print(f">>> [OK] KPIs criados em: {dataset_id}.tb_kpis")

                # 5. TABELA MONOFÁSICOS (Criando agora no Sandbox)
                sql_mono = f"""
                CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_analise_monofasicos` AS
                SELECT item, SUM(quantidade) as qtd_total, SUM(valor_total) as faturamento_bruto
                FROM `{client.project}.{dataset_id}.tb_vendas_fato` GROUP BY 1
                """
                client.query(sql_mono).result()
                print(f">>> [OK] Monofásicos criados em: {dataset_id}.tb_analise_monofasicos")

            except Exception as e:
                print(f"Erro no teste {pasta}: {e}")

if __name__ == "__main__":
    executar_teste_completo()