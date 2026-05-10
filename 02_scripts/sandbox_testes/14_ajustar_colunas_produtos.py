# ==============================================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 14_ajustar_colunas_produtos.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# FINALIDADE: Padronizar o nome da coluna de identificação para 'item' 
#             em todos os datasets, garantindo que o JOIN do KPI funcione.
# ==============================================================================

from google.cloud import bigquery
import os
import re

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def executar():
    print(f"\n=== 🛠️ AJUSTANDO COLUNAS DE DIMENSÃO (ITEM) NO SCRIPT 14 ===")
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    
    for pasta in pastas:
        dataset_id = gerar_nome_dataset(pasta)
        tabela_ref = f"{client.project}.{dataset_id}.tb_produtos_dim"
        
        try:
            table = client.get_table(tabela_ref)
            colunas = [schema.name.lower() for schema in table.schema]
            
            if 'item' in colunas:
                print(f"  ✔️ {dataset_id}: Coluna 'item' já está correta.")
                continue
            
            alvos = ['produto', 'descricao', 'descrição', 'nome', 'nome_item']
            coluna_encontrada = None
            for alvo in alvos:
                if alvo in colunas:
                    coluna_encontrada = alvo
                    break
            
            if coluna_encontrada:
                sql_rename = f"ALTER TABLE `{tabela_ref}` RENAME COLUMN `{coluna_encontrada}` TO `item`"
                client.query(sql_rename).result()
                print(f"  ✨ {dataset_id}: Coluna '{coluna_encontrada}' renomeada para 'item'.")
            else:
                sql_add = f"ALTER TABLE `{tabela_ref}` ADD COLUMN `item` STRING"
                client.query(sql_add).result()
                print(f"  ⚠️ {dataset_id}: Coluna identificadora não achada. Criada 'item' vazia.")

        except Exception as e:
            print(f"  ❌ Erro em {dataset_id}: {e}")

if __name__ == "__main__":
    executar()