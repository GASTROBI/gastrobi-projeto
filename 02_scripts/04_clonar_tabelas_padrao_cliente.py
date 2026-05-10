# ==============================================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 04_clonar_tabelas_padrao_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 2.3 - Ajuste de Nomenclatura conforme Print do BigQuery
# FINALIDADE:
# Clonar a estrutura das tabelas do dataset modelo para TODOS os clientes ativos.
# Nomes ajustados para: tb_vendas_fato, tb_produtos_dim, tb_kpis
# ==============================================================================

import os
import re
from datetime import datetime
from google.cloud import bigquery

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
DATASET_MODELO = "modelo_gastrobi"

client = bigquery.Client(project=PROJECT_ID)

# NOMES EXATOS CONFORME O PRINT DO SEU BIGQUERY (Dataset: modelo_gastrobi)
TABELAS_PADRAO = ["tb_vendas_fato", "tb_produtos_dim", "tb_kpis"]

# ==============================================================================
# FUNÇÕES DE APOIO
# ==============================================================================
def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

def registrar_log_local(caminho_pasta, mensagem):
    pasta_log = os.path.join(caminho_pasta, "99_log")
    if not os.path.exists(pasta_log):
        os.makedirs(pasta_log)
    arquivo_log = os.path.join(pasta_log, "log_estruturacao.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [CLONAGEM]: {mensagem}\n")

# ==============================================================================
# FUNÇÃO PRINCIPAL: CLONAR ESTRUTURA PARA TODOS
# ==============================================================================
def clonar_estrutura():
    print(f"\n=== 🧬 CLONAGEM DE ESTRUTURAS (PADRÃO SEBRAE) ===")
    
    try:
        pastas_ativos = [p for p in os.listdir(PASTA_CLIENTES) if p.lower().strip().endswith("_ativo")]
        
        if not pastas_ativos:
            print("!!! [AVISO]: Nenhum cliente ativo encontrado.")
            return

        for pasta in pastas_ativos:
            caminho_cliente = os.path.join(PASTA_CLIENTES, pasta)
            dataset_destino = gerar_nome_dataset(pasta)
            
            print(f"\n>>> Cliente: {pasta.upper()}")
            
            for tabela in TABELAS_PADRAO:
                origem = f"{PROJECT_ID}.{DATASET_MODELO}.{tabela}"
                destino = f"{PROJECT_ID}.{dataset_destino}.{tabela}"

                try:
                    client.get_table(destino)
                    print(f"  ✔️ {tabela} já existe.")
                except:
                    try:
                        # Clona apenas a estrutura
                        query = f"CREATE TABLE `{destino}` AS SELECT * FROM `{origem}` WHERE 1 = 0"
                        job = client.query(query)
                        job.result()
                        
                        msg = f"Tabela {tabela} criada com sucesso."
                        print(f"  ✨ {msg}")
                        registrar_log_local(caminho_cliente, msg)
                    except Exception as e:
                        msg_erro = f"Falha ao clonar {tabela}: {str(e)}"
                        print(f"  ❌ {msg_erro}")
                        registrar_log_local(caminho_cliente, msg_erro)

    except Exception as erro_geral:
        print(f"!!! [ERRO GERAL]: {erro_geral}")

if __name__ == "__main__":
    clonar_estrutura()
    print(f"\n=== ✅ ESTRUTURAS SINCRONIZADAS COM O MODELO ===")