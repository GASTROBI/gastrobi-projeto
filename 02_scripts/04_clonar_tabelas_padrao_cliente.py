# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 04_clonar_tabelas_padrao_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-07
# FINALIDADE:
# Identificar o cliente ativo e criar as tabelas padrão 
# (vendas, produtos, kpis) dentro do seu dataset específico,
# baseando-se no dataset modelo.
# ==========================================================

import os
import re
from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
DATASET_MODELO = "modelo_gastrobi"

client = bigquery.Client(project=PROJECT_ID)

# Tabelas que todo cliente deve ter
TABELAS_PADRAO = ["tb_vendas_fato", "tb_produtos_dim", "tb_kpis"]

# ==========================================================
# FUNÇÕES DE APOIO (DETECÇÃO)
# ==========================================================
def detectar_cliente_ativo():
    try:
        pastas = os.listdir(PASTA_CLIENTES)
        for pasta in pastas:
            if pasta.lower().strip().endswith("_ativo"):
                return pasta
        return None
    except:
        return None

def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    nome = re.sub(r"[^a-z0-9_]", "", nome)
    return nome

# ==========================================================
# FUNÇÃO PRINCIPAL: CLONAR ESTRUTURA
# ==========================================================
def clonar_estrutura():
    try:
        pasta_cliente = detectar_cliente_ativo()
        if not pasta_cliente:
            print("Nenhum cliente ativo para clonar tabelas.")
            return

        dataset_destino = gerar_nome_dataset(pasta_cliente)
        print(f"Alvo da clonagem: {dataset_destino}")

        for tabela in TABELAS_PADRAO:
            origem = f"{PROJECT_ID}.{DATASET_MODELO}.{tabela}"
            destino = f"{PROJECT_ID}.{dataset_destino}.{tabela}"

            try:
                client.get_table(destino)
                print(f"Tabela já existe no cliente: {tabela}")
            except:
                # Cria a tabela vazia copiando apenas a estrutura do modelo
                query = f"CREATE TABLE `{destino}` AS SELECT * FROM `{origem}` WHERE 1 = 0"
                job = client.query(query)
                job.result()
                print(f"Tabela {tabela} criada com sucesso em {dataset_destino}")

    except Exception as erro:
        print("Erro na clonagem de tabelas:", erro)

# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":
    clonar_estrutura()