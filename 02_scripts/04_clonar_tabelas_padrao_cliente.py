# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 04_clonar_tabelas_padrao_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Copiar automaticamente as tabelas padrão do dataset modelo
# para o dataset do novo cliente.
#
# TABELAS:
# tb_vendas_fato
# tb_produtos_dim
# tb_kpis
# ==========================================================

from google.cloud import bigquery

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PROJECT_ID = "v2-gastrobi-lab"

DATASET_MODELO = "modelo_gastrobi"
DATASET_CLIENTE = "restaurante_teste"

# ==========================================================
# CLIENTE BIGQUERY
# ==========================================================
client = bigquery.Client(project=PROJECT_ID)

# ==========================================================
# LISTA DE TABELAS
# ==========================================================
TABELAS = [
    "tb_vendas_fato",
    "tb_produtos_dim",
    "tb_kpis"
]

# ==========================================================
# FUNÇÃO CLONAR TABELAS
# ==========================================================
def clonar_tabelas():

    for tabela in TABELAS:

        origem = f"{PROJECT_ID}.{DATASET_MODELO}.{tabela}"
        destino = f"{PROJECT_ID}.{DATASET_CLIENTE}.{tabela}"

        try:
            client.get_table(destino)
            print(f"Tabela já existe: {tabela}")

        except:

            query = f"""
            CREATE TABLE `{destino}`
            AS
            SELECT *
            FROM `{origem}`
            WHERE 1 = 0
            """

            job = client.query(query)
            job.result()

            print(f"Tabela criada com sucesso: {tabela}")

# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":

    clonar_tabelas()

    print("Processo finalizado.")