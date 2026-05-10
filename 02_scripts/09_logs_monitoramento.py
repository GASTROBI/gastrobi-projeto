# ==============================================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 09_logs_monitoramento.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# FINALIDADE: Painel de controle para visualizar o status de todos os 
#             clientes ativos no BigQuery e logs locais.
# ==============================================================================

import os
import re
from google.cloud import bigquery

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def monitorar():
    print(f"\n{'='*60}")
    print(f"{'PAINEL DE MONITORAMENTO GASTROBI V2':^60}")
    print(f"{'='*60}\n")

    pastas_ativas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]

    if not pastas_ativas:
        print("!!! [AVISO]: Nenhum cliente ativo encontrado na pasta 01_clientes.")
        return

    for pasta in pastas_ativas:
        dataset_id = gerar_nome_dataset(pasta)
        print(f"📊 CLIENTE: {pasta.upper()}")
        
        # Verifica Tabelas no BigQuery
        tabelas_foco = ["tb_vendas_fato", "tb_produto_dim", "tb_kpis"]
        status_bq = []
        
        for tab in tabelas_foco:
            try:
                tabela_ref = client.get_table(f"{client.project}.{dataset_id}.{tab}")
                status_bq.append(f"{tab} ({tabela_ref.num_rows} linhas)")
            except:
                status_bq.append(f"{tab} (Vazia/Não criada)")

        print(f"   ☁️ BigQuery: { ' | '.join(status_bq) }")

        # Verifica Logs Locais
        caminho_log = os.path.join(PASTA_CLIENTES, pasta, "99_log")
        if os.path.exists(caminho_log):
            arquivos_log = os.listdir(caminho_log)
            print(f"   📂 Logs Locais: {len(arquivos_log)} arquivos encontrados em 99_log")
        else:
            print(f"   ⚠️ Logs Locais: Pasta 99_log não encontrada.")
        
        print(f"{'-'*60}")

if __name__ == "__main__":
    monitorar()