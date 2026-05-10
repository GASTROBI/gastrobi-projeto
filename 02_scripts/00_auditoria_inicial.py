# ==============================================================================
# SCRIPT: 00_auditoria_inicial.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# OBJETIVO: Gerar relatório de auditoria em Excel com coluna CLIENTE individualizada.
# ==============================================================================

import pandas as pd
import os
import re
from google.cloud import bigquery
from datetime import datetime

# CONFIGURAÇÕES
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
client = bigquery.Client(project=PROJECT_ID)

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def executar_auditoria_completa():
    print(f"--- Iniciando Auditoria Consolidada GASTROBI V2 ---")
    
    dados_auditoria = []
    pastas_ativas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    tabelas_foco = ["tb_vendas_fato", "tb_produtos_dim", "tb_kpis"]

    for pasta in pastas_ativas:
        dataset_id = gerar_nome_dataset(pasta)
        timestamp = datetime.now()
        
        for tabela in tabelas_foco:
            tabela_ref = f"{PROJECT_ID}.{dataset_id}.{tabela}"
            try:
                tbl = client.get_table(tabela_ref)
                status = "SUCESSO"
                detalhe = f"Tabela encontrada com {tbl.num_rows} linhas."
            except Exception:
                status = "AVISO"
                detalhe = "Tabela não encontrada ou dataset pendente."

            # CRIANDO A ESTRUTURA COM A COLUNA CLIENTE QUE VOCÊ PEDIU
            dados_auditoria.append({
                "Data": timestamp.strftime("%d/%m/%Y"),
                "Hora": timestamp.strftime("%H:%M:%S"),
                "Cliente": pasta.upper(), # AQUI ESTÁ A COLUNA QUE FALTAVA
                "Script": "00_auditoria",
                "Tabela": tabela,
                "Status": status,
                "Log_Detalhado": detalhe
            })

    # Gerar o Excel consolidado mas com linhas separadas por cliente
    df = pd.DataFrame(dados_auditoria)
    nome_arquivo = f"Auditoria_GastroBI_Consolidada_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    caminho_final = os.path.join(PASTA_CLIENTES, nome_arquivo)
    
    df.to_excel(caminho_final, index=False)
    print(f"\n✅ Relatório gerado com sucesso: {nome_arquivo}")
    print(f"📍 Local: {PASTA_CLIENTES}")

if __name__ == "__main__":
    executar_auditoria_completa()