# ==============================================================================
# PROJETO: GASTROBI V2 - ECOSSISTEMA DE INTELIGÊNCIA
# CONSULTORIA: SERGIO PAULO DOS SANTOS
# ARQUIVO: 09_logs_monitoramento.py (VERSÃO AUDITORIA INDIVIDUALIZADA)
# OBJETIVO: Gerar logs no terminal, Excel Consolidado e Excels por Cliente.
# ==============================================================================

import pandas as pd
import os
from google.cloud import bigquery
from datetime import datetime

# CONFIGURAÇÕES DE CAMINHOS
PROJECT_ID = "v2-gastrobi-lab"
PASTA_RAIZ_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
PASTA_LOG_GERAL = r"G:\Drives compartilhados\V2_GASTROBI\99_log"
client = bigquery.Client(project=PROJECT_ID)

def gerar_monitoramento():
    print("="*60)
    print(f"{'PAINEL DE MONITORAMENTO GASTROBI V2':^60}")
    print("="*60)

    lista_consolidada = []
    pastas_ativas = [p for p in os.listdir(PASTA_RAIZ_CLIENTES) if "_ativo" in p.lower()]
    
    data_atual = datetime.now().strftime("%d/%m/%Y")
    hora_atual = datetime.now().strftime("%H:%M:%S")
    timestamp_arquivo = datetime.now().strftime("%Y%m%d_%H%M")

    for pasta_cliente in pastas_ativas:
        dataset_id = pasta_cliente.split("_")[2].lower() if "_" in pasta_cliente else pasta_cliente.lower()
        print(f"\n📊 CLIENTE: {pasta_cliente}")
        
        tabelas = ["tb_vendas_fato", "tb_produtos_dim", "tb_kpis"]
        dados_do_cliente = []

        for tab in tabelas:
            tabela_ref = f"{PROJECT_ID}.{dataset_id}.{tab}"
            try:
                tbl = client.get_table(tabela_ref)
                linhas = tbl.num_rows
            except:
                linhas = 0
            
            registro = {
                "Data": data_atual,
                "Hora": hora_atual,
                "Cliente": pasta_cliente.upper(),
                "Tabela": tab,
                "Linhas_BigQuery": linhas,
                "Status": "OK" if linhas > 0 else "VAZIA/PENDENTE"
            }
            dados_do_cliente.append(registro)
            lista_consolidada.append(registro)

        print(f"   ☁️ BigQuery: fato ({dados_do_cliente[0]['Linhas_BigQuery']}) | dim ({dados_do_cliente[1]['Linhas_BigQuery']}) | kpis ({dados_do_cliente[2]['Linhas_BigQuery']})")

        # --- GERAÇÃO DO EXCEL INDIVIDUAL DO CLIENTE ---
        caminho_log_cliente = os.path.join(PASTA_RAIZ_CLIENTES, pasta_cliente, "99_log")
        
        if os.path.exists(caminho_log_cliente):
            df_individual = pd.DataFrame(dados_do_cliente)
            nome_excel_individual = f"Auditoria_{pasta_cliente}_{timestamp_arquivo}.xlsx"
            df_individual.to_excel(os.path.join(caminho_log_cliente, nome_excel_individual), index=False)
            print(f"   📂 Relatório individual gerado em: {pasta_cliente}/99_log")
        else:
            print(f"   ⚠️ Pasta 99_log não encontrada para {pasta_cliente}. Pulando Excel individual.")

    # --- GERAÇÃO DO EXCEL CONSOLIDADO GERAL ---
    if lista_consolidada:
        df_consolidado = pd.DataFrame(lista_consolidada)
        nome_excel_geral = f"Auditoria_Consolidada_{timestamp_arquivo}.xlsx"
        caminho_final_geral = os.path.join(PASTA_LOG_GERAL, nome_excel_geral)
        
        try:
            df_consolidado.to_excel(caminho_final_geral, index=False)
            print(f"\n✅ MASTER EXCEL GERADO: {nome_excel_geral}")
        except Exception as e:
            print(f"\n❌ ERRO NO MASTER EXCEL: {e}")

    print("\n" + "="*60)

if __name__ == "__main__":
    gerar_monitoramento()