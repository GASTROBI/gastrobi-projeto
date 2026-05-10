# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/07_importar_bigquery.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 2.7 - Carga Híbrida (Vendas + Produtos) e Log Individual
# OBJETIVO: Importar os arquivos processados para o BigQuery de cada cliente.
# ==============================================================================

import pandas as pd
import os
import re
from datetime import datetime
import pandas_gbq

# --- CONFIGURAÇÕES ---
PROJECT_ID = "v2-gastrobi-lab"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def registrar_log_local(caminho_cliente, mensagem):
    """Grava o log da importação na pasta 99_log do cliente."""
    pasta_log = os.path.join(caminho_cliente, "99_log")
    if not os.path.exists(pasta_log): os.makedirs(pasta_log)
    arquivo_log = os.path.join(pasta_log, "log_bigquery.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [BIGQUERY]: {mensagem}\n")

def executar():
    print(f"\n=== ☁️ INICIANDO IMPORTAÇÃO PARA BIGQUERY (LAB) ===")
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    
    for pasta in pastas:
        caminho_base = os.path.join(PASTA_CLIENTES, pasta)
        caminho_processed = os.path.join(caminho_base, "02_processed")
        dataset_id = gerar_nome_dataset(pasta)
        
        if not os.path.exists(caminho_processed): continue

        print(f"\n>>> Cliente: {pasta.upper()}")

        # LISTA DE CARGAS (Vendas e Produtos)
        cargas = [
            {"arquivo": "vendas_final.csv", "tabela": "tb_vendas_fato"},
            {"arquivo": "produtos_final.csv", "tabela": "tb_produtos_dim"}
        ]

        for carga in cargas:
            caminho_csv = os.path.join(caminho_processed, carga["arquivo"])
            
            if os.path.exists(caminho_csv):
                try:
                    df = pd.read_csv(caminho_csv)
                    if not df.empty:
                        # Upload usando pandas_gbq (limpa o erro de FutureWarning)
                        tabela_destino = f"{dataset_id}.{carga['tabela']}"
                        pandas_gbq.to_gbq(
                            df, 
                            destination_table=tabela_destino, 
                            project_id=PROJECT_ID, 
                            if_exists='replace',
                            progress_bar=False
                        )
                        
                        msg = f"Sucesso: {len(df)} linhas enviadas para {tabela_destino}"
                        print(f"  ✔️ {msg}")
                        registrar_log_local(caminho_base, msg)
                except Exception as e:
                    msg_erro = f"Erro ao subir {carga['tabela']}: {str(e)}"
                    print(f"  ❌ {msg_erro}")
                    registrar_log_local(caminho_base, msg_erro)
            else:
                # Se não tem o CSV de produtos, avisa no log para você saber por que o KPI dará NULL
                if "produtos" in carga["arquivo"]:
                    msg_vazio = "Aviso: produtos_final.csv não encontrado. Dimensão não atualizada."
                    print(f"  ⚠️ {msg_vazio}")
                    registrar_log_local(caminho_base, msg_vazio)

    print(f"\n=== ✅ FIM DA IMPORTAÇÃO CLOUD ===")

if __name__ == "__main__":
    executar()