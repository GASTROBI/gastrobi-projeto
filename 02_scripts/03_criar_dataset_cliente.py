# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 03_criar_dataset_cliente.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 2.1 - Multicliente com Log Individual (99_log)
# OBJETIVO: Criar datasets no BigQuery para todos os clientes ativos e 
#           registrar a operação na pasta de log de cada cliente.
# ==============================================================================

import os
import re
from datetime import datetime
from google.cloud import bigquery

# --- CONFIGURAÇÕES ---
client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    """Limpa o nome da pasta para o padrão de ID do BigQuery."""
    nome = nome_pasta.lower()
    nome = re.sub(r"^\d+_", "", nome) # Remove o prefixo numérico (ex: 01_)
    nome = nome.replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def registrar_log_local(caminho_pasta, mensagem):
    """Grava a mensagem de log na pasta 99_log do cliente específico."""
    pasta_log = os.path.join(caminho_pasta, "99_log")
    
    # Garante que a pasta existe (dobra de segurança)
    if not os.path.exists(pasta_log):
        os.makedirs(pasta_log)
        
    arquivo_log = os.path.join(pasta_log, "log_estruturacao.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {mensagem}\n")

def executar():
    print(f"\n=== 🏗️ INICIANDO ESTRUTURAÇÃO DE DATASETS GASTROBI ===")
    
    # Identifica pastas de clientes ativos
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    
    if not pastas:
        print("!!! [AVISO]: Nenhuma pasta '_ativo' encontrada para processar.")
        return

    for pasta in pastas:
        caminho_cliente = os.path.join(PASTA_CLIENTES, pasta)
        dataset_id = gerar_nome_dataset(pasta)
        full_id = f"{client.project}.{dataset_id}"
        
        try:
            # Verifica se o dataset já existe
            client.get_dataset(full_id)
            msg = f"Dataset já existe no BigQuery: {dataset_id}"
            print(f"✔️ {pasta}: {msg}")
            registrar_log_local(caminho_cliente, msg)
            
        except Exception:
            # Cria o dataset caso não exista
            try:
                dataset = bigquery.Dataset(full_id)
                dataset.location = "US"
                client.create_dataset(dataset)
                
                msg = f"Dataset criado com sucesso: {dataset_id}"
                print(f"✨ {pasta}: {msg}")
                registrar_log_local(caminho_cliente, msg)
            except Exception as e:
                msg_erro = f"Erro ao criar dataset {dataset_id}: {str(e)}"
                print(f"❌ {pasta}: {msg_erro}")
                registrar_log_local(caminho_cliente, msg_erro)

    print(f"=== ✅ FIM DO PROCESSAMENTO DE DATASETS ===\n")

if __name__ == "__main__":
    executar()