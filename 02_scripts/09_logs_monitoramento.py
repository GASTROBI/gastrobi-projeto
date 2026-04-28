# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 09_logs_monitoramento.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Registrar logs operacionais dos scripts do projeto
# ==========================================================

import os
import csv
from datetime import datetime

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PASTA_LOGS = r"G:\Drives compartilhados\V2_GASTROBI\03_logs"
ARQUIVO_LOG = "logs_gastrobi.csv"

# ==========================================================
# CRIAR PASTA SE NÃO EXISTIR
# ==========================================================
def preparar_pasta():

    if not os.path.exists(PASTA_LOGS):
        os.makedirs(PASTA_LOGS)

# ==========================================================
# CAMINHO ARQUIVO
# ==========================================================
def caminho_log():

    return os.path.join(PASTA_LOGS, ARQUIVO_LOG)

# ==========================================================
# CRIAR CABEÇALHO
# ==========================================================
def criar_arquivo():

    arquivo = caminho_log()

    if not os.path.exists(arquivo):

        with open(arquivo, mode="w", newline="", encoding="utf-8-sig") as f:

            writer = csv.writer(f)

            writer.writerow([
                "data_hora",
                "script",
                "cliente",
                "status",
                "mensagem"
            ])

# ==========================================================
# REGISTRAR LOG
# ==========================================================
def registrar(script, cliente, status, mensagem):

    preparar_pasta()
    criar_arquivo()

    arquivo = caminho_log()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(arquivo, mode="a", newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f)

        writer.writerow([
            agora,
            script,
            cliente,
            status,
            mensagem
        ])

    print("Log registrado com sucesso.")

# ==========================================================
# TESTE MANUAL
# ==========================================================
if __name__ == "__main__":

    registrar(
        "09_logs_monitoramento.py",
        "restaurante_teste",
        "SUCESSO",
        "Sistema de logs criado e validado."
    )