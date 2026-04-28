# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 10_main.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Script principal de automação do GastroBI
# Executa toda rotina operacional em sequência
# ==========================================================

import subprocess
import os
from datetime import datetime
import csv

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
PASTA_SCRIPTS = r"G:\Drives compartilhados\V2_GASTROBI\02_scripts"
PYTHON_EXE = r"C:\Users\profe\AppData\Local\Programs\Python\Python314\python.exe"

PASTA_LOGS = r"G:\Drives compartilhados\V2_GASTROBI\03_logs"
ARQUIVO_LOG = "logs_gastrobi.csv"

# ==========================================================
# ROTINA OFICIAL DOS SCRIPTS
# ==========================================================
ROTINA = [
    "02_detectar_cliente_ativo.py",
    "03_criar_dataset_cliente.py",
    "04_clonar_tabelas_padrao_cliente.py",
    "05_ler_planilha_operacional.py",
    "06_tratar_dados_planilha.py",
    "07_importar_bigquery.py",
    "08_calcular_kpis.py"
]

# ==========================================================
# PREPARAR LOGS
# ==========================================================
def preparar_logs():

    if not os.path.exists(PASTA_LOGS):
        os.makedirs(PASTA_LOGS)

def caminho_log():

    return os.path.join(PASTA_LOGS, ARQUIVO_LOG)

def criar_log():

    arquivo = caminho_log()

    if not os.path.exists(arquivo):

        with open(arquivo, "w", newline="", encoding="utf-8-sig") as f:

            writer = csv.writer(f)

            writer.writerow([
                "data_hora",
                "script",
                "status",
                "mensagem"
            ])

def registrar(script, status, mensagem):

    preparar_logs()
    criar_log()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(caminho_log(), "a", newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f)

        writer.writerow([
            agora,
            script,
            status,
            mensagem
        ])

# ==========================================================
# EXECUTAR SCRIPT INDIVIDUAL
# ==========================================================
def rodar_script(nome_script):

    caminho = os.path.join(PASTA_SCRIPTS, nome_script)

    try:

        resultado = subprocess.run(
            [PYTHON_EXE, caminho],
            capture_output=True,
            text=True
        )

        if resultado.returncode == 0:

            registrar(
                nome_script,
                "SUCESSO",
                resultado.stdout.strip()
            )

            print(nome_script, "OK")

        else:

            registrar(
                nome_script,
                "ERRO",
                resultado.stderr.strip()
            )

            print(nome_script, "ERRO")

    except Exception as erro:

        registrar(
            nome_script,
            "ERRO",
            str(erro)
        )

        print(nome_script, "FALHOU")

# ==========================================================
# EXECUTAR ROTINA COMPLETA
# ==========================================================
def executar_rotina():

    print("INICIANDO ROTINA GASTROBI")
    print("-" * 40)

    for script in ROTINA:
        rodar_script(script)

    print("-" * 40)
    print("ROTINA FINALIZADA")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":

    executar_rotina()