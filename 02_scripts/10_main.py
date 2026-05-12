# ==============================================================================
# PROJETO: GASTROBI V2 - ECOSSISTEMA DE INTELIGÊNCIA
# CONSULTORIA: SERGIO PAULO DOS SANTOS
# Data criação 12/05/2026
# ARQUIVO: 10_main.py (ORQUESTRADOR CENTRAL - VERSÃO EXECUTIVA)
# OBJETIVO: Executar a cadeia de dados (01 a 09) conforme estrutura real da pasta.
# ==============================================================================

import subprocess
import os
import sys
from pathlib import Path

# Configuração de Caminhos baseada no local do script
BASE_DIR = Path(__file__).resolve().parent

# Sequência de Execução Ajustada conforme a estrutura da pasta 02_scripts
# Cada tupla contém (nome_do_arquivo, nome_da_etapa)
SCRIPTS = [
    ("01_criar_tabelas_bigquery.py",   "TABELAS BQ"),
    ("03_criar_dataset_cliente.py",    "DATASETS BQ"),
    ("05_ler_planilha_operacional.py", "EXTRAÇÃO"),
    ("06_tratar_dados_planilha.py",    "TRATAMENTO"),
    ("07_importar_bigquery.py",       "CARGA CLOUD"),
    ("08_calcular_kpis.py",           "INTELIGÊNCIA"),
    ("09_logs_monitoramento.py",      "RESULTADOS")
]

def rodar_maestro():
    # Limpa o terminal para uma apresentação executiva
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("="*80)
    print(f"{'GASTROBI V2 - ECOSSISTEMA DE INTELIGÊNCIA':^80}")
    print(f"{'EXECUTIVO RESPONSÁVEL: SERGIO PAULO DOS SANTOS':^80}")
    print("="*80)
    print(f"[*] Iniciando Processamento Consolidado Multicliente...\n")

    sucessos = 0
    falhas = 0

    for script, etapa in SCRIPTS:
        caminho_script = BASE_DIR / script
        
        if not caminho_script.exists():
            print(f" [!] ERRO: {etapa:<15} | Arquivo {script} não localizado na pasta.")
            falhas += 1
            continue

        print(f" [+] EXECUTANDO: {etapa:<15} | {script}...", end="\r")
        
        try:
            # Executa o script mantendo a integridade do que já existe no BigQuery
            subprocess.run([sys.executable, str(caminho_script)], check=True)
            print(f" [OK] CONCLUÍDO: {etapa:<15} | Processamento finalizado com sucesso.    ")
            sucessos += 1
        except subprocess.CalledProcessError:
            print(f" [X] FALHA:     {etapa:<15} | Erro durante a execução do script.      ")
            falhas += 1
            continue

    print("\n" + "="*80)
    print(f"{'RELATÓRIO DE PROCESSAMENTO FINALIZADO':^80}")
    print(f"{f'SUCESSOS: {sucessos} | FALHAS: {falhas}':^80}")
    print("="*80)
    print(f"Status: Ambiente sincronizado com Google BigQuery.")

if __name__ == "__main__":
    rodar_maestro()