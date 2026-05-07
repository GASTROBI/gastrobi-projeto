# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 10_main.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Script Mestre Orquestrador - Execução em Sequência.
# ==============================================================================

import subprocess
import os
import sys

PASTA_SCRIPTS = "02_scripts"

def executar_script(nome_script):
    """Executa o script de forma simples, sem passar argumentos."""
    caminho_script = os.path.join(PASTA_SCRIPTS, nome_script)
    try:
        # Roda o script exatamente como você roda no terminal
        subprocess.run([sys.executable, caminho_script], check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def processar_gastrobi():
    print("\n" + "="*60)
    print("--- MOTOR GASTROBI V2 - EXECUÇÃO GERAL ---")
    print("="*60)
    
    # Sequência de scripts que você quer rodar
    scripts_fluxo = [
        "03_criar_dataset_cliente.py",
        "05_ler_planilha_operacional.py",
        "06_tratar_dados_planilha.py",
        "07_importar_bigquery.py",
        "08_calcular_kpis.py",
        "12_monofasicos.py"
    ]

    for script in scripts_fluxo:
        print(f"\n>> Executando: {script}...")
        sucesso = executar_script(script)
        
        if sucesso:
            print(f"[ OK ] {script} finalizado.")
        else:
            print(f"[ ERRO ] Falha no script {script}. Interrompendo fluxo.")
            break 

    print("\n" + "="*60)
    print("CICLO DE PROCESSAMENTO FINALIZADO.")
    print("="*60 + "\n")

if __name__ == "__main__":
    processar_gastrobi()