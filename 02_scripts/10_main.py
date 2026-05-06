# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# CONSULTOR: Sérgio Paulo dos Santos
# DATA: 06/05/2026
# OBJETIVO: Script mestre BLINDADO. Executa a sequência uma única vez.
#           Proibida a reinicialização automática para evitar loops.
# ==============================================================================

import subprocess
import os
import sys

def executar_script(nome_script):
    caminho_script = os.path.join("02_scripts", nome_script)
    print(f"\n>>> EXECUTANDO: {nome_script}")
    
    try:
        # O shell=False impede que o script chame novos processos indesejados
        resultado = subprocess.run([sys.executable, caminho_script], check=True)
        return True
    except Exception as e:
        print(f"!!! PARADA DE SEGURANÇA no script {nome_script}: {e}")
        return False

def iniciar_fluxo_unico():
    scripts = [
        "00_auditoria_inicial.py",
        "01_criar_tabelas_bigquery.py",
        "02_detectar_cliente_ativo.py",
        "02_gerar_amostra.py",
        "03_criar_dataset_cliente.py",
        "04_clonar_tabelas_padrao_cliente.py",
        "04_limpeza_cruzamento.py",
        "05_ler_planilha_operacional.py",
        "06_tratar_dados_planilha.py",
        "07_importar_bigquery.py",
        "08_calcular_kpis.py",
        "09_logs_monitoramento.py",
        "11_importar_gastos.py",
        "12_importar_monofasicos.py"
    ]

    print("--- INICIANDO PROCESSAMENTO ÚNICO GASTROBI V2 ---")
    
    for script in scripts:
        sucesso = executar_script(script)
        if not sucesso:
            print("FLUXO INTERROMPIDO PARA EVITAR LOOP OU ERRO.")
            break

    print("\n======================================================")
    print("PROCESSO FINALIZADO COM SEGURANÇA.")
    print("======================================================")

if __name__ == "__main__":
    iniciar_fluxo_unico()