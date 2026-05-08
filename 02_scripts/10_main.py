# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 10_main.py
# CLIENTE: Gestão Multiclientes (Padrão V2)
# AUTOR: Sergio Paulo dos Santos
# DATA: 08/05/2026
# VERSÃO: 2.1 - Auditoria Distribuída e Blindagem de Processos
# OBJETIVO: Orquestrar o fluxo de dados do GastroBI, garantindo que cada cliente
#           tenha seu próprio log de auditoria em Excel para conferência.
# ==============================================================================

import subprocess
import os
import sys
import pandas as pd
import shutil
from datetime import datetime

# --- CONFIGURAÇÕES DE CAMINHOS ---
PASTA_RAIZ = r"G:\Drives compartilhados\V2_GASTROBI"
PASTA_CLIENTES = os.path.join(PASTA_RAIZ, "01_clientes")
PASTA_SCRIPTS = os.path.join(PASTA_RAIZ, "02_scripts")
PASTA_LOG_GERAL = os.path.join(PASTA_RAIZ, "99_log")

def criar_infra_logs():
    """Garante a existência das pastas de log central e por cliente."""
    if not os.path.exists(PASTA_LOG_GERAL):
        os.makedirs(PASTA_LOG_GERAL)
    
    # Varre as pastas de clientes para criar a 99_log em cada um
    if os.path.exists(PASTA_CLIENTES):
        pastas_clientes = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
        for cliente in pastas_clientes:
            caminho_log_cliente = os.path.join(PASTA_CLIENTES, cliente, "99_log")
            if not os.path.exists(caminho_log_cliente):
                os.makedirs(caminho_log_cliente)

def executar_ciclo():
    criar_infra_logs()
    
    # Ordem de execução rigorosa
    scripts = [
        "03_criar_dataset_cliente.py",
        "05_ler_planilha_operacional.py",
        "06_tratar_dados_planilha.py",
        "07_importar_bigquery.py",
        "08_calcular_kpis.py",
        "12_monofasicos.py"
    ]
    
    historico = []
    horario_inicio = datetime.now().strftime('%H:%M:%S')
    print(f"=== INICIANDO MOTOR GASTROBI: {horario_inicio} ===")

    for script in scripts:
        print(f">> Executando: {script}...", end=" ", flush=True)
        caminho_completo = os.path.join(PASTA_SCRIPTS, script)
        
        try:
            # Execução com captura de saída e erro
            resultado = subprocess.run(
                [sys.executable, caminho_completo], 
                capture_output=True, 
                text=True, 
                timeout=600 # Limite de 10 minutos por script
            )
            
            if resultado.returncode == 0:
                status = "SUCESSO"
                detalhe = resultado.stdout
                print(" [OK]")
            else:
                status = "ERRO"
                detalhe = resultado.stderr
                print(" [FALHOU]")
                
        except Exception as e:
            status = "FALHA CRÍTICA"
            detalhe = str(e)
            print(" [CRÍTICO]")

        historico.append({
            "Data": datetime.now().strftime("%d/%m/%Y"),
            "Hora": datetime.now().strftime("%H:%M:%S"),
            "Script": script,
            "Status": status,
            "Log_Detalhado": detalhe
        })

    # --- GERAÇÃO DO ARQUIVO DE AUDITORIA ---
    df_auditoria = pd.DataFrame(historico)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    nome_excel = f"Auditoria_GastroBI_{timestamp}.xlsx"
    caminho_excel_geral = os.path.join(PASTA_LOG_GERAL, nome_excel)
    
    # Salva o log mestre
    df_auditoria.to_excel(caminho_excel_geral, index=False)

    # --- DISTRIBUIÇÃO DOS LOGS PARA AS PASTAS DOS CLIENTES ---
    if os.path.exists(PASTA_CLIENTES):
        pastas_clientes = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
        for cliente in pastas_clientes:
            pasta_destino = os.path.join(PASTA_CLIENTES, cliente, "99_log")
            if os.path.exists(pasta_destino):
                shutil.copy(caminho_excel_geral, os.path.join(pasta_destino, nome_excel))

    print(f"\n=== PROCESSO CONCLUÍDO COM SUCESSO EM: {datetime.now().strftime('%H:%M:%S')} ===")
    print(f"Log Central: {caminho_excel_geral}")

if __name__ == "__main__":
    executar_ciclo()