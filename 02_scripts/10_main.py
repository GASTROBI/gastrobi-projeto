# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 10_main.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Script Mestre ORQUESTRADOR BLINDADO V2.
#           - Varre clientes ativos.
#           - Executa fluxo completo (Estrutura -> Dados -> KPIs).
#           - Gera Log em Excel individual por cliente.
#           - Dispara e-mail de alerta em caso de falha (Script 09).
# ==============================================================================

import subprocess
import os
import sys
import pandas as pd
import importlib.util 
from datetime import datetime

# CONFIGURAÇÕES DE CAMINHOS
PASTA_SCRIPTS = "02_scripts"
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# --- CARREGAMENTO DO VIGIA DE E-MAIL (SCRIPT 09) ---
def carregar_vigia_email():
    try:
        caminho_vigia = os.path.join(PASTA_SCRIPTS, "09_logs_monitoramento.py")
        if os.path.exists(caminho_vigia):
            spec = importlib.util.spec_from_file_location("monitoramento", caminho_vigia)
            modulo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modulo)
            return modulo.registrar_e_notificar
    except Exception as e:
        print(f"!!! Aviso: Script 09 não carregado corretamente. Erro: {e}")
    return lambda *args: None

registrar_e_notificar = carregar_vigia_email()

def registrar_log_individual(caminho_cliente, nome_cliente, script, status, detalhe):
    """Cria ou atualiza um log em Excel dentro da pasta do próprio cliente."""
    pasta_log_cliente = os.path.join(caminho_cliente, "99_logs_processamento")
    
    if not os.path.exists(pasta_log_cliente):
        os.makedirs(pasta_log_cliente)
    
    arquivo_excel = os.path.join(pasta_log_cliente, "relatorio_atualizacao.xlsx")
    
    nova_linha = {
        "Data_Hora": [datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
        "Cliente": [nome_cliente.upper()],
        "Script": [script],
        "Status": [status],
        "Detalhe_Erro": [detalhe]
    }
    
    df_novo = pd.DataFrame(nova_linha)

    try:
        if os.path.exists(arquivo_excel):
            df_antigo = pd.read_excel(arquivo_excel)
            df_final = pd.concat([df_antigo, df_novo], ignore_index=True)
            df_final.to_excel(arquivo_excel, index=False)
        else:
            df_novo.to_excel(arquivo_excel, index=False)
    except Exception as e:
        print(f"Erro ao salvar log no cliente: {e}")

def executar_script(nome_script):
    """Executa um script específico da pasta 02_scripts via subprocesso."""
    caminho_script = os.path.join(PASTA_SCRIPTS, nome_script)
    try:
        subprocess.run([sys.executable, caminho_script], check=True)
        return True
    except Exception as e:
        return str(e)

def processar_gastrobi():
    print("\n" + "="*60)
    print("--- MOTOR GASTROBI V2 INICIADO (ORQUESTRADOR DE ESCALA) ---")
    print("="*60)
    
    try:
        todos_itens = os.listdir(PASTA_CLIENTES)
        clientes_ativos = [c for c in todos_itens if c.lower().strip().endswith("_ativo")]
    except Exception as e:
        print(f"Erro fatal ao acessar diretório de clientes: {e}")
        return

    if not clientes_ativos:
        print("Aviso: Nenhum cliente ativo encontrado na pasta 01_clientes.")
        return

    # SEQUÊNCIA LÓGICA DE PROCESSAMENTO (DO ZERO AO DASHBOARD)
    scripts_fluxo = [
        "03_criar_dataset_cliente.py",
        "04_clonar_tabelas_padrao_cliente.py",
        "05_ler_planilha_operacional.py",
        "06_tratar_dados_planilha.py",
        "07_importar_bigquery.py",
        "08_calcular_kpis.py"  # Agora integrado no fluxo automático
    ]

    for cliente in clientes_ativos:
        caminho_completo_cliente = os.path.join(PASTA_CLIENTES, cliente)
        print(f"\n>>> PROCESSANDO: {cliente}")
        
        nome_exibicao = cliente.replace("_ativo", "").replace("_", " ").upper()
        
        for script in scripts_fluxo:
            print(f"Executando etapa: {script}...")
            resultado = executar_script(script)
            
            if resultado is True:
                registrar_log_individual(caminho_completo_cliente, nome_exibicao, script, "SUCESSO", "-")
            else:
                # REGISTRO DE FALHA E NOTIFICAÇÃO
                registrar_log_individual(caminho_completo_cliente, nome_exibicao, script, "ERRO", str(resultado))
                
                print(f"!!! FALHA NO CLIENTE: {nome_exibicao}")
                print(f"!!! DISPARANDO ALERTA PARA O SEU E-MAIL...")
                
                registrar_e_notificar(nome_exibicao, script, "ERRO", str(resultado))
                
                # Pula para o próximo cliente da fila em caso de erro
                break 

    print("\n" + "="*60)
    print("CICLO DE PROCESSAMENTO FINALIZADO.")
    print("============================================================\n")

if __name__ == "__main__":
    processar_gastrobi()