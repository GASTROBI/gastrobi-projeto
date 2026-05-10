# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 05_ler_planilha_operacional.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 2.4 - Auditoria de Arquivos e Log Individual (99_log)
# OBJETIVO: Identificar, classificar e validar arquivos na pasta 01_entrada_raw.
#           Registra a detecção na pasta 99_log de cada cliente.
# ==============================================================================

import os
import sys
import glob
from datetime import datetime

# --- CONFIGURAÇÕES ---
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
NOME_PASTA_ENTRADA = "01_entrada_raw"

def registrar_log_local(caminho_cliente, mensagem):
    """Grava a auditoria de arquivos na pasta 99_log do cliente."""
    pasta_log = os.path.join(caminho_cliente, "99_log")
    if not os.path.exists(pasta_log):
        os.makedirs(pasta_log)
        
    arquivo_log = os.path.join(pasta_log, "log_operacional.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [ARQUIVOS]: {mensagem}\n")

def verificar_arquivos(nome_pasta_cliente):
    """Verifica e classifica os arquivos para processamento individual."""
    caminho_base_cliente = os.path.join(PASTA_CLIENTES, nome_pasta_cliente)
    caminho_raw = os.path.join(caminho_base_cliente, NOME_PASTA_ENTRADA)
    
    print(f"\n🔍 Auditando: {nome_pasta_cliente.upper()}")
    
    if not os.path.exists(caminho_raw):
        msg = f"ERRO: Pasta {NOME_PASTA_ENTRADA} não localizada."
        print(f"  ❌ {msg}")
        registrar_log_local(caminho_base_cliente, msg)
        return

    # Busca por CSV e XLSX
    arquivos = glob.glob(os.path.join(caminho_raw, "*.csv")) + glob.glob(os.path.join(caminho_raw, "*.xlsx"))
    
    if not arquivos:
        msg = "AVISO: Nenhum arquivo encontrado para processar nesta carga."
        print(f"  ⚠️ {msg}")
        registrar_log_local(caminho_base_cliente, msg)
        return

    # --- CLASSIFICAÇÃO PARA EVITAR O ERRO DA CHURRASCARIA ---
    vendas_detectadas = []
    produtos_detectados = []

    for arq in arquivos:
        nome_arq = os.path.basename(arq).lower()
        if "venda" in nome_arq:
            vendas_detectadas.append(nome_arq)
        elif "produto" in nome_arq or "estoque" in nome_arq or "dim" in nome_arq:
            produtos_detectados.append(nome_arq)

    # Registro e Feedback
    resumo = f"Detectados: {len(vendas_detectadas)} arquivo(s) de Vendas e {len(produtos_detectados)} de Produtos."
    print(f"  ✔️ {resumo}")
    
    if len(produtos_detectados) == 0:
        print("  ⚠️ ALERTA: Arquivo de PRODUTOS não identificado. A tb_produtos_dim não será atualizada.")
    
    registrar_log_local(caminho_base_cliente, f"{resumo} | Arquivos: {[os.path.basename(a) for a in arquivos]}")

if __name__ == "__main__":
    # Orquestrador (Script 10) ou execução individual
    if len(sys.argv) > 1:
        verificar_arquivos(sys.argv[1])
    else:
        pastas_ativas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
        for pasta in pastas_ativas:
            verificar_arquivos(pasta)
    
    print(f"\n=== ✅ FIM DA AUDITORIA DE ARQUIVOS ===")