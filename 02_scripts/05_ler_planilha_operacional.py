# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 05_ler_planilha_operacional.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 07/05/2026
# OBJETIVO: Identificar e validar arquivos na pasta 01_entrada_raw.
#           - Compatível com o Orquestrador (Script 10).
# ==============================================================================

import os
import sys
import glob

PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"
NOME_PASTA_ENTRADA = "01_entrada_raw"

def verificar_arquivos(nome_pasta_cliente):
    """Verifica se existem arquivos para processar na pasta do cliente."""
    caminho_raw = os.path.join(PASTA_CLIENTES, nome_pasta_cliente, NOME_PASTA_ENTRADA)
    
    if not os.path.exists(caminho_raw):
        print(f"Erro: Pasta {NOME_PASTA_ENTRADA} não encontrada para {nome_pasta_cliente}")
        return

    arquivos = glob.glob(os.path.join(caminho_raw, "*.csv")) + glob.glob(os.path.join(caminho_raw, "*.xlsx"))
    
    if arquivos:
        print(f"Arquivos detectados para {nome_pasta_cliente}: {[os.path.basename(a) for a in arquivos]}")
    else:
        print(f"Aviso: Nenhum arquivo de dados encontrado em {nome_pasta_cliente}")

if __name__ == "__main__":
    # Se o Script 10 (Main) enviar o nome do cliente, ele processa.
    # Se você rodar o script sozinho, ele processa todos os ativos.
    if len(sys.argv) > 1:
        verificar_arquivos(sys.argv[1])
    else:
        # Modo de segurança: processa todos os ativos se rodado sozinho
        pastas_ativas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
        for pasta in pastas_ativas:
            verificar_arquivos(pasta)