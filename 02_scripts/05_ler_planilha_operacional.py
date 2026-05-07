# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 05_ler_planilha_operacional.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-07
# FINALIDADE:
# Localizar automaticamente os dados do cliente ativo.
# Suporta Planilha Excel (.xlsx) ou CSV (.csv) na pasta /raw/.
# CORREÇÃO: Adicionado suporte a caracteres latinos (acentos).
# ==========================================================

import os
import pandas as pd

# ==========================================================
# CAMINHO CLIENTES
# ==========================================================
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# ==========================================================
# FUNÇÃO DETECTAR CLIENTE ATIVO
# ==========================================================
def detectar_cliente_ativo():
    try:
        pastas = os.listdir(PASTA_CLIENTES)
        for pasta in pastas:
            caminho = os.path.join(PASTA_CLIENTES, pasta)
            if os.path.isdir(caminho):
                if pasta.lower().strip().endswith("_ativo"):
                    return pasta
        return None
    except Exception as erro:
        print("Erro ao localizar cliente ativo:", erro)
        return None

# ==========================================================
# FUNÇÃO LER DADOS (EXCEL OU CSV)
# ==========================================================
def ler_dados():
    try:
        cliente = detectar_cliente_ativo()
        if not cliente:
            print("Nenhum cliente ativo encontrado.")
            return

        pasta_cliente = os.path.join(PASTA_CLIENTES, cliente)
        pasta_raw = os.path.join(pasta_cliente, "raw")
        
        # --------------------------------------------------
        # TENTATIVA 1: PROCURAR CSV NA PASTA RAW
        # --------------------------------------------------
        if os.path.exists(pasta_raw):
            arquivos_raw = os.listdir(pasta_raw)
            csvs = [f for f in arquivos_raw if f.endswith('.csv')]
            
            if csvs:
                caminho_csv = os.path.join(pasta_raw, csvs[0])
                print(f"Arquivo CSV detectado na pasta RAW: {csvs[0]}")
                
                # CORREÇÃO AQUI: encoding='latin-1' para aceitar acentos
                df_vendas = pd.read_csv(caminho_csv, encoding='latin-1')
                print("Vendas carregadas via CSV com sucesso!")
                print("Total de linhas lidas:", df_vendas.shape[0])
                print("\nPrimeiras linhas dos dados:")
                print(df_vendas.head())
                return

        # --------------------------------------------------
        # TENTATIVA 2: PROCURAR EXCEL NA RAIZ
        # --------------------------------------------------
        arquivos_raiz = os.listdir(pasta_cliente)
        planilhas = [f for f in arquivos_raiz if f.endswith('.xlsx')]

        if planilhas:
            caminho_xlsx = os.path.join(pasta_cliente, planilhas[0])
            print(f"Planilha Excel detectada: {planilhas[0]}")
            
            excel = pd.ExcelFile(caminho_xlsx)
            abas = excel.sheet_names
            abas_dict = {aba.lower(): aba for aba in abas}

            if "vendas" in abas_dict:
                df_vendas = pd.read_excel(caminho_xlsx, sheet_name=abas_dict["vendas"])
                print("Vendas carregadas via Excel com sucesso!")
                print("Total de linhas lidas:", df_vendas.shape[0])
        else:
            print("Nenhum arquivo de dados (CSV ou XLSX) encontrado.")

    except Exception as erro:
        print("Erro ao ler os dados do cliente:", erro)

# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":
    ler_dados()