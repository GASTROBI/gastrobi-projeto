# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 05_ler_planilha_operacional.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Localizar automaticamente a planilha operacional dentro
# da pasta do cliente ativo e ler suas abas principais.
#
# PLANILHA:
# Planilha_Operacional_Restaurante_Teste.xlsx
#
# ABAS ACEITAS:
# Instrucoes
# Vendas
# Gastos
# Produtos
# GERENCIAL_PRIVADO
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

                if pasta.lower().endswith("_ativo"):
                    return pasta

        return None

    except Exception as erro:
        print("Erro ao localizar cliente ativo:", erro)
        return None


# ==========================================================
# FUNÇÃO LER PLANILHA
# ==========================================================
def ler_planilha():

    try:

        cliente = detectar_cliente_ativo()

        if not cliente:
            print("Nenhum cliente ativo encontrado.")
            return

        pasta_cliente = os.path.join(PASTA_CLIENTES, cliente)

        arquivo = "Planilha_Operacional_Restaurante_Teste.xlsx"

        caminho_arquivo = os.path.join(pasta_cliente, arquivo)

        print("Arquivo encontrado:")
        print(caminho_arquivo)

        # --------------------------------------------------
        # IDENTIFICAR ABAS
        # --------------------------------------------------
        excel = pd.ExcelFile(caminho_arquivo)

        abas = excel.sheet_names

        print("Abas encontradas:", abas)

        # cria dicionário ignorando maiúsculas/minúsculas
        abas_dict = {}

        for aba in abas:
            abas_dict[aba.lower()] = aba

        # --------------------------------------------------
        # LER VENDAS
        # --------------------------------------------------
        if "vendas" in abas_dict:

            df_vendas = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["vendas"]
            )

            print("Vendas carregada:", df_vendas.shape)

        # --------------------------------------------------
        # LER GASTOS
        # --------------------------------------------------
        if "gastos" in abas_dict:

            df_gastos = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["gastos"]
            )

            print("Gastos carregada:", df_gastos.shape)

        # --------------------------------------------------
        # LER PRODUTOS
        # --------------------------------------------------
        if "produtos" in abas_dict:

            df_produtos = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["produtos"]
            )

            print("Produtos carregada:", df_produtos.shape)

        # --------------------------------------------------
        # LER GERENCIAL PRIVADO
        # --------------------------------------------------
        if "gerencial_privado" in abas_dict:

            df_gerencial = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["gerencial_privado"]
            )

            print("GERENCIAL_PRIVADO carregada:", df_gerencial.shape)

        print("Planilha lida com sucesso.")

    except Exception as erro:
        print("Erro ao ler planilha:", erro)


# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":

    ler_planilha()