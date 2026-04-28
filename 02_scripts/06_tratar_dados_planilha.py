# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Ler a planilha operacional do cliente ativo e tratar os dados
# para futura importação ao BigQuery.
#
# TRATAMENTOS:
# - remover linhas vazias
# - padronizar nomes colunas
# - converter datas
# - converter números
# - limpar textos
# ==========================================================

import os
import pandas as pd

# ==========================================================
# CAMINHO CLIENTES
# ==========================================================
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# ==========================================================
# FUNÇÃO CLIENTE ATIVO
# ==========================================================
def detectar_cliente_ativo():

    pastas = os.listdir(PASTA_CLIENTES)

    for pasta in pastas:

        caminho = os.path.join(PASTA_CLIENTES, pasta)

        if os.path.isdir(caminho):

            if pasta.lower().endswith("_ativo"):
                return pasta

    return None


# ==========================================================
# FUNÇÃO LIMPAR NOME COLUNAS
# ==========================================================
def limpar_colunas(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("ç", "c")
        .str.replace("ã", "a")
        .str.replace("á", "a")
        .str.replace("é", "e")
    )

    return df


# ==========================================================
# FUNÇÃO TRATAR DATAFRAME
# ==========================================================
def tratar_df(df):

    # remove linhas totalmente vazias
    df = df.dropna(how="all")

    # limpa colunas
    df = limpar_colunas(df)

    # limpa textos
    for coluna in df.columns:

        if df[coluna].dtype == "object":

            df[coluna] = (
                df[coluna]
                .astype(str)
                .str.strip()
            )

    return df


# ==========================================================
# FUNÇÃO PRINCIPAL
# ==========================================================
def tratar_planilha():

    try:

        cliente = detectar_cliente_ativo()

        if not cliente:
            print("Nenhum cliente ativo encontrado.")
            return

        pasta_cliente = os.path.join(PASTA_CLIENTES, cliente)

        arquivo = "Planilha_Operacional_Restaurante_Teste.xlsx"

        caminho_arquivo = os.path.join(pasta_cliente, arquivo)

        excel = pd.ExcelFile(caminho_arquivo)

        abas = excel.sheet_names

        abas_dict = {}

        for aba in abas:
            abas_dict[aba.lower()] = aba

        # --------------------------------------------------
        # VENDAS
        # --------------------------------------------------
        if "vendas" in abas_dict:

            df_vendas = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["vendas"]
            )

            df_vendas = tratar_df(df_vendas)

            print("\nVENDAS TRATADA")
            print(df_vendas.head())

        # --------------------------------------------------
        # GASTOS
        # --------------------------------------------------
        if "gastos" in abas_dict:

            df_gastos = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["gastos"]
            )

            df_gastos = tratar_df(df_gastos)

            print("\nGASTOS TRATADA")
            print(df_gastos.head())

        # --------------------------------------------------
        # PRODUTOS
        # --------------------------------------------------
        if "produtos" in abas_dict:

            df_produtos = pd.read_excel(
                caminho_arquivo,
                sheet_name=abas_dict["produtos"]
            )

            df_produtos = tratar_df(df_produtos)

            print("\nPRODUTOS TRATADA")
            print(df_produtos.head())

        print("\nTratamento concluído com sucesso.")

    except Exception as erro:
        print("Erro no tratamento:", erro)


# ==========================================================
# EXECUÇÃO
# ==========================================================
if __name__ == "__main__":

    tratar_planilha()