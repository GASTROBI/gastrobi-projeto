# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Corrigir leitura da aba Produtos considerando nomes
# reais das colunas do Excel e enriquecer dados.
# ==========================================================

import os
import pandas as pd

# ==========================================================
# CAMINHO BASE
# ==========================================================
PASTA_BASE = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# ==========================================================
# LOCALIZAR CLIENTE ATIVO
# ==========================================================
def localizar_cliente():

    for pasta in os.listdir(PASTA_BASE):
        if pasta.endswith("_ativo"):
            return os.path.join(PASTA_BASE, pasta)

    return None

# ==========================================================
# LOCALIZAR PLANILHA
# ==========================================================
def localizar_planilha(pasta_cliente):

    for arq in os.listdir(pasta_cliente):
        if arq.lower().endswith(".xlsx"):
            return os.path.join(pasta_cliente, arq)

    return None

# ==========================================================
# NORMALIZAR TEXTO
# ==========================================================
def normalizar(txt):

    if pd.isna(txt):
        return ""

    return str(txt).strip().upper()

# ==========================================================
# PRODUTOS
# ==========================================================
def tratar_produtos(df):

    df.columns = df.columns.str.strip()

    # renomeia qualquer variação possível
    mapa = {
        "Produto": "produto_original",
        "produto": "produto_original",
        "Categoria": "categoria",
        "categoria": "categoria",
        "Preço Venda": "preco_venda",
        "Preço_Venda": "preco_venda",
        "Preco Venda": "preco_venda",
        "Preco_Venda": "preco_venda",
        "preco_venda": "preco_venda"
    }

    df = df.rename(columns=mapa)

    # garante coluna preço
    df["preco_venda"] = (
        df["preco_venda"]
        .astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )

    df["preco_venda"] = pd.to_numeric(
        df["preco_venda"],
        errors="coerce"
    ).fillna(0)

    df["produto_original"] = df["produto_original"].astype(str).str.strip()
    df["nome_produto_normalizado"] = df["produto_original"].apply(normalizar)

    df["categoria"] = df["categoria"].fillna("GERAL")
    df["subcategoria"] = "PADRAO"
    df["ncm"] = "00000000"
    df["tributacao"] = "NORMAL"
    df["monofasico"] = "NAO"
    df["custo_unitario"] = (df["preco_venda"] * 0.35).round(2)
    df["valor_gorjeta_padrao"] = 0
    df["ativo"] = "SIM"
    df["data_atualizacao"] = pd.Timestamp.today().date()

    return df[
        [
            "nome_produto_normalizado",
            "produto_original",
            "categoria",
            "subcategoria",
            "ncm",
            "tributacao",
            "monofasico",
            "custo_unitario",
            "preco_venda",
            "valor_gorjeta_padrao",
            "ativo",
            "data_atualizacao"
        ]
    ]

# ==========================================================
# VENDAS
# ==========================================================
def tratar_vendas(df_vendas, df_produtos):

    df_vendas.columns = df_vendas.columns.str.strip()

    mapa = {
        "Data": "data",
        "Produto": "produto",
        "Quantidade": "quantidade",
        "Valor Total": "valor_total",
        "Valor_Total": "valor_total",
        "Forma Pagamento": "forma_pagamento",
        "Forma_Pagamento": "forma_pagamento",
        "Canal Venda": "canal_venda",
        "Canal_Venda": "canal_venda"
    }

    df_vendas = df_vendas.rename(columns=mapa)

    df_vendas["produto"] = df_vendas["produto"].astype(str).str.strip()
    df_vendas["produto_key"] = df_vendas["produto"].apply(normalizar)

    df_vendas["quantidade"] = pd.to_numeric(
        df_vendas["quantidade"], errors="coerce"
    ).fillna(0)

    df_vendas["valor_total"] = pd.to_numeric(
        df_vendas["valor_total"], errors="coerce"
    ).fillna(0)

    df = pd.merge(
        df_vendas,
        df_produtos,
        left_on="produto_key",
        right_on="nome_produto_normalizado",
        how="left"
    )

    df["custo_total"] = (
        df["quantidade"] * df["custo_unitario"]
    ).round(2)

    df["valor_gorjeta"] = df["valor_gorjeta_padrao"].fillna(0)

    return df

# ==========================================================
# MAIN
# ==========================================================
def main():

    pasta = localizar_cliente()

    if not pasta:
        print("Cliente ativo não encontrado.")
        return

    arquivo = localizar_planilha(pasta)

    if not arquivo:
        print("Planilha não encontrada.")
        return

    abas = pd.read_excel(arquivo, sheet_name=None)

    produtos = tratar_produtos(abas["Produtos"])
    vendas = tratar_vendas(abas["Vendas"], produtos)

    produtos.to_pickle("produtos_tratado.pkl")
    vendas.to_pickle("vendas_tratado.pkl")

    print("Produtos tratados com sucesso.")
    print("Vendas tratadas com sucesso.")

# ==========================================================
# EXECUTAR
# ==========================================================
if __name__ == "__main__":
    main()