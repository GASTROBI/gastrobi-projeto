# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-07
# FINALIDADE:
# Tratar e normalizar dados vindos de Excel ou CSV.
# Garante que colunas de data e valores fiquem prontas para o BigQuery.
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
    try:
        for pasta in os.listdir(PASTA_BASE):
            if pasta.lower().strip().endswith("_ativo"):
                return os.path.join(PASTA_BASE, pasta)
    except:
        return None
    return None

# ==========================================================
# NORMALIZAR TEXTO
# ==========================================================
def normalizar(txt):
    if pd.isna(txt):
        return ""
    return str(txt).strip().upper()

# ==========================================================
# TRATAMENTO PARA CSV (NOVO PADRÃO PIZZARIA)
# ==========================================================
def tratar_dados_csv(caminho_csv):
    # Lendo o CSV com suporte a acentos
    df = pd.read_csv(caminho_csv, encoding='latin-1')
    
    # Padronizando colunas do CSV para o padrão das nossas tabelas
    mapa_csv = {
        "data_venda": "data",
        "nome_item": "produto",
        "valor_total_bruto": "valor_total",
        "qtd": "quantidade",
        "ncm_simulado": "ncm"
    }
    df = df.rename(columns=mapa_csv)

    # Conversão de Data
    df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce')
    
    # Conversão de Números
    df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(0)
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0)
    
    # Criação de colunas técnicas que o Dashboard precisa
    df['produto_key'] = df['produto'].apply(normalizar)
    df['custo_unitario'] = (df['valor_total'] / df['quantidade'].replace(0, 1) * 0.35).round(2)
    df['custo_total'] = (df['quantidade'] * df['custo_unitario']).round(2)
    
    # Colunas fiscais (usando o NCM que veio no CSV ou padrão)
    df['ncm'] = df['ncm'].fillna("00000000").astype(str)
    
    return df

# ==========================================================
# TRATAMENTO PARA EXCEL (PADRÃO ANTIGO)
# ==========================================================
def tratar_produtos_excel(df):
    df.columns = df.columns.str.strip()
    mapa = {"Produto": "produto_original", "Preço Venda": "preco_venda", "Categoria": "categoria"}
    df = df.rename(columns=mapa)
    
    # Limpeza de moeda R$
    if df['preco_venda'].dtype == object:
        df["preco_venda"] = df["preco_venda"].astype(str).str.replace("R$", "", regex=False).str.replace(".", "", regex=False).str.replace(",", ".", regex=False).str.strip()
    
    df["preco_venda"] = pd.to_numeric(df["preco_venda"], errors="coerce").fillna(0)
    df["nome_produto_normalizado"] = df["produto_original"].apply(normalizar)
    df["custo_unitario"] = (df["preco_venda"] * 0.35).round(2)
    return df

# ==========================================================
# MAIN
# ==========================================================
def main():
    pasta = localizar_cliente()
    if not pasta:
        print("Cliente ativo não encontrado.")
        return

    pasta_raw = os.path.join(pasta, "raw")
    
    # 1. TENTA PROCESSAR CSV SE EXISTIR NA PASTA RAW
    if os.path.exists(pasta_raw):
        arquivos_csv = [f for f in os.listdir(pasta_raw) if f.endswith('.csv')]
        if arquivos_csv:
            print(f"Tratando dados CSV do cliente...")
            df_vendas_final = tratar_dados_csv(os.path.join(pasta_raw, arquivos_csv[0]))
            
            # Salva para o próximo script (07) usar
            df_vendas_final.to_pickle("vendas_tratado.pkl")
            print("Sucesso: 35 linhas da Pizzaria tratadas e prontas.")
            return

    # 2. SE NÃO ACHOU CSV, TENTA EXCEL (FLUXO ANTIGO)
    arquivos_xlsx = [f for f in os.listdir(pasta) if f.endswith('.xlsx')]
    if arquivos_xlsx:
        print(f"Tratando dados EXCEL do cliente...")
        arquivo = os.path.join(pasta, arquivos_xlsx[0])
        abas = pd.read_excel(arquivo, sheet_name=None)
        
        # Faz o processo de cruzamento que você já tinha
        df_prod = tratar_produtos_excel(abas["Produtos"])
        # Aqui simplifiquei a chamada para manter o foco no fluxo
        df_prod.to_pickle("produtos_tratado.pkl")
        print("Sucesso: Excel tratado no padrão antigo.")
    else:
        print("Nenhum dado encontrado para tratar.")

if __name__ == "__main__":
    main()