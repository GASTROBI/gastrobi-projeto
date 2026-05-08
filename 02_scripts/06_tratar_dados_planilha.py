# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 08/05/2026
# OBJETIVO: Padronizar nomes das colunas principais MANTENDO todos os outros 
#           campos originais (NCM, Categoria, etc.) para evitar perda de dados.
# ==============================================================================

import pandas as pd
import os
import glob

PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def executar():
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    for pasta in pastas:
        caminho_in = os.path.join(PASTA_CLIENTES, pasta, "01_entrada_raw")
        caminho_out = os.path.join(PASTA_CLIENTES, pasta, "02_processed")
        if not os.path.exists(caminho_out): os.makedirs(caminho_out)
        
        arquivos = glob.glob(os.path.join(caminho_in, "*.csv")) + glob.glob(os.path.join(caminho_in, "*.xlsx"))
        for arq in arquivos:
            try:
                # Carregamento do arquivo
                df = pd.read_excel(arq) if arq.endswith('.xlsx') else pd.read_csv(arq, sep=None, engine='python', encoding='latin-1')
                
                # Limpeza inicial apenas de espaços nos nomes das colunas
                df.columns = [str(c).strip().lower() for c in df.columns]
                
                # MAPEAMENTO DAS COLUNAS CHAVE (Sem deletar as outras)
                mapeamento = {
                    'valor total bruto': 'valor_total', 
                    'vlr total': 'valor_total',
                    'venda': 'valor_total', 
                    'nome item': 'item', 
                    'produto': 'item',
                    'data venda': 'data', 
                    'qtd': 'quantidade', 
                    'quantidade': 'quantidade'
                }
                df = df.rename(columns=mapeamento)
                
                # Garantia de que as colunas padronizadas existem
                for col in ['data', 'item', 'valor_total', 'quantidade']:
                    if col not in df.columns:
                        df[col] = 0
                
                # SALVAMENTO: Aqui ele salva TODAS as colunas que vieram no arquivo
                df.to_csv(os.path.join(caminho_out, "vendas_final.csv"), index=False, encoding='utf-8-sig')
                print(f">>> [OK] {pasta} tratada com todas as colunas preservadas.")
                
            except Exception as e:
                print(f"Erro ao tratar {pasta}: {e}")

if __name__ == "__main__":
    executar()