# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 2.6 - Chave Mestra para Movimentação e Vendas
# OBJETIVO: Ajuste fino para reconhecer arquivos de 'movimento' como Vendas.
# ==============================================================================

import pandas as pd
import os
import glob
from datetime import datetime

# --- CONFIGURAÇÕES ---
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def registrar_log_local(caminho_cliente, mensagem):
    pasta_log = os.path.join(caminho_cliente, "99_log")
    if not os.path.exists(pasta_log): os.makedirs(pasta_log)
    arquivo_log = os.path.join(pasta_log, "log_tratamento.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [TRATAMENTO]: {mensagem}\n")

def executar():
    print(f"\n=== ⚙️ RE-PROCESSANDO TRATAMENTO (AJUSTE CHURRASCARIA) ===")
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    
    for pasta in pastas:
        caminho_base = os.path.join(PASTA_CLIENTES, pasta)
        caminho_in = os.path.join(caminho_base, "01_entrada_raw")
        caminho_out = os.path.join(caminho_base, "02_processed")
        if not os.path.exists(caminho_out): os.makedirs(caminho_out)
        
        arquivos = glob.glob(os.path.join(caminho_in, "*.csv")) + glob.glob(os.path.join(caminho_in, "*.xlsx"))
        if not arquivos: continue

        print(f"\n>>> Cliente: {pasta.upper()}")
        
        for arq in arquivos:
            nome_original = os.path.basename(arq).lower()
            try:
                df = pd.read_excel(arq) if arq.endswith('.xlsx') else pd.read_csv(arq, sep=None, engine='python', encoding='latin-1')
                df.columns = [str(c).strip().lower() for c in df.columns]
                
                # Mapeamento expandido
                mapeamento = {
                    'valor total bruto': 'valor_total', 'vlr total': 'valor_total', 'venda': 'valor_total', 'total': 'valor_total',
                    'nome item': 'item', 'produto': 'item', 'descrição': 'item', 'nome': 'item',
                    'data venda': 'data', 'movimento': 'data', 'data': 'data', 'emissão': 'data',
                    'qtd': 'quantidade', 'quantidade': 'quantidade',
                    'custo unitario': 'custo_unitario', 'custo': 'custo_unitario', 'vlr custo': 'custo_unitario'
                }
                df = df.rename(columns=mapeamento)

                # --- LÓGICA DE CLASSIFICAÇÃO MELHORADA ---
                # Agora 'movimento' também é considerado Venda
                if any(x in nome_original for x in ["venda", "movimento", "faturamento"]):
                    tipo = "vendas_final.csv"
                    for col in ['data', 'item', 'valor_total', 'quantidade']:
                        if col not in df.columns: df[col] = 0
                    msg = f"VENDAS IDENTIFICADAS: {nome_original}"
                
                elif any(x in nome_original for x in ["produto", "estoque", "item", "dim"]):
                    tipo = "produtos_final.csv"
                    if 'custo_unitario' not in df.columns: df['custo_unitario'] = 0.0
                    msg = f"PRODUTOS IDENTIFICADOS: {nome_original}"
                
                else:
                    tipo = f"extra_{nome_original}.csv"
                    msg = f"ARQUIVO EXTRA: {nome_original}"

                df.to_csv(os.path.join(caminho_out, tipo), index=False, encoding='utf-8-sig')
                print(f"  ✔️ {msg}")
                registrar_log_local(caminho_base, msg)
                
            except Exception as e:
                print(f"  ❌ Erro: {str(e)}")
                registrar_log_local(caminho_base, f"Erro: {str(e)}")

if __name__ == "__main__":
    executar()