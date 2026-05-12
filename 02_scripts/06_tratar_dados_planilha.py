# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 02_scripts/06_tratar_dados_planilha.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 12/05/2026
# VERSÃO: 2.8 - Chave Mestra Universal (CSV, Excel e JSON)
# OBJETIVO: Processar múltiplos formatos de entrada de forma automática e escalável.
# ==============================================================================

import pandas as pd
import os
import json

# Configuração de Caminho Raiz
PASTA_RAIZ = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def tratar_dados():
    print("="*80)
    print(f"{'ENGINE DE TRATAMENTO GASTROBI V2 - MOTOR UNIVERSAL':^80}")
    print("="*80)
    
    if not os.path.exists(PASTA_RAIZ):
        print(f"❌ ERRO: Caminho raiz não acessível: {PASTA_RAIZ}")
        return

    pastas_ativas = [p for p in os.listdir(PASTA_RAIZ) if "_ativo" in p.lower()]
    
    for pasta in pastas_ativas:
        caminho_cliente = os.path.join(PASTA_RAIZ, pasta)
        
        # Identificação da pasta de entrada
        subpastas = os.listdir(caminho_cliente)
        pasta_raw_nome = next((f for f in subpastas if "01_entrada_raw" in f.lower()), None)
        
        if not pasta_raw_nome:
            continue
            
        caminho_raw = os.path.join(caminho_cliente, pasta_raw_nome)
        arquivos = [f for f in os.listdir(caminho_raw) if not f.startswith('~$')]
        
        # Busca arquivos de vendas (CSV, XLSX ou JSON)
        venda_bruta = [f for f in arquivos if 
                       (f.lower().endswith('.csv') or 
                        f.lower().endswith('.xlsx') or 
                        f.lower().endswith('.json')) and 
                       ("_final" not in f.lower())]
        
        if venda_bruta:
            arq_origem = os.path.join(caminho_raw, venda_bruta[0])
            print(f">>> Cliente: {pasta:<40} | Processando: {venda_bruta[0]}")
            
            try:
                # 1. LEITURA DE JSON
                if arq_origem.lower().endswith('.json'):
                    df = pd.read_json(arq_origem)
                
                # 2. LEITURA DE CSV (Com tratamento de Encoding)
                elif arq_origem.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(arq_origem, sep=None, engine='python', encoding='utf-8-sig')
                    except:
                        df = pd.read_csv(arq_origem, sep=None, engine='python', encoding='iso-8859-1')
                
                # 3. LEITURA DE EXCEL
                else:
                    df = pd.read_excel(arq_origem)

                # Padronização de Colunas
                df.columns = [str(c).strip() for c in df.columns]
                
                # Salva o resultado unificado para o script 07
                caminho_saida = os.path.join(caminho_raw, "vendas_final.csv")
                df.to_csv(caminho_saida, index=False, encoding='utf-8-sig')
                print(f"   ✔️ SUCESSO: vendas_final.csv gerado a partir de {venda_bruta[0].split('.')[-1].upper()}.")
                
            except Exception as e:
                print(f"   ❌ ERRO NO PROCESSAMENTO: {e}")
        else:
            print(f">>> Cliente: {pasta:<40} | ⚠️ Sem arquivos de entrada.")

    print("\n" + "="*80)
    print(f"{'MOTOR DE TRATAMENTO FINALIZADO':^80}")
    print("="*80)

if __name__ == "__main__":
    tratar_dados()