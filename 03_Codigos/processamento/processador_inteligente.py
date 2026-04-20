"""
PROJETO: GastroBI - Inteligência de Negócio para Food Service
AUTOR: Sergio Santos
DATA: 08/04/2026
VERSÃO: 7.5 (Final de Laboratório - Saneamento e Limpeza de NULLs)
"""

import pandas as pd
import os

def processar_gastrobi_final():
    PROJETO_ID = "gastrobi-core-profissional"
    DATASET_ID = "dev_sandbox" 
    TABELA_ID = "vendas_homologacao"
    caminho_base = r"G:\Drives compartilhados\gastrobi-core-profissional\01_Clientes"
    
    lista_dfs = []
    print(f"🚀 GASTROBI: Sergio Santos - Finalizando saneamento e eliminando NULLs...")

    def limpar_valor(valor):
        if pd.isna(valor) or str(valor).strip() == "" or str(valor).lower() == 'null': 
            return 0.0
        texto = str(valor).upper().strip().replace('R$', '').replace(' ', '')
        if ',' in texto and '.' in texto: texto = texto.replace('.', '')
        texto = texto.replace(',', '.')
        try: return float(texto)
        except: return 0.0

    if not os.path.exists(caminho_base): return

    for nome_pasta in os.listdir(caminho_base):
        caminho_cliente = os.path.join(caminho_base, nome_pasta)
        if os.path.isdir(caminho_cliente):
            for arquivo in os.listdir(caminho_cliente):
                caminho_full = os.path.join(caminho_cliente, arquivo)
                df_temp = None
                try:
                    if arquivo.endswith('.csv'):
                        try: df_temp = pd.read_json(caminho_full)
                        except: df_temp = pd.read_csv(caminho_full, sep=None, engine='python', encoding='utf-8-sig')
                    elif arquivo.endswith('.xlsx'):
                        df_temp = pd.read_excel(caminho_full)

                    if df_temp is not None:
                        df_temp.columns = [str(col).lower().strip() for col in df_temp.columns]
                        mapeamento = {
                            'data': 'data_venda', 'emissão': 'data_venda', 'movimento': 'data_venda',
                            'produto': 'nome_item', 'descrição': 'nome_item', 'item': 'nome_item',
                            'faturamento': 'valor_bruto_total', 'valor_total_bruto': 'valor_bruto_total',
                            'serviço': 'servico', 'gorjeta': 'servico', 'quantidade': 'quantidade', 'qtd': 'quantidade',
                            'ncm_simulado': 'ncm'
                        }
                        df_temp.rename(columns=mapeamento, inplace=True)
                        df_temp['nome_cliente'] = nome_pasta

                        colunas = ['nome_cliente', 'data_venda', 'nome_item', 'categoria', 'valor_bruto_total', 'valor_unitario', 'servico', 'quantidade', 'ncm']
                        for col in colunas:
                            if col not in df_temp.columns:
                                df_temp[col] = 0.0 if col in ['valor_bruto_total', 'valor_unitario', 'servico', 'quantidade'] else 'NÃO INFORMADO'

                        for n_col in ['valor_bruto_total', 'valor_unitario', 'servico', 'quantidade']:
                            df_temp[n_col] = df_temp[n_col].apply(limpar_valor).fillna(0.0).astype(float)
                        
                        df_temp['nome_item'] = df_temp['nome_item'].astype(str).str.upper()
                        lista_dfs.append(df_temp[colunas])
                except: continue

    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True).drop_duplicates()
        df_final.to_gbq(f"{DATASET_ID}.{TABELA_ID}", project_id=PROJETO_ID, if_exists='replace')
        print(f"✨ GASTROBI: Base 100% limpa e sem valores nulos!")

if __name__ == "__main__":
    processar_gastrobi_final()