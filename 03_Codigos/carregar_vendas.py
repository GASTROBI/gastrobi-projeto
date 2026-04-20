import pandas as pd
from google.cloud import bigquery
import os

diretorio_do_script = os.path.dirname(os.path.abspath(__file__))
pasta_raiz = os.path.dirname(diretorio_do_script)
pasta_clientes = os.path.join(pasta_raiz, "01_Clientes")

try:
    lista_dfs = []
    for root, dirs, files in os.walk(pasta_clientes):
        nome_pasta = os.path.basename(root)
        if nome_pasta in ["Pizzaria_Sergio", "Cliente_Teste", "01_Clientes"]: continue
        
        for arquivo in files:
            if arquivo.endswith(".xlsx") or arquivo.endswith(".csv"):
                caminho_full = os.path.join(root, arquivo)
                try:
                    df_temp = pd.read_excel(caminho_full) if arquivo.endswith(".xlsx") else pd.read_csv(caminho_full, sep=None, engine='python', encoding='latin1', on_bad_lines='skip')
                    
                    if not df_temp.empty:
                        df_temp.columns = [str(c).strip().lower() for c in df_temp.columns]
                        
                        # 1. REMOVE LIXO DO GIT (Marcadores de conflito)
                        df_temp = df_temp[~df_temp.iloc[:, 0].astype(str).str.contains('<<<<<<<|=======|>>>>>>>')]
                        
                        # 2. IDENTIFICA COLUNA DE DATA (Procura exaustiva)
                        col_data = next((c for c in df_temp.columns if any(x in c for x in ['data', 'date', 'venda'])), None)
                        
                        if col_data:
                            # Converte e limpa
                            df_temp['data_formatada'] = pd.to_datetime(df_temp[col_data], dayfirst=True, errors='coerce')
                            df_temp['data_original_bruta'] = df_temp[col_data].astype(str)
                            df_temp = df_temp.rename(columns={col_data: 'data_venda_original'})
                        
                        df_temp['nome_cliente'] = nome_pasta
                        lista_dfs.append(df_temp)
                except Exception: continue

    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        df_final.columns = [str(c).replace(' ', '_').replace('/', '_').replace('.', '_').replace('-', '_').lower() for c in df_final.columns]
        
        client = bigquery.Client(project="gastrobi-core-profissional")
        tabela_id = "gastrobi-core-profissional.raw.vendas_brutas"
        client.delete_table(tabela_id, not_found_ok=True)
        client.load_table_from_dataframe(df_final, tabela_id).result()
        print("🚀 SUCESSO! Conflitos do Git removidos e dados carregados.")
except Exception as e: print(f"Erro: {e}")