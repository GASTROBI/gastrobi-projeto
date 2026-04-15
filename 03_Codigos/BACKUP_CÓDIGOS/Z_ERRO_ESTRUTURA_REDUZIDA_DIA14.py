"""
PROJETO: GastroBI - Inteligência de Negócio para Food Service
AUTOR: Sergio Santos
DATA: 14/04/2026 (RESTAURAÇÃO FASE 1 - DRIVE G:)
"""

import pandas as pd
import os

def processar_gastrobi_fase1_drive():
    PROJETO_ID = "gastrobi-core-profissional"
    DATASET_ID = "gastrobi_lab" 
    TABELA_ID = "cliente_teste_vendas"
    caminho_base = r"G:\Drives compartilhados\gastrobi-core-profissional\01_Clientes"
    
    lista_dfs = []
    print(f"🚀 GASTROBI: Iniciando Validação Fase 1 no Drive G:...")

    if not os.path.exists(caminho_base):
        print(f"❌ Erro: Drive G: não acessível.")
        return

    # Processando apenas o que estiver na pasta Cliente_Teste
    pasta_foco = "Cliente_Teste"
    caminho_cliente = os.path.join(caminho_base, pasta_foco)
    
    if os.path.exists(caminho_cliente):
        # O pulo do gato: o nome exato da sua pasta de dados
        caminho_dados = os.path.join(caminho_cliente, "01_Dados_Brutos")
        busca_em = caminho_dados if os.path.exists(caminho_dados) else caminho_cliente
        
        print(f"📁 Lendo pasta: {busca_em}")
        
        for arquivo in os.listdir(busca_em):
            if arquivo.lower().endswith('.csv'):
                caminho_full = os.path.join(busca_em, arquivo)
                print(f"  ✅ Localizado: {arquivo}")
                
                try:
                    # Leitura com o seu padrão validado
                    df_temp = pd.read_csv(caminho_full, sep=None, engine='python', encoding='utf-8-sig')
                    df_temp.columns = [str(col).lower().strip() for col in df_temp.columns]
                    df_temp['nome_cliente'] = pasta_foco
                    lista_dfs.append(df_temp)
                except Exception as e:
                    print(f"  ❌ Erro no arquivo {arquivo}: {e}")

    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        df_final.to_gbq(f"{DATASET_ID}.{TABELA_ID}", project_id=PROJETO_ID, if_exists='replace')
        print(f"✨ SUCESSO: Base de Teste atualizada no BigQuery!")
    else:
        print("⚠️ Arquivo não encontrado. Verifique se o CSV está em '01_Dados_Brutos'.")

if __name__ == "__main__":
    processar_gastrobi_fase1_drive()