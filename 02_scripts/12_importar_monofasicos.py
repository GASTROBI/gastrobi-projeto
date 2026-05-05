# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# CONSULTOR: Sérgio Paulo dos Santos
# DATA: 05/05/2026
# OBJETIVO: Criar automaticamente o dataset 'dados_cliente' (se não existir) e
#           importar a tabela 'tb_ncm_monofasicos' da aba 'Impostos_monofasicos'.
#           Garante que a fundação do BigQuery esteja pronta sem intervenção manual.
# ==============================================================================

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

# Configurações de acesso
DATASET_ID = "dados_cliente"
TABELA_NCM = "tb_ncm_monofasicos"
ABA_CORRETA = "Impostos_monofasicos" 

def importar_ncm_monofasicos():
    client = bigquery.Client()

    # Caminho confirmado para a planilha operacional
    caminho_final = r"01_clientes\01_restaurante_teste_ativo\Planilha_Operacional_Restaurante_Teste.xlsx"

    print(f"--- Iniciando Importação de NCMs (Consultor: Sérgio) ---")
    
    try:
        # 1. VALIDAÇÃO/CRIAÇÃO DO DATASET (Etapa 3 do Guia Mestre)
        dataset_ref = client.dataset(DATASET_ID)
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset '{DATASET_ID}' já existe.")
        except NotFound:
            print(f"Dataset '{DATASET_ID}' não encontrado. Criando agora...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Ou a região que você preferir
            client.create_dataset(dataset)
            print(f"Dataset '{DATASET_ID}' criado com sucesso.")

        # 2. LEITURA DA ABA CORRETA
        print(f"Lendo dados da aba: {ABA_CORRETA}...")
        df_ncm = pd.read_excel(caminho_final, sheet_name=ABA_CORRETA, dtype={'ncm': str}, engine='openpyxl')

        # 3. LIMPEZA DOS DADOS
        # Remove linhas totalmente vazias ou sem o código NCM
        df_ncm = df_ncm.dropna(subset=['ncm'])

        # 4. ENVIO PARA O BIGQUERY
        table_ref = dataset_ref.table(TABELA_NCM)
        
        # WRITE_TRUNCATE: Atualiza apenas esta tabela específica.
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

        print(f"Enviando {len(df_ncm)} linhas para {DATASET_ID}.{TABELA_NCM}...")
        job = client.load_table_from_dataframe(df_ncm, table_ref, job_config=job_config)
        job.result()

        print(f"SUCESSO: Dataset e Tabela validados e atualizados.")
        print(f"------------------------------------------------------------------")

    except Exception as e:
        print(f"ERRO CRÍTICO NA IMPORTAÇÃO: {e}")

if __name__ == "__main__":
    importar_ncm_monofasicos()