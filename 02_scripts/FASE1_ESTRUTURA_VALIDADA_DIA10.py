"""
PROJETO: GastroBI - Inteligência de Negócio
AUTOR: Sergio Santos | VERSÃO: 1.9 (Nomenclatura Ajustada & Direito Intelectual)
REGISTRO JURÍDICO: 10/04/2026
---------------------------------------------------------------------------
DIRETRIZ: Manutenção obrigatória da data no cabeçalho para fins de 
proteção de propriedade intelectual e rastreabilidade jurídica.
---------------------------------------------------------------------------
"""

import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os

def executar_processamento_estratégico():
    PROJETO_ID = "gastrobi-core-profissional"
    DATASET_ID = "dev_sandbox" 
    TABELA_ID = "dim_ficha_tecnica" 
    
    # NOME EXATO DETECTADO PELO TERMINAL DO SERGIO
    NOME_ARQUIVO = "Cópia de complemento_ficha_tecnica.xlsx"
    
    # Cabeçalho de Proteção Intelectual - ESSENCIAL
    data_atual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    print("-" * 65)
    print(f"PROPRIEDADE INTELECTUAL: GASTROBI - CONSULTOR SERGIO SANTOS")
    print(f"REGISTRO DE PROCESSAMENTO JURÍDICO: {data_atual}")
    print("-" * 65)

    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    caminho_completo = os.path.join(diretorio_atual, NOME_ARQUIVO)

    if not os.path.exists(caminho_completo):
        print(f"❌ ERRO: O arquivo '{NOME_ARQUIVO}' ainda não foi localizado.")
        print(f"📂 Pasta atual: {diretorio_atual}")
        return

    try:
        # Leitura com motor para Excel moderno
        df = pd.read_excel(caminho_completo, engine='openpyxl')

        # SANEAMENTO (Data Quality para os 12 indicadores)
        colunas_necessarias = [
            'id_produto', 'canal_venda', 'custo_insumos', 'custo_embalagem', 
            'custo_mao_de_obra', 'impostos_percentual', 'taxa_plataforma'
        ]

        for col in colunas_necessarias:
            if col not in df.columns:
                print(f"❌ ERRO DE INTEGRIDADE: Coluna '{col}' ausente!")
                print(f"Colunas encontradas no arquivo: {list(df.columns)}")
                return

        # Saneamento de valores para cálculos de Lucro Real
        for col in colunas_necessarias[2:]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        df['canal_venda'] = df['canal_venda'].astype(str).str.upper().str.strip()
        
        # Auditoria Jurídica por linha
        df['data_geracao_bi'] = data_atual
        
        # Carga no BigQuery (Alimenta a Dimensão do Star Schema)
        df.to_gbq(f"{DATASET_ID}.{TABELA_ID}", project_id=PROJETO_ID, if_exists='replace')
        
        print(f"✨ GASTROBI: Sucesso! Ficha Técnica integrada com Assinatura Jurídica.")

    except Exception as e:
        print(f"⚠️ Falha Crítica no Processamento: {e}")

if __name__ == "__main__":
    executar_processamento_estratégico()