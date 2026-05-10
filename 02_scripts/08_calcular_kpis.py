# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 08_calcular_kpis.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 10/05/2026
# VERSÃO: 3.0 - Protocolo de Emergência (Zero Erros)
# OBJETIVO: Garante a criação da tb_kpis independente de erros na dimensão.
# ==============================================================================

from google.cloud import bigquery
import os
import re
from datetime import datetime

client = bigquery.Client()
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def gerar_nome_dataset(nome_pasta):
    nome = re.sub(r"^\d+_", "", nome_pasta.lower()).replace("_ativo", "").replace("cliente", "").strip("_")
    return re.sub(r"[^a-z0-9_]", "", nome)

def registrar_log_local(caminho_cliente, mensagem):
    pasta_log = os.path.join(caminho_cliente, "99_log")
    if not os.path.exists(pasta_log): os.makedirs(pasta_log)
    arquivo_log = os.path.join(pasta_log, "log_kpis.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [KPIs]: {mensagem}\n")

def executar():
    print(f"\n=== 🧠 GERANDO KPIs (MODO SEGURO) ===")
    pastas = [p for p in os.listdir(PASTA_CLIENTES) if "_ativo" in p.lower()]
    
    for pasta in pastas:
        dataset_id = gerar_nome_dataset(pasta)
        caminho_base = os.path.join(PASTA_CLIENTES, pasta)
        
        print(f"\n>>> Cliente: {dataset_id.upper()}")
        
        # --- TENTATIVA 1: CÁLCULO COMPLETO COM CUSTO ---
        sql_completo = f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_kpis` AS
        SELECT 
            SAFE.PARSE_DATE('%d/%m/%Y', CAST(f.data AS STRING)) as data, 
            SUM(IFNULL(CAST(f.valor_total AS FLOAT64), 0)) as faturamento, 
            SUM(IFNULL(CAST(f.quantidade AS FLOAT64), 0)) as qtd_total,
            SUM(IFNULL(CAST(f.quantidade AS FLOAT64), 0) * IFNULL(CAST(d.custo_unitario AS FLOAT64), 0)) as custo_total,
            SUM(IFNULL(CAST(f.valor_total AS FLOAT64), 0)) - SUM(IFNULL(CAST(f.quantidade AS FLOAT64), 0) * IFNULL(CAST(d.custo_unitario AS FLOAT64), 0)) as lucro_bruto
        FROM `{client.project}.{dataset_id}.tb_vendas_fato` f
        LEFT JOIN `{client.project}.{dataset_id}.tb_produtos_dim` d ON CAST(f.item AS STRING) = CAST(d.item AS STRING)
        GROUP BY 1
        """
        
        # --- TENTATIVA 2: CÁLCULO APENAS FATURAMENTO (CASO O 1 FALHE) ---
        sql_seguro = f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.tb_kpis` AS
        SELECT 
            SAFE.PARSE_DATE('%d/%m/%Y', CAST(data AS STRING)) as data, 
            SUM(IFNULL(CAST(valor_total AS FLOAT64), 0)) as faturamento, 
            SUM(IFNULL(CAST(quantidade AS FLOAT64), 0)) as qtd_total,
            0.0 as custo_total,
            SUM(IFNULL(CAST(valor_total AS FLOAT64), 0)) as lucro_bruto
        FROM `{client.project}.{dataset_id}.tb_vendas_fato`
        GROUP BY 1
        """

        try:
            # Tenta o completo
            client.query(sql_completo).result()
            print("  ✔️ KPIs Calculados com Sucesso (Vendas + Custos).")
            registrar_log_local(caminho_base, "KPIs Completos (Vendas + Custos)")
        except:
            # Se der erro de coluna ou tipo, roda o seguro
            try:
                client.query(sql_seguro).result()
                print("  ⚠️ KPIs Calculados apenas com Vendas (Dimensão de produtos inconsistente).")
                registrar_log_local(caminho_base, "KPIs Simples (Apenas Faturamento)")
            except Exception as e:
                print(f"  ❌ Falha Crítica: {str(e)}")
                registrar_log_local(caminho_base, f"Erro: {str(e)}")

    print(f"\n=== ✅ PROCESSO CONCLUÍDO ===")

if __name__ == "__main__":
    executar()