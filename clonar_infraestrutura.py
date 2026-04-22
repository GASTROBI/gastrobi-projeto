# ==============================================================================
# PROJETO: V2_GASTROBI
# SCRIPT: 01_clonar_estrutura.py
# OBJETIVO: Copiar a estrutura (schema) das tabelas reais para o Laboratório
# ==============================================================================

from google.cloud import bigquery

# CONFIGURAÇÕES
project_id = "v2-gastrobi-lab"
dataset_origem = "08_espetaria_gauchaii_ativo" # Ajuste se necessário
dataset_destino = "lab_testes"
tabelas = ["fato_vendas", "dim_produtos", "tabela_mestre"] 

client = bigquery.Client(project=project_id)

def espelhar_estrutura():
    print(f"--- Iniciando espelhamento de estrutura para {dataset_destino} ---")
    for nome in tabelas:
        try:
            # 1. Pega a tabela de origem
            origem_ref = f"{project_id}.{dataset_origem}.{nome}"
            tabela_origem = client.get_table(origem_ref)
            
            # 2. Prepara o destino
            destino_ref = f"{project_id}.{dataset_destino}.{nome}"
            
            # 3. Deleta se já existir no laboratório para limpar
            try:
                client.delete_table(destino_ref)
                print(f"Limpeza: Tabela {nome} removida do laboratório.")
            except:
                pass
                
            # 4. Cria a nova tabela no laboratório com o schema da origem
            nova_tabela = bigquery.Table(destino_ref, schema=tabela_origem.schema)
            client.create_table(nova_tabela)
            print(f"Sucesso: Tabela {nome} espelhada com sucesso.")
            
        except Exception as e:
            print(f"Erro ao processar {nome}: {e}")

if __name__ == "__main__":
    espelhar_estrutura()
    