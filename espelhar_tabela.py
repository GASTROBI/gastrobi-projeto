# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: RESET E ESPELHAMENTO DE LABORATÓRIO (SANDBOX)
# OBJETIVO: Limpar tabelas de teste e recriar a estrutura igual à produção
# ==============================================================================

from google.cloud import bigquery

# CONFIGURAÇÃO DE SEGURANÇA: Aponta APENAS para o dataset de laboratório
project_id = "v2-gastrobi-lab"
dataset_destino = "lab_testes" 
dataset_origem = "08_espetaria_gauchaii_ativo" # Dataset que serve de modelo
tabelas = ["fato_vendas", "dim_produtos"] # Tabelas que queremos espelhar

client = bigquery.Client(project=project_id)

def resetar_e_espelhar_lab():
    print(f"--- Iniciando Reset do Laboratório ({dataset_destino}) ---")
    
    for nome_tabela in tabelas:
        try:
            # 1. Apaga a tabela do Laboratório (Ação segura: apenas no lab)
            tabela_ref_destino = f"{project_id}.{dataset_destino}.{nome_tabela}_teste"
            try:
                client.delete_table(tabela_ref_destino)
                print(f"Apagado: {tabela_ref_destino}")
            except:
                print(f"Tabela não existia no laboratório, pulando deleção: {nome_tabela}")
            
            # 2. Busca o modelo na Produção
            origem_ref = f"{project_id}.{dataset_origem}.{nome_tabela}"
            origem_table = client.get_table(origem_ref)
            
            # 3. Recria no Laboratório com a estrutura original
            nova_tabela = bigquery.Table(tabela_ref_destino, schema=origem_table.schema)
            client.create_table(nova_tabela)
            print(f"Recriado: {tabela_ref_destino} com estrutura de produção.")
            
        except Exception as e:
            print(f"Erro ao processar {nome_tabela}: {e}")

    print("--- Fim do Reset do Laboratório ---")

if __name__ == "__main__":
    resetar_e_espelhar_lab()