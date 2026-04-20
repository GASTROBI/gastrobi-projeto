import pandas as pd
from google.cloud import bigquery
import os
import re
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÕES DE CAMINHO E AMBIENTE
# Objetivo: Definir onde o script busca os dados e onde registra os eventos.
# ==============================================================================
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))
pasta_raiz = os.path.dirname(diretorio_do_script)
caminho_clientes = os.path.join(pasta_raiz, "01_Clientes")
arquivo_log = os.path.join(diretorio_do_script, "log_execucao.txt")

client = bigquery.Client(project="gastrobi-core-profissional")
tabela_destino = "gastrobi-core-profissional.raw.vendas_brutas"

def registrar_log(mensagem):
    """
    FUNCIONALIDADE: Sistema de Auditoria (Log).
    POR QUE: Para que na automação das 22h você saiba exatamente o que aconteceu.
    PARA QUÊ: Registra erros, sucessos e alertas em um arquivo de texto permanente.
    """
    data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        f.write(f"[{data_hora}] {mensagem}\n")
    print(mensagem)

def limpar_nome_coluna(nome):
    """
    FUNCIONALIDADE: Padronização de Schema.
    POR QUE: O BigQuery não aceita espaços, parênteses ou acentos nos nomes das colunas.
    PARA QUÊ: Garante que 'Valor (Bruto)' vire 'valor_bruto', evitando erros de carga.
    """
    nome = nome.lower().strip()
    nome = re.sub(r'[^\w\s]', '', nome)
    nome = nome.replace(' ', '_')
    return nome

# Início da execução
registrar_log("🚀 Iniciando Varredura Multi-Cliente...")
lista_dfs = []

# ==============================================================================
# VARREDURA RECURSIVA (O MOTOR DE ESCALA)
# Objetivo: Entrar em todas as pastas e identificar quem é o cliente automaticamente.
# ==============================================================================
for root, dirs, files in os.walk(caminho_clientes):
    # Regra: Só processa arquivos que estiverem dentro da pasta padrão '01_Dados_Brutos'
    if "01_Dados_Brutos" in root:
        # Pega o nome da pasta pai (ex: Cafe_Gourmet) para usar como identificador
        nome_cliente = os.path.basename(os.path.dirname(root))
        
        for arquivo in files:
            # Ignora arquivos temporários do Excel ou documentos de texto
            if arquivo.startswith("~") or arquivo.endswith(".docx"):
                continue
                
            caminho_completo = os.path.join(root, arquivo)
            try:
                # FUNCIONALIDADE: Poliglotismo de Dados.
                # Identifica o formato e aplica o motor de leitura correto (CSV, TXT ou XLSX).
                if arquivo.endswith(".csv"):
                    temp_df = pd.read_csv(caminho_completo, sep=None, engine='python', encoding='latin1')
                elif arquivo.endswith(".txt"):
                    temp_df = pd.read_csv(caminho_completo, sep='|', encoding='latin1')
                elif arquivo.endswith(".xlsx"):
                    temp_df = pd.read_excel(caminho_completo)
                else:
                    continue

                # Aplica a limpeza de colunas definida acima
                temp_df.columns = [limpar_nome_coluna(c) for c in temp_df.columns]
                
                # FUNCIONALIDADE: Blindagem de Integridade.
                # POR QUE: Sem NCM, a consultoria GastroBI não consegue calcular impostos.
                # PARA QUÊ: Pula arquivos incompletos e avisa o consultor via Log.
                if 'ncm_simulado' not in temp_df.columns:
                    registrar_log(f"⚠️ ERRO CRÍTICO: Cliente {nome_cliente} enviou arquivo {arquivo} sem a coluna NCM!")
                    continue

                # Adiciona a "etiqueta" do cliente para que o BigQuery saiba separar os dados no Looker
                temp_df['origem_arquivo'] = nome_cliente
                lista_dfs.append(temp_df)
                registrar_log(f"✅ Lido com sucesso: {nome_cliente} -> {arquivo}")
                
            except Exception as e:
                registrar_log(f"❌ ERRO GRAVE no arquivo {arquivo} do cliente {nome_cliente}: {str(e)}")

# ==============================================================================
# CONSOLIDAÇÃO E ENVIO PARA NUVEM
# Objetivo: Unificar todos os clientes em uma única tabela mestra no BigQuery.
# ==============================================================================
if lista_dfs:
    df_final = pd.concat(lista_dfs, ignore_index=True)
    df_final = df_final.astype(str) # Força texto para evitar conflitos de tipo na nuvem
    
    # Deleta a tabela anterior para garantir que os dados estejam sempre atualizados e limpos
    client.delete_table(tabela_destino, not_found_ok=True)
    
    registrar_log(f"⏳ Enviando {len(df_final)} linhas para o BigQuery...")
    job = client.load_table_from_dataframe(df_final, tabela_destino)
    job.result() # Aguarda a confirmação do Google
    registrar_log("🏆 SUCESSO TOTAL: Nuvem atualizada para todos os clientes.")
else:
    registrar_log("❌ NENHUM DADO PROCESSADO: Verifique as pastas 01_Dados_Brutos.")