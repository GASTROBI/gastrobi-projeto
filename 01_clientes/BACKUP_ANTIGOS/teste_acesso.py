# ==============================================================================
# PROJETO: V2_GASTROBI
# MÓDULO: TESTE DE ACESSO (CAMINHO DINÂMICO)
# DATA: 18/04/2026
# VERSÃO: 1.0.3
# OBJETIVO: Ajuste automático de caminho para evitar erro de diretório
# AUTOR: Sergio Santos / Gemini Collaborator
# ==============================================================================

import os
import logging

# 1. Achar onde o arquivo de script está salvo
pasta_onde_esta_o_script = os.path.dirname(os.path.abspath(__file__))

# 2. Apontar para a pasta do restaurante (ajuste o nome se necessário)
pasta_raiz = os.path.join(pasta_onde_esta_o_script, '01_restaurante_sergio_ativo')
pasta_logs = os.path.join(pasta_raiz, '04_logs')
pasta_entrada = os.path.join(pasta_raiz, '01_entrada_raw')

# 3. Criar as pastas se não existirem
os.makedirs(pasta_logs, exist_ok=True)
os.makedirs(pasta_entrada, exist_ok=True)

# 4. Configurar Log
logging.basicConfig(
    filename=os.path.join(pasta_logs, 'log_processamento.txt'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

def validar_ambiente():
    try:
        arquivo_teste = os.path.join(pasta_entrada, 'arquivo_teste.txt')
        with open(arquivo_teste, 'w') as f:
            f.write("Teste de escrita validado com sucesso!")
        
        msg = f"Sucesso! Arquivo criado em: {pasta_entrada}"
        logging.info(msg)
        print(msg)
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    validar_ambiente()