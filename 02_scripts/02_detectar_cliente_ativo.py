# ==============================================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 02_detectar_cliente_ativo.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-05-10
# VERSÃO: 2.0 - Multicliente e Gestão de Logs
# FINALIDADE:
# Identificar todos os clientes ativos e organizar a estrutura de 
# diretórios para processamento individual e gravação de logs (99_log).
# ==============================================================================

import os

# ==============================================================================
# CAMINHOS E CONFIGURAÇÕES
# ==============================================================================
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

def detectar_todos_clientes_ativos():
    """
    Lista todas as pastas que terminam com '_ativo' e garante a 
    existência da subpasta '99_log' em cada uma delas.
    """
    try:
        if not os.path.exists(PASTA_CLIENTES):
            print(f"!!! [ERRO]: Caminho não encontrado: {PASTA_CLIENTES}")
            return []

        pastas = os.listdir(PASTA_CLIENTES)
        clientes_ativos = []

        for pasta in pastas:
            caminho_completo = os.path.join(PASTA_CLIENTES, pasta)
            
            # Verifica se é diretório e se termina com _ativo
            if os.path.isdir(caminho_completo) and pasta.lower().strip().endswith("_ativo"):
                
                # --- NOVIDADE: GARANTIR ESTRUTURA DE LOG INDIVIDUAL ---
                caminho_log = os.path.join(caminho_completo, "99_log")
                if not os.path.exists(caminho_log):
                    os.makedirs(caminho_log)
                    print(f">>> Estrutura 99_log criada para: {pasta}")
                
                clientes_ativos.append(pasta)

        # ----------------------------------------------------------------------
        # RESULTADOS DO LABORATÓRIO
        # ----------------------------------------------------------------------
        if not clientes_ativos:
            print("--- [SENTINELA]: Nenhum cliente ativo encontrado.")
            return []
        else:
            print(f"--- [SENTINELA]: {len(clientes_ativos)} cliente(s) ativo(s) detectado(s):")
            for c in clientes_ativos:
                print(f"    -> {c}")
            return clientes_ativos

    except Exception as erro:
        print(f"!!! [ERRO] ao detectar clientes ativos: {erro}")
        return []

# ==============================================================================
# TESTE LOCAL (LABORATÓRIO)
# ==============================================================================
if __name__ == "__main__":
    # Testando a detecção múltipla e criação de pastas de log
    detectar_todos_clientes_ativos()