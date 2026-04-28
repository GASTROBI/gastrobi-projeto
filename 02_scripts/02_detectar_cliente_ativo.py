# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 02_detectar_cliente_ativo.py
# AUTOR: Sergio Paulo dos Santos
# DATA: 2026-04-29
# FINALIDADE:
# Identificar automaticamente qual cliente está ativo dentro
# da pasta 01_clientes do Google Drive compartilhado.
# ==========================================================

import os

# ==========================================================
# CAMINHO CORRETO INFORMADO PELO USUÁRIO
# ==========================================================
PASTA_CLIENTES = r"G:\Drives compartilhados\V2_GASTROBI\01_clientes"

# ==========================================================
# FUNÇÃO PRINCIPAL
# ==========================================================
def detectar_cliente_ativo():

    try:
        pastas = os.listdir(PASTA_CLIENTES)

        clientes_ativos = []

        for pasta in pastas:

            caminho_completo = os.path.join(PASTA_CLIENTES, pasta)

            if os.path.isdir(caminho_completo):

                if pasta.lower().strip().endswith("_ativo"):
                    clientes_ativos.append(pasta)

        # --------------------------------------------------
        # RESULTADOS
        # --------------------------------------------------
        if len(clientes_ativos) == 0:
            print("Nenhum cliente ativo encontrado.")
            return None

        elif len(clientes_ativos) == 1:
            cliente = clientes_ativos[0]
            print("Cliente ativo encontrado:", cliente)
            return cliente

        else:
            print("Mais de um cliente ativo encontrado:")

            for cliente in clientes_ativos:
                print("-", cliente)

            print("Deixe somente 1 cliente com final _ativo.")
            return None

    except Exception as erro:
        print("Erro ao detectar cliente ativo:", erro)
        return None

# ==========================================================
# TESTE LOCAL
# ==========================================================
if __name__ == "__main__":

    detectar_cliente_ativo()