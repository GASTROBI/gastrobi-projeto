# ==============================================================================
# PROJETO: GastroBI - Inteligência de Negócios para Food Service
# ARQUIVO: 13_sentinela_alertas.py
# AUTOR: Sergio Paulo dos Santos
# VERSÃO: 5.2 - Sentinela Multiclientes (Padrão Consultoria)
# ==============================================================================

from google.cloud import bigquery
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# ==============================================================================
# CREDENCIAIS
# ==============================================================================

EMAIL_REMETENTE = "alertasgastrobiv2@gmail.com"
SENHA_APP = "bfkgsvzfgibhnpti"
EMAIL_DESTINATARIO = "sergio@gastrobisolution.com.br"

client = bigquery.Client()

# ==============================================================================
# CAMINHOS
# ==============================================================================

PASTA_RAIZ = r"G:\Drives compartilhados\V2_GASTROBI"
PASTA_CLIENTES = os.path.join(PASTA_RAIZ, "01_clientes")

# ==============================================================================
# FUNÇÃO ENVIO DE E-MAIL
# ==============================================================================

def enviar_email(corpo):

    try:

        msg = MIMEMultipart()

        msg['From'] = EMAIL_REMETENTE
        msg['To'] = EMAIL_DESTINATARIO
        msg['Subject'] = (
            f"🛡️ Sentinela GastroBI - "
            f"{datetime.now().strftime('%d/%m/%Y')}"
        )

        msg.attach(MIMEText(corpo, 'plain', 'utf-8'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()

        server.login(EMAIL_REMETENTE, SENHA_APP)

        server.send_message(msg)

        server.quit()

        print("✅ E-mail enviado com sucesso.")

    except Exception as erro:

        print(f"❌ Falha ao enviar e-mail: {erro}")

# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================

def analisar_performance():

    print("\n==================================================")
    print("🛡️ AUDITORIA SENTINELA GASTROBI")
    print("==================================================")

    pastas = [
        p for p in os.listdir(PASTA_CLIENTES)
        if "_ativo" in p.lower()
    ]

    # ==========================================================================
    # CABEÇALHO DO RELATÓRIO
    # ==========================================================================

    relatorio = [

        "✅ Sistema funcionando normalmente.",
        "",
        "⚠️ RELATÓRIO SENTINELA MULTICLIENTES",
        "",
    ]

    # ==========================================================================
    # LOOP CLIENTES
    # ==========================================================================

    for pasta in pastas:

        nome = (
            re.sub(r"^\d+_", "", pasta.lower())
            .replace("_ativo", "")
            .replace("cliente", "")
            .strip("_")
        )

        ds = re.sub(r"[^a-z0-9_]", "", nome)

        exibicao = pasta

        try:

            # ==============================================================
            # CONSULTA FATURAMENTO DO DIA
            # ==============================================================

            sql = f"""
                SELECT 
                    IFNULL(SUM(valor_total), 0) AS fat
                FROM `{client.project}.{ds}.tb_vendas_fato`
                WHERE SAFE.PARSE_DATE('%d/%m/%Y', CAST(data AS STRING)) = CURRENT_DATE()
            """

            resultado = list(client.query(sql).result())[0]

            fat = float(resultado.fat)

            # ==============================================================
            # VALIDAÇÕES
            # ==============================================================

            if fat > 0:

                msg = (
                    f"🟢 {exibicao}: "
                    f"faturamento hoje R$ {fat:,.2f}"
                )

            else:

                msg = (
                    f"🟠 {exibicao}: "
                    f"sem faturamento hoje."
                )

            print(msg)

            relatorio.append(msg)

        except Exception:

            msg_erro = (
                f"🔴 {exibicao}: "
                f"base vazia."
            )

            print(msg_erro)

            relatorio.append(msg_erro)

    # ==========================================================================
    # RODAPÉ
    # ==========================================================================

    relatorio.append("")
    relatorio.append("🛡️ Monitoramento automático GastroBI ativo.")

    # ==========================================================================
    # ENVIA E-MAIL
    # ==========================================================================

    enviar_email("\n".join(relatorio))

    print("==================================================")
    print("✅ FIM DA AUDITORIA")
    print("==================================================")

# ==============================================================================
# EXECUÇÃO
# ==============================================================================

if __name__ == "__main__":
    analisar_performance()