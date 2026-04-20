"""
PROJETO: GastroBI - Sentinela de Monitoramento
AUTOR: Sergio (Consultoria GastroBI)
VERSÃO: 1.0 (Abril/2026)
MOTIVO: Monitoramento proativo de integridade de dados e alertas de CMV para multiclientes.
"""

import pandas as pd
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

def enviar_alerta(mensagem_corpo):
    remetente = "sergio@gastrobisolution.com.br"
    destinatario = "sergio@gastrobisolution.com.br"
    senha = "yxbx rngm wxtu qsgq"  # Usei a senha que você gerou

    msg = MIMEText(mensagem_corpo)
    # ... resto do código continua igual
    msg = MIMEText(mensagem_corpo)
    msg['Subject'] = f"⚠️ [GastroBI] ALERTA DE VIGILÂNCIA - {datetime.now().strftime('%d/%m/%Y')}"
    msg['From'] = remetente
    msg['To'] = destinatario

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(remetente, senha)
            server.sendmail(remetente, destinatario, msg.as_string())
        print("✅ E-mail de alerta enviado com sucesso!")
    except Exception as e:
        print(f"❌ Falha ao enviar e-mail: {e}")

def verificar_saude_dados():
    client = bigquery.Client(project="gastrobi-core-profissional")
    
    # SQL para identificar inatividade e anomalias
    sql_check = """
    SELECT 
        cliente,
        MAX(data_venda) as ultima_venda,
        AVG(CMV / valor_venda) as perc_cmv_medio
    FROM `gastrobi-core-profissional.staging.vendas_processadas`
    GROUP BY cliente
    """
    
    df = client.query(sql_check).to_dataframe()
    alertas = []
    hoje = datetime.now().date()

    for index, row in df.iterrows():
        # 1. Checagem de Inatividade (Mais de 2 dias sem dados)
        dias_sem_dados = (hoje - row['ultima_venda']).days
        if dias_sem_dados > 2:
            alertas.append(f"🔴 CLIENTE PARADO: {row['cliente']} não envia dados há {dias_sem_dados} dias.")

        # 2. Checagem de CMV Crítico (Acima de 45%)
        if row['perc_cmv_medio'] > 0.45:
            alertas.append(f"🟡 CMV CRÍTICO: {row['cliente']} está com CMV de {row['perc_cmv_medio']*100:.2f}%. Verifique desperdícios.")

    if alertas:
        corpo_email = "Relatório de Inconsistências GastroBI:\n\n" + "\n".join(alertas)
        enviar_alerta(corpo_email)
    else:
        print("🟢 Tudo em ordem com os clientes.")

if __name__ == "__main__":
    verificar_saude_dados()