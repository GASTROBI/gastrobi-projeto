# ==========================================================
# PROJETO: GASTROBI V2
# ARQUIVO: 09_logs_monitoramento.py
# FINALIDADE: Vigia de Erros com Alerta por E-mail (SMTP Google)
# ==========================================================

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# ==========================================================
# CONFIGURAÇÕES DE CONEXÃO (SEGURANÇA)
# ==========================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_REMETENTE = "sergio@gastrobisolution.com.br"
# Senha de App fornecida
EMAIL_SENHA = "iqdvvszrodbekzan" 
EMAIL_DESTINATARIO = "sergio@gastrobisolution.com.br"

def enviar_alerta_erro(cliente, script, erro):
    """Envia um e-mail imediato se um cliente falhar no fluxo."""
    assunto = f"⚠️ ALERTA CRÍTICO: Erro no Cliente {cliente}"
    
    corpo = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #d9534f;">Falha detectada no Processamento GastroBI V2</h2>
        <p><b>Data/Hora:</b> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        <p><b>Cliente:</b> {cliente}</p>
        <p><b>Script que falhou:</b> {script}</p>
        <hr>
        <p style="color: #333; background: #f9f9f9; padding: 10px; border: 1px solid #ddd;">
            <b>Detalhe do Erro:</b><br>{erro}
        </p>
        <hr>
        <p><i>Este é um alerta automático do seu Motor de Escala GastroBI. Por favor, verifique a pasta do cliente.</i></p>
    </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['From'] = EMAIL_REMETENTE
    msg['To'] = EMAIL_DESTINATARIO
    msg['Subject'] = assunto
    msg.attach(MIMEText(corpo, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_REMETENTE, EMAIL_SENHA)
        server.sendmail(EMAIL_REMETENTE, EMAIL_DESTINATARIO, msg.as_string())
        server.quit()
        print(f">>> Alerta de e-mail enviado com sucesso para {EMAIL_DESTINATARIO}")
    except Exception as e:
        print(f"!!! Falha ao enviar e-mail: {e}")

def registrar_e_notificar(cliente, script, status, mensagem):
    """Função chamada pelo main.py quando ocorre um erro."""
    if status == "ERRO":
        print(f"Disparando alerta de erro para {cliente}...")
        enviar_alerta_erro(cliente, script, mensagem)