# ==========================================================
# V2_GASTROBI - SENTINELA V2 VISUAL
# Sergio + ChatGPT
# Monitoramento multiclientes com alerta por e-mail
# ==========================================================

from google.cloud import bigquery
from email.mime.text import MIMEText
import smtplib
from datetime import datetime, date
import pandas as pd
import time
import traceback

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================

PROJECT_ID = "v2-gastrobi-lab"
LOCATION = "US"

EMAIL_REMETENTE = "alertasgastrobiv2@gmail.com"
EMAIL_DESTINO = "sergioconsultormogi@gmail.com"
EMAIL_SENHA_APP = "bpgokgswgletdbmf"

TABELA = "fato_vendas"

# ==========================================================
# EMAIL
# ==========================================================

def enviar_email(assunto, mensagem):

    try:
        print("📧 Enviando e-mail...")

        msg = MIMEText(mensagem, "plain", "utf-8")
        msg["Subject"] = assunto
        msg["From"] = EMAIL_REMETENTE
        msg["To"] = EMAIL_DESTINO

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
            servidor.login(EMAIL_REMETENTE, EMAIL_SENHA_APP)
            servidor.sendmail(
                EMAIL_REMETENTE,
                EMAIL_DESTINO,
                msg.as_string()
            )

        print("✅ E-mail enviado com sucesso!\n")

    except Exception as e:
        print("❌ Falha ao enviar email:", e)

# ==========================================================
# LISTAR DATASETS
# ==========================================================

def listar_datasets(client):

    lista = []

    for ds in client.list_datasets():
        if ds.dataset_id.endswith("_ativo"):
            lista.append(ds.dataset_id)

    return lista

# ==========================================================
# ANALISAR CLIENTE
# ==========================================================

def analisar_cliente(client, dataset):

    print(f"🔍 Analisando {dataset}...")

    alertas = []

    try:

        tabela_ref = f"{PROJECT_ID}.{dataset}.{TABELA}"

        sql = f"""
        SELECT
          COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', Data),
            SAFE.PARSE_DATE('%Y-%m-%d', Data)
          ) AS Data,
          faturamento_bruto,
          qtd,
          custo_unitario
        FROM `{tabela_ref}`
        WHERE COALESCE(
          SAFE.PARSE_DATE('%d/%m/%Y', Data),
          SAFE.PARSE_DATE('%Y-%m-%d', Data)
        ) >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
        """

        df = client.query(sql, location=LOCATION).to_dataframe()

        print(f"   📊 {len(df)} linhas carregadas")

        if df.empty:
            alertas.append(f"🔴 {dataset}: base vazia.")
            return alertas

        hoje = date.today()

        ultima_data = df["Data"].max()
        dias = (hoje - ultima_data).days

        if dias > 2:
            alertas.append(f"🔴 {dataset}: sem dados há {dias} dias.")

        venda_hoje = df[df["Data"] == hoje]["faturamento_bruto"].sum()

        if venda_hoje == 0:
            alertas.append(f"🟠 {dataset}: sem faturamento hoje.")

        media7 = (
            df[df["Data"] < hoje]
            .groupby("Data")["faturamento_bruto"]
            .sum()
            .tail(7)
            .mean()
        )

        if pd.notna(media7) and media7 > 0:
            if venda_hoje < media7 * 0.80:
                alertas.append(
                    f"🟡 {dataset}: faturamento caiu. Hoje {venda_hoje:.2f} / Média {media7:.2f}"
                )

        df["custo_total"] = df["qtd"] * df["custo_unitario"]

        receita = df["faturamento_bruto"].sum()
        custo = df["custo_total"].sum()

        if receita > 0:
            cmv = custo / receita

            if cmv > 0.40:
                alertas.append(
                    f"🟣 {dataset}: CMV alto {cmv*100:.2f}%"
                )

        if not alertas:
            print("   🟢 Cliente normal")
        else:
            print(f"   ⚠️ {len(alertas)} alerta(s) encontrado(s)")

    except Exception as e:

        alertas.append(f"🚨 {dataset}: erro técnico -> {e}")
        print("   ❌ Erro técnico")

    print()
    time.sleep(1)

    return alertas

# ==========================================================
# PRINCIPAL
# ==========================================================

def executar():

    try:

        print("\n===================================")
        print("🚀 SENTINELA GASTROBI V2 VISUAL")
        print("===================================\n")

        client = bigquery.Client(project=PROJECT_ID)

        print("🔗 Conectado ao BigQuery")

        datasets = listar_datasets(client)

        print(f"📦 {len(datasets)} clientes encontrados\n")

        todos_alertas = []

        for ds in datasets:
            retorno = analisar_cliente(client, ds)
            todos_alertas.extend(retorno)

        print("===================================")

        if todos_alertas:

            print(f"⚠️ Total de alertas: {len(todos_alertas)}")

            corpo = "✅ Sistema funcionando normalmente.\n"
            corpo += "⚠️ RELATÓRIO SENTINELA MULTICLIENTES\n\n"
            corpo += "\n".join(todos_alertas)
            corpo += f"\n\nExecutado em {datetime.now()}"

            enviar_email(
                "⚠️ ALERTAS GASTROBI",
                corpo
            )

        else:

            print("🟢 Nenhum problema encontrado")

            enviar_email(
                "🟢 GASTROBI NORMAL",
                f"""✅ Sistema funcionando normalmente.

Todos clientes normais.

Executado em {datetime.now()}"""
            )

        print("🏁 Finalizado com sucesso.")
        print("===================================\n")

    except Exception:

        erro = traceback.format_exc()

        print("❌ ERRO GERAL")
        print(erro)

        enviar_email(
            "🚨 ERRO GERAL GASTROBI",
            erro
        )

# ==========================================================
# EXECUTAR
# ==========================================================

if __name__ == "__main__":
    executar()