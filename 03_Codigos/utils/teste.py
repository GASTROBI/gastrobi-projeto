from google.cloud import bigquery
try:
    client = bigquery.Client(project="gastrobi-core-profissional")
    print("📡 Tentando conexão...")
    datasets = list(client.list_datasets())
    print("✅ SUCESSO! O motor do GastroBI está conectado.")
except Exception as e:
    print(f"❌ Erro: {e}")