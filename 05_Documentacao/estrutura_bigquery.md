# Estrutura de Dados - GastroBI_Core

## Conjuntos de Dados (Datasets) no BigQuery
Os datasets foram criados via script Python utilizando a região `southamerica-east1` (São Paulo).

- [cite_start]**raw**: Armazena os dados brutos recebidos dos clientes (Excel/CSV)[cite: 52, 64].
- **staging**: Área de limpeza e transformação. [cite_start]O Python processa os dados aqui antes do cálculo final[cite: 57, 65].
- [cite_start]**analytics**: Camada final com os 12 KPIs calculados (CMV, Margem, etc.), conectada ao Looker Studio[cite: 60, 66].

## Tabelas Planejadas
- [cite_start]`vendas_raw`: Tabela inicial para teste de carga de vendas[cite: 75, 81].
- [cite_start]`ncm_mestre_raw`: Tabela de referência para impostos monofásicos[cite: 84].