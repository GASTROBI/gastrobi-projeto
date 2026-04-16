/*******************************************************************************
* NOME DO PROJETO: GASTROBI - BUSINESS INTELLIGENCE PARA FOOD SERVICE
* CONSULTOR RESPONSÁVEL: SÉRGIO PAULO DOS SANTOS
* CLIENTE: CAFÉ GOURMET
* DOCUMENTO: KPI_MATRIZ_BCG_CAFÉ
* DATA: 16/04/2026
*
* DESCRIÇÃO: CLASSIFICAÇÃO ESTRATÉGICA DOS PRODUTOS DO CAFÉ GOURMET.
* IDENTIFICAÇÃO DE ITENS COM PREJUÍZO (ABACAXIS) E OPORTUNIDADES.
*******************************************************************************/

WITH estatisticas AS (
    SELECT 
        produto,
        SUM(qtd) as volume,
        SUM(valor_total - (custo_total_insumos * qtd)) as lucro_total_bruto,
        AVG((valor_total - (custo_total_insumos * qtd)) / NULLIF(valor_total, 0)) * 100 as margem_media_pct
    FROM `gastrobi-core-profissional.cafe_gourmet_lab.fato_vendas_validada`
    GROUP BY produto
),
medias AS (
    SELECT 
        AVG(volume) as media_volume,
        AVG(lucro_total_bruto) as media_lucro
    FROM estatisticas
)
SELECT 
    e.produto,
    e.volume,
    ROUND(e.lucro_total_bruto, 2) as lucro_bruto,
    ROUND(e.margem_media_pct, 2) as margem_pct,
    CASE 
        WHEN e.lucro_total_bruto < 0 THEN '⚠️ PREJUÍZO CRÍTICO (Preço abaixo do custo)'
        WHEN e.volume >= m.media_volume AND e.lucro_total_bruto >= m.media_lucro THEN '🌟 ESTRELA (Vende muito / Lucra muito)'
        WHEN e.volume >= m.media_volume AND e.lucro_total_bruto < m.media_lucro THEN '🐄 VACA LEITEIRA (Gira muito / Lucro baixo)'
        WHEN e.volume < m.media_volume AND e.lucro_total_bruto >= m.media_lucro THEN '❓ INTERROGAÇÃO (Vende pouco / Lucro alto)'
        ELSE ' Pineapple ABACAXI (Vende pouco / Lucro baixo)'
    END as classificacao_bcg
FROM estatisticas e, medias m
ORDER BY lucro_total_bruto DESC;

/* KPI ESPECIAL: MATRIZ BCG GASTROBI
VALIDAÇÃO: CONSULTOR SERGIO SANTOS
CLASSIFICAÇÃO: Estrela, Vaca Leiteira, Interrogação ou Abacaxi
*/

WITH estatisticas AS (
    SELECT 
        produto,
        SUM(qtd) as volume,
        SUM(valor_total - custo_total_insumos) as lucro_total_bruto,
        AVG((valor_total - custo_total_insumos) / NULLIF(valor_total, 0)) * 100 as margem_media_pct
    FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
    GROUP BY produto
),
medias AS (
    SELECT 
        AVG(volume) as media_volume,
        AVG(lucro_total_bruto) as media_lucro
    FROM estatisticas
)
SELECT 
    e.produto,
    e.volume,
    ROUND(e.lucro_total_bruto, 2) as lucro_bruto,
    ROUND(e.margem_media_pct, 2) as margem_pct,
    CASE 
        WHEN e.volume >= m.media_volume AND e.lucro_total_bruto >= m.media_lucro THEN '🌟 ESTRELA (Vende muito / Lucra muito)'
        WHEN e.volume >= m.media_volume AND e.lucro_total_bruto < m.media_lucro THEN '🐄 VACA LEITEIRA (Gira muito / Lucro baixo)'
        WHEN e.volume < m.media_volume AND e.lucro_total_bruto >= m.media_lucro THEN '❓ INTERROGAÇÃO (Vende pouco / Lucro alto)'
        ELSE '🍍 ABACAXI (Vende pouco / Lucro baixo)'
    END as classificacao_bcg
FROM estatisticas e, medias m
ORDER BY lucro_total_bruto DESC;

/*
KPI 01: FATURAMENTO LÍQUIDO VS BRUTO (COM TAXAS/GORJETAS)
VALIDAÇÃO: GASTROBI - SERGIO SANTOS
*/

SELECT 
    nome_cliente,
    SUM(valor_total) as receita_liquida,
    SUM(gorjeta) as total_gorjetas,
    SUM(faturamento_bruto) as receita_total_com_taxas,
    ROUND((SUM(gorjeta) / NULLIF(SUM(valor_total), 0)) * 100, 2) as percentual_taxa_media
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY 1;

CREATE OR REPLACE TABLE `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada` (
    data DATE,
    produto STRING,
    ncm STRING,
    tributacao STRING,
    categoria STRING,
    qtd FLOAT64,
    valor_unitario FLOAT64,
    valor_total FLOAT64,
    faturamento_bruto FLOAT64,
    gorjeta FLOAT64,
    custo_total_insumos FLOAT64, -- KPI de cruzamento
    canal_vendas STRING,
    forma_pagamento STRING, 
    nome_cliente STRING
);

/*
PROJETO: GastroBI - Inteligência de Negócio para Food Service
AUTOR: Sergio Santos | CONSULTORIA ESTRATÉGICA
DATA DE VALIDAÇÃO: 15/04/2026
VERSÃO: 2.1 (Reconstrução de Base Mestre - 30 Linhas Validadas)
---------------------------------------------------------------------------
DIRETRIZ JURÍDICA: Este código e a estrutura de dados resultante são 
propriedade intelectual da GastroBI. O saneamento aplicado garante a 
rastreabilidade de tributação (Monofásicos/Bifásicos) e Lucro Real.
---------------------------------------------------------------------------
*/

-- 1. LIMPEZA PREVENTIVA (Garante que não haverá duplicidade no teste)
TRUNCATE TABLE `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`;

-- 2. INSERÇÃO DAS 30 LINHAS DE INTELIGÊNCIA
INSERT INTO `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada` (
    data, produto, ncm, tributacao, categoria, qtd, valor_unitario, 
    valor_total, faturamento_bruto, gorjeta, custo_total_insumos, 
    canal_vendas, forma_pagamento, nome_cliente
)
VALUES
('2026-04-15', 'BURGER ARTESANAL BACON', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 35.90, 35.90, 39.49, 3.59, 15.50, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'COCA-COLA LATA', '22021000', 'BIFÁSICO', 'BEBIDAS', 2, 6.00, 12.00, 12.00, 0.00, 4.20, 'BALCÃO', 'DINHEIRO', 'CLIENTE_TESTE'),
('2026-04-15', 'CHEESEBURGER CLASSICO', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 28.00, 28.00, 28.00, 0.00, 12.10, 'BALCÃO', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'BURGER ARTESANAL DUPLO', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 42.00, 42.00, 46.20, 4.20, 18.20, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'BURGER VEGETARIANO', '21069090', 'ALÍQUOTA ZERO', 'LANCHES', 1, 32.00, 32.00, 35.20, 3.20, 14.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'SUCO LARANJA NATURAL', '20091900', 'SUBSTITUIÇÃO', 'BEBIDAS', 1, 9.00, 9.00, 9.90, 0.90, 3.50, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'CERVEJA ARTESANAL IPA', '22030000', 'BIFÁSICO', 'BEBIDAS', 1, 18.00, 18.00, 18.00, 0.00, 9.00, 'BALCÃO', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'AGUA MINERAL SEM GAS', '22011000', 'SUBSTITUIÇÃO', 'BEBIDAS', 3, 4.50, 13.50, 13.50, 0.00, 1.50, 'WHATSAPP', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'BATATA FRITA MEDIA', '20041000', 'SUBSTITUIÇÃO', 'PORÇÕES', 1, 15.00, 15.00, 16.50, 1.50, 5.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'ANEL DE CEBOLA', '20049000', 'SUBSTITUIÇÃO', 'PORÇÕES', 1, 18.00, 18.00, 18.00, 0.00, 6.50, 'BALCÃO', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'BATATA FRITA GRANDE', '20041000', 'SUBSTITUIÇÃO', 'PORÇÕES', 1, 22.00, 22.00, 22.00, 0.00, 7.50, 'WHATSAPP', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'BROWNIE ARTESANAL', '19059090', 'MONOFÁSICO', 'SOBREMESA', 2, 12.00, 24.00, 26.40, 2.40, 4.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'MILKSHAKE BAUNILHA', '21050090', 'SUBSTITUIÇÃO', 'SOBREMESA', 1, 19.00, 19.00, 19.00, 0.00, 8.20, 'BALCÃO', 'DINHEIRO', 'CLIENTE_TESTE'),
('2026-04-15', 'COMBO FAMILIA 4 BURGERS', '16025000', 'MONOFÁSICO', 'COMBOS', 1, 120.00, 120.00, 120.00, 0.00, 48.00, 'WHATSAPP', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'CHEESEBURGER CLASSICO', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 28.00, 28.00, 30.80, 2.80, 12.10, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'COCA-COLA 2L', '22021000', 'BIFÁSICO', 'BEBIDAS', 1, 14.00, 14.00, 15.40, 1.40, 7.50, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'GUARANA ANTARCTICA LATA', '22021000', 'BIFÁSICO', 'BEBIDAS', 2, 6.00, 12.00, 12.00, 0.00, 4.20, 'BALCÃO', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'BURGER ARTESANAL BACON', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 35.90, 35.90, 35.90, 0.00, 15.50, 'BALCÃO', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'FRITAS COM CHEDDAR E BACON', '20041000', 'SUBSTITUIÇÃO', 'PORÇÕES', 1, 28.00, 28.00, 30.80, 2.80, 9.50, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'SUCO DE UVA INTEGRAL', '20096100', 'SUBSTITUIÇÃO', 'BEBIDAS', 1, 12.00, 12.00, 13.20, 1.20, 5.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'AGUA COM GAS', '22011000', 'SUBSTITUIÇÃO', 'BEBIDAS', 1, 5.00, 5.00, 5.00, 0.00, 1.60, 'BALCÃO', 'DINHEIRO', 'CLIENTE_TESTE'),
('2026-04-15', 'ADICIONAL DE BACON', '16025000', 'MONOFÁSICO', 'ADICIONAIS', 1, 5.00, 5.00, 5.50, 0.50, 1.50, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'ADICIONAL DE QUEIJO', '04069010', 'SUBSTITUIÇÃO', 'ADICIONAIS', 1, 4.00, 4.00, 4.40, 0.40, 1.20, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'MAYONESE ARTESANAL EXTRA', '21039011', 'SUBSTITUIÇÃO', 'ADICIONAIS', 1, 3.50, 3.50, 3.50, 0.00, 0.80, 'BALCÃO', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'BURGER ARTESANAL DUPLO', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 42.00, 42.00, 42.00, 0.00, 18.20, 'WHATSAPP', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'NUGGETS DE FRANGO 10UN', '16023220', 'MONOFÁSICO', 'PORÇÕES', 1, 20.00, 20.00, 22.00, 2.00, 7.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'PUDIM DE LEITE', '19019090', 'SUBSTITUIÇÃO', 'SOBREMESA', 1, 10.00, 10.00, 11.00, 1.00, 3.00, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE'),
('2026-04-15', 'CERVEJA LATA 350ML', '22030000', 'BIFÁSICO', 'BEBIDAS', 3, 7.00, 21.00, 21.00, 0.00, 10.50, 'BALCÃO', 'DINHEIRO', 'CLIENTE_TESTE'),
('2026-04-15', 'CHEESEBURGER CLASSICO', '16025000', 'MONOFÁSICO', 'LANCHES', 1, 28.00, 28.00, 28.00, 0.00, 12.10, 'BALCÃO', 'PIX', 'CLIENTE_TESTE'),
('2026-04-15', 'COCA-COLA LATA', '22021000', 'BIFÁSICO', 'BEBIDAS', 1, 6.00, 6.00, 6.60, 0.60, 2.10, 'IFOOD', 'CARTÃO', 'CLIENTE_TESTE');

/* KPI 02/03: ENGENHARIA DE CARDÁPIO & LUCRO BRUTO */
SELECT 
    produto,
    SUM(qtd) as total_vendido,
    SUM(valor_total) as faturamento_liquido,
    SUM(custo_total_insumos) as custo_mercadoria_total,
    SUM(valor_total) - SUM(custo_total_insumos) as lucro_bruto_moeda,
    ROUND(((SUM(valor_total) - SUM(custo_total_insumos)) / SUM(valor_total)) * 100, 2) as margem_contribuicao_pct
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY produto
ORDER BY lucro_bruto_moeda DESC; 

/* KPI 04/05: TICKET MÉDIO E PERFORMANCE POR PEDIDO */
SELECT 
    nome_cliente,
    COUNT(*) as total_vendas,
    SUM(valor_total) as receita_total,
    ROUND(SUM(valor_total) / COUNT(*), 2) as ticket_medio,
    ROUND(SUM(custo_total_insumos) / COUNT(*), 2) as custo_medio_pedido
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY 1;]

/* KPI 07: CONCENTRAÇÃO DE RECEITA POR CATEGORIA */
SELECT 
    categoria,
    COUNT(*) as volume_pedidos,
    SUM(valor_total) as faturamento_liquido,
    ROUND((SUM(valor_total) / (SELECT SUM(valor_total) FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`)) * 100, 2) as representatividade_pct
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY categoria
ORDER BY faturamento_liquido DESC;

/* KPI 08: ANÁLISE DE MEIOS DE PAGAMENTO */
SELECT 
    forma_pagamento,
    SUM(valor_total) as total_recebido,
    COUNT(*) as volume_transacoes,
    ROUND((SUM(valor_total) / (SELECT SUM(valor_total) FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`)) * 100, 2) as representatividade_pct
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY forma_pagamento
ORDER BY total_recebido DESC;

/* KPI 06: PERFORMANCE DE PRODUTOS */
SELECT 
    produto,
    SUM(qtd) as quantidade
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY produto
ORDER BY quantidade DESC
LIMIT 5;

/* KPI 09: SEGREGAÇÃO FISCAL PARA RECUPERAÇÃO TRIBUTÁRIA */
SELECT 
    tributacao,
    SUM(valor_total) as base_calculo,
    COUNT(*) as total_itens,
    CASE 
        WHEN tributacao = 'MONOFÁSICO' THEN '✅ Isento de PIS/COFINS (Recuperável)'
        ELSE '⚠️ Tributação Normal'
    END as status_fiscal
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY tributacao;

/* KPI 10: LUCRATIVIDADE POR CANAL DE VENDA */
SELECT 
    canal_vendas,
    SUM(valor_total) as faturamento_liquido,
    SUM(gorjeta) as total_taxas_plataforma,
    SUM(custo_total_insumos) as custo_mercadoria,
    SUM(valor_total) - SUM(custo_total_insumos) - SUM(gorjeta) as lucro_real_estimado
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY canal_vendas
ORDER BY lucro_real_estimado DESC;

/* KPI 11: AUDITORIA DE TAXAS E GORJETAS POR CANAL */
SELECT 
    canal_vendas,
    SUM(valor_total) as faturamento_liquido,
    SUM(gorjeta) as total_gorjetas,
    ROUND(AVG(gorjeta), 2) as media_gorjeta_por_item
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
WHERE gorjeta > 0
GROUP BY canal_vendas;

/* KPI 12: MARGEM LÍQUIDA REAL POR CANAL (KPI DE DECISÃO) */
SELECT 
    canal_vendas,
    SUM(valor_total) as receita_liquida,
    SUM(custo_total_insumos) as custo_mercadoria,
    SUM(gorjeta) as taxas_estimadas,
    SUM(valor_total) - SUM(custo_total_insumos) - SUM(gorjeta) as sobra_caixa_real,
    ROUND(((SUM(valor_total) - SUM(custo_total_insumos) - SUM(gorjeta)) / SUM(valor_total)) * 100, 2) as margem_final_pct
FROM `gastrobi-core-profissional.gastrobi_lab.fato_vendas_validada`
GROUP BY canal_vendas
ORDER BY sobra_caixa_real DESC;



