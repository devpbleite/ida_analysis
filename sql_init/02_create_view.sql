-- Script SQL para criação da View analítica v_taxa_variacao_resolvidas_5d

CREATE OR REPLACE VIEW v_taxa_variacao_resolvidas_5d AS
WITH TaxaResolvidas5d AS (
    -- Seleciona os dados brutos da métrica específica
    SELECT
        f.id_tempo,
        f.id_grupo,
        f.valor
    FROM fato_ida f
    JOIN dim_metrica dm ON f.id_metrica = dm.id_metrica
    WHERE dm.nome = 'Taxa de reclamações respondidas em até 5 dias úteis'
),
MensalGrupo AS (
    -- Agrega por mês e grupo (caso haja múltiplas entradas, embora não esperado pela granularidade)
    -- Junta com dim_tempo para obter ano_mes
    SELECT
        t.ano_mes,
        tr5d.id_grupo,
        AVG(tr5d.valor) AS valor_medio_grupo -- Usar AVG para garantir um valor por mês/grupo
    FROM TaxaResolvidas5d tr5d
    JOIN dim_tempo t ON tr5d.id_tempo = t.id_tempo
    GROUP BY t.ano_mes, tr5d.id_grupo
),
MediaMensalGeral AS (
    -- Calcula a média geral da taxa para cada mês, considerando todos os grupos
    SELECT
        ano_mes,
        AVG(valor_medio_grupo) AS valor_medio_geral
    FROM MensalGrupo
    GROUP BY ano_mes
),
VariacaoMediaGeral AS (
    -- Calcula a taxa de variação mensal da média geral
    SELECT
        ano_mes,
        valor_medio_geral,
        LAG(valor_medio_geral, 1) OVER (ORDER BY ano_mes) AS valor_medio_geral_anterior,
        -- Calcula a taxa de variação percentual, tratando divisão por zero
        COALESCE(
            ( (valor_medio_geral - LAG(valor_medio_geral, 1) OVER (ORDER BY ano_mes)) / NULLIF(LAG(valor_medio_geral, 1) OVER (ORDER BY ano_mes), 0) ) * 100,
            0 -- Ou NULL, dependendo de como tratar o primeiro mês ou meses com valor anterior zero
        ) AS taxa_variacao_media_geral
    FROM MediaMensalGeral
),
VariacaoIndividual AS (
    -- Calcula a taxa de variação mensal individual para cada grupo econômico
    SELECT
        ano_mes,
        id_grupo,
        valor_medio_grupo,
        LAG(valor_medio_grupo, 1) OVER (PARTITION BY id_grupo ORDER BY ano_mes) AS valor_medio_grupo_anterior,
        -- Calcula a taxa de variação percentual individual, tratando divisão por zero
        COALESCE(
            ( (valor_medio_grupo - LAG(valor_medio_grupo, 1) OVER (PARTITION BY id_grupo ORDER BY ano_mes)) / NULLIF(LAG(valor_medio_grupo, 1) OVER (PARTITION BY id_grupo ORDER BY ano_mes), 0) ) * 100,
            0 -- Ou NULL
        ) AS taxa_variacao_individual
    FROM MensalGrupo
),
DiferencaVariacao AS (
    -- Junta as variações e calcula a diferença
    SELECT
        vi.ano_mes,
        vi.id_grupo,
        vmg.taxa_variacao_media_geral,
        vi.taxa_variacao_individual,
        -- Calcula a diferença entre a taxa individual e a média geral
        vi.taxa_variacao_individual - vmg.taxa_variacao_media_geral AS diferenca_variacao
    FROM VariacaoIndividual vi
    JOIN VariacaoMediaGeral vmg ON vi.ano_mes = vmg.ano_mes
)
-- Pivota o resultado final para ter grupos como colunas
SELECT
    dv.ano_mes AS "Mes",
    -- Arredondar para melhor visualização
    ROUND(MAX(dv.taxa_variacao_media_geral), 2) AS "Taxa de Variação Média",
    -- Pivot para cada grupo econômico encontrado nos dados de 2019
    -- Usar MAX pois GROUP BY agrupa por mês, e queremos o valor daquele grupo para o mês
    ROUND(MAX(CASE WHEN dg.nome = 'ALGAR' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "ALGAR",
    ROUND(MAX(CASE WHEN dg.nome = 'CLARO' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "CLARO",
    ROUND(MAX(CASE WHEN dg.nome = 'EMBRATEL' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "EMBRATEL",
    ROUND(MAX(CASE WHEN dg.nome = 'NET' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "NET", -- Incluído pois aparece em SCM 2019
    ROUND(MAX(CASE WHEN dg.nome = 'NEXTEL' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "NEXTEL", -- Incluído pois aparece em SMP 2019
    ROUND(MAX(CASE WHEN dg.nome = 'OI' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "OI",
    ROUND(MAX(CASE WHEN dg.nome = 'SERCOMTEL' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "SERCOMTEL",
    ROUND(MAX(CASE WHEN dg.nome = 'SKY' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "SKY",
    ROUND(MAX(CASE WHEN dg.nome = 'TIM' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "TIM",
    ROUND(MAX(CASE WHEN dg.nome = 'VIVO' THEN dv.diferenca_variacao ELSE NULL END), 2) AS "VIVO"
    -- Adicionar mais colunas CASE WHEN para outros grupos se necessário
FROM DiferencaVariacao dv
JOIN dim_grupo_economico dg ON dv.id_grupo = dg.id_grupo
GROUP BY dv.ano_mes
ORDER BY dv.ano_mes;

COMMENT ON VIEW v_taxa_variacao_resolvidas_5d IS 'View que calcula a taxa de variação mensal da média da "Taxa de reclamações respondidas em até 5 dias úteis" e a diferença entre essa taxa média e a taxa de variação individual de cada grupo econômico, pivotando os grupos em colunas.';

