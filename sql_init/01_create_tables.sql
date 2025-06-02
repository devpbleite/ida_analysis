-- Script SQL para criação das tabelas do Data Mart IDA Anatel
-- Modelo Estrela

-- Dimensão Tempo
CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano_mes VARCHAR(7) UNIQUE NOT NULL -- Formato YYYY-MM
);

COMMENT ON TABLE dim_tempo IS 'Dimensão que armazena informações temporais (ano e mês) das medições do IDA.';
COMMENT ON COLUMN dim_tempo.id_tempo IS 'Identificador único da dimensão tempo (chave primária).';
COMMENT ON COLUMN dim_tempo.ano IS 'Ano da medição do IDA.';
COMMENT ON COLUMN dim_tempo.mes IS 'Mês da medição do IDA (1 a 12).';
COMMENT ON COLUMN dim_tempo.ano_mes IS 'Combinação de ano e mês no formato YYYY-MM (chave única).';

-- Dimensão Grupo Econômico
CREATE TABLE dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome VARCHAR(255) UNIQUE NOT NULL
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensão que armazena os nomes dos grupos econômicos das operadoras.';
COMMENT ON COLUMN dim_grupo_economico.id_grupo IS 'Identificador único do grupo econômico (chave primária).';
COMMENT ON COLUMN dim_grupo_economico.nome IS 'Nome do grupo econômico (ex: CLARO, OI, VIVO) (chave única).';

-- Dimensão Serviço
CREATE TABLE dim_servico (
    id_servico SERIAL PRIMARY KEY,
    sigla VARCHAR(10) UNIQUE NOT NULL,
    nome VARCHAR(255) NOT NULL
);

COMMENT ON TABLE dim_servico IS 'Dimensão que armazena os tipos de serviço de telecomunicação avaliados pelo IDA.';
COMMENT ON COLUMN dim_servico.id_servico IS 'Identificador único do serviço (chave primária).';
COMMENT ON COLUMN dim_servico.sigla IS 'Sigla do serviço (ex: SCM, SMP, STFC) (chave única).';
COMMENT ON COLUMN dim_servico.nome IS 'Nome completo do serviço (ex: Banda Larga Fixa, Serviço Móvel Pessoal, Serviço Telefônico Fixo Comutado).';

-- Dimensão Métrica
CREATE TABLE dim_metrica (
    id_metrica SERIAL PRIMARY KEY,
    nome VARCHAR(500) UNIQUE NOT NULL
);

COMMENT ON TABLE dim_metrica IS 'Dimensão que armazena os nomes das diferentes métricas/variáveis que compõem ou são derivadas do IDA.';
COMMENT ON COLUMN dim_metrica.id_metrica IS 'Identificador único da métrica/variável (chave primária).';
COMMENT ON COLUMN dim_metrica.nome IS 'Nome da métrica ou variável reportada nos arquivos ODS (ex: Indicador de Desempenho no Atendimento (IDA), Taxa de reclamações respondidas em até 5 dias úteis) (chave única).';

-- Tabela Fato IDA
CREATE TABLE fato_ida (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL,
    id_grupo INTEGER NOT NULL,
    id_servico INTEGER NOT NULL,
    id_metrica INTEGER NOT NULL,
    valor NUMERIC,
    FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo),
    FOREIGN KEY (id_grupo) REFERENCES dim_grupo_economico(id_grupo),
    FOREIGN KEY (id_servico) REFERENCES dim_servico(id_servico),
    FOREIGN KEY (id_metrica) REFERENCES dim_metrica(id_metrica)
);

COMMENT ON TABLE fato_ida IS 'Tabela fato que armazena os valores numéricos das métricas do IDA, conectando as dimensões.';
COMMENT ON COLUMN fato_ida.id_fato IS 'Identificador único da linha na tabela fato (chave primária).';
COMMENT ON COLUMN fato_ida.id_tempo IS 'Chave estrangeira referenciando a dimensão Tempo (dim_tempo).';
COMMENT ON COLUMN fato_ida.id_grupo IS 'Chave estrangeira referenciando a dimensão Grupo Econômico (dim_grupo_economico).';
COMMENT ON COLUMN fato_ida.id_servico IS 'Chave estrangeira referenciando a dimensão Serviço (dim_servico).';
COMMENT ON COLUMN fato_ida.id_metrica IS 'Chave estrangeira referenciando a dimensão Métrica (dim_metrica).';
COMMENT ON COLUMN fato_ida.valor IS 'Valor numérico da métrica para a combinação específica das dimensões. Pode ser nulo se o dado não estiver disponível.';


