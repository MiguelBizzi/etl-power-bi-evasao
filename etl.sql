-- =========================================================
-- ETL COMPLETO (INEP + IBGE)
-- PostgreSQL 18 — Execução via psql
-- Fontes em: C:\Program Files\PostgreSQL\18\data
-- =========================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS transformacao;
CREATE SCHEMA IF NOT EXISTS dw;

-- =========================================================
-- ETAPA 1: EXTRAÇÃO
-- =========================================================

-- ---------- INEP: Taxas de rendimento (normalizado via script.py) ----------
DROP TABLE IF EXISTS staging.stg_inep_rendimento;
CREATE TABLE IF NOT EXISTS staging.stg_inep_rendimento (
    ano INT,
    regiao VARCHAR(50),
    uf VARCHAR(2),
    dependencia VARCHAR(50),
    nivel VARCHAR(20),
    taxa_aprovacao FLOAT,
    taxa_reprovacao FLOAT,
    taxa_abandono FLOAT
);

\copy staging.stg_inep_rendimento
FROM 'input-csv/processed/inep_rendimento.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');


-- ---------- INEP: Taxa de distorção idade-série (normalizado via script.py) ----------
DROP TABLE IF EXISTS staging.stg_inep_distorcao;
CREATE TABLE IF NOT EXISTS staging.stg_inep_distorcao (
    ano INT,
    regiao VARCHAR(50),
    uf VARCHAR(2),
    dependencia VARCHAR(50),
    nivel VARCHAR(20),
    taxa_distorcao FLOAT
);

\copy staging.stg_inep_distorcao
FROM 'input-csv/processed/inep_distorcao.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');


-- ---------- IBGE: Taxa de analfabetismo (normalizado via script.py) ----------
DROP TABLE IF EXISTS staging.stg_ibge_analfabetismo;
CREATE TABLE IF NOT EXISTS staging.stg_ibge_analfabetismo (
    ano INT,
    regiao VARCHAR(50),
    uf VARCHAR(2),
    taxa_analfabetismo FLOAT
);

\copy staging.stg_ibge_analfabetismo
FROM 'input-csv/processed/ibge_analfabetismo.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');


-- ---------- IBGE: Motivos para evasão (normalizado via script.py) ----------
DROP TABLE IF EXISTS staging.stg_ibge_motivos;
CREATE TABLE IF NOT EXISTS staging.stg_ibge_motivos (
    ano INT,
    motivo VARCHAR(200),
    sexo VARCHAR(20),
    faixa_etaria VARCHAR(30),
    percentual FLOAT
);

\copy staging.stg_ibge_motivos
FROM 'input-csv/processed/ibge_motivos.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');


-- =========================================================
-- ETAPA 2: TRANSFORMAÇÃO
-- =========================================================

CREATE OR REPLACE VIEW transformacao.vw_inep_integrado AS
SELECT
    r.ano,
    upper(r.uf) AS uf,
    initcap(r.regiao) AS regiao,
    initcap(r.dependencia) AS dependencia,
    initcap(r.nivel) AS nivel,
    ROUND(COALESCE(r.taxa_aprovacao, 0), 2) AS taxa_aprovacao,
    ROUND(COALESCE(r.taxa_reprovacao, 0), 2) AS taxa_reprovacao,
    ROUND(COALESCE(r.taxa_abandono, 0), 2) AS taxa_abandono,
    ROUND(COALESCE(d.taxa_distorcao, 0), 2) AS taxa_distorcao
FROM staging.stg_inep_rendimento r
LEFT JOIN staging.stg_inep_distorcao d
    ON r.uf = d.uf AND r.ano = d.ano AND r.dependencia = d.dependencia AND r.nivel = d.nivel;

CREATE OR REPLACE VIEW transformacao.vw_ibge_integrado AS
SELECT
    a.ano,
    upper(a.uf) AS uf,
    initcap(a.regiao) AS regiao,
    ROUND(COALESCE(a.taxa_analfabetismo, 0), 2) AS taxa_analfabetismo,
    m.motivo,
    initcap(m.sexo) AS sexo,
    m.faixa_etaria,
    ROUND(COALESCE(m.percentual, 0), 2) AS percentual_motivo
FROM staging.stg_ibge_analfabetismo a
LEFT JOIN staging.stg_ibge_motivos m
    ON a.ano = m.ano;


-- =========================================================
-- ETAPA 3: CARGA
-- =========================================================

CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INT UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_localidade (
    id_localidade SERIAL PRIMARY KEY,
    uf VARCHAR(2),
    regiao VARCHAR(50),
    UNIQUE (uf, regiao)
);

CREATE TABLE IF NOT EXISTS dw.dim_dependencia (
    id_dependencia SERIAL PRIMARY KEY,
    tipo_dependencia VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_perfil_socio (
    id_perfil SERIAL PRIMARY KEY,
    sexo VARCHAR(20),
    faixa_etaria VARCHAR(30),
    motivo_evasao VARCHAR(200),
    UNIQUE (sexo, faixa_etaria, motivo_evasao)
);

CREATE TABLE IF NOT EXISTS dw.dim_nivel (
    id_nivel SERIAL PRIMARY KEY,
    nivel VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.fato_evasao (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INT REFERENCES dw.dim_tempo(id_tempo),
    id_localidade INT REFERENCES dw.dim_localidade(id_localidade),
    id_dependencia INT REFERENCES dw.dim_dependencia(id_dependencia),
    id_nivel INT REFERENCES dw.dim_nivel(id_nivel),
    id_perfil INT REFERENCES dw.dim_perfil_socio(id_perfil),
    taxa_aprovacao FLOAT,
    taxa_reprovacao FLOAT,
    taxa_abandono FLOAT,
    taxa_distorcao FLOAT,
    taxa_analfabetismo FLOAT,
    percentual_motivo FLOAT
);

-- Carga das dimensões
INSERT INTO dw.dim_tempo (ano)
SELECT DISTINCT ano FROM transformacao.vw_inep_integrado
ON CONFLICT (ano) DO NOTHING;

INSERT INTO dw.dim_dependencia (tipo_dependencia)
SELECT DISTINCT dependencia FROM transformacao.vw_inep_integrado
WHERE dependencia IS NOT NULL
ON CONFLICT (tipo_dependencia) DO NOTHING;

INSERT INTO dw.dim_localidade (uf, regiao)
SELECT DISTINCT uf, regiao FROM transformacao.vw_inep_integrado
ON CONFLICT (uf, regiao) DO NOTHING;

INSERT INTO dw.dim_perfil_socio (sexo, faixa_etaria, motivo_evasao)
SELECT DISTINCT sexo, faixa_etaria, motivo
FROM transformacao.vw_ibge_integrado
ON CONFLICT (sexo, faixa_etaria, motivo_evasao) DO NOTHING;

INSERT INTO dw.dim_nivel (nivel)
SELECT DISTINCT nivel FROM transformacao.vw_inep_integrado
WHERE nivel IS NOT NULL
ON CONFLICT (nivel) DO NOTHING;

-- Carga da tabela fato
TRUNCATE TABLE dw.fato_evasao;

INSERT INTO dw.fato_evasao (
    id_tempo, id_localidade, id_dependencia, id_nivel, id_perfil,
    taxa_aprovacao, taxa_reprovacao, taxa_abandono,
    taxa_distorcao, taxa_analfabetismo, percentual_motivo
)
SELECT
    t.id_tempo,
    l.id_localidade,
    d.id_dependencia,
    n.id_nivel,
    p.id_perfil,
    i.taxa_aprovacao,
    i.taxa_reprovacao,
    i.taxa_abandono,
    i.taxa_distorcao,
    b.taxa_analfabetismo,
    b.percentual_motivo
FROM transformacao.vw_inep_integrado i
JOIN transformacao.vw_ibge_integrado b ON i.ano = b.ano AND i.uf = b.uf
JOIN dw.dim_tempo t ON t.ano = i.ano
JOIN dw.dim_localidade l ON l.uf = i.uf
JOIN dw.dim_dependencia d ON d.tipo_dependencia = i.dependencia
JOIN dw.dim_nivel n ON n.nivel = i.nivel
JOIN dw.dim_perfil_socio p ON p.sexo = b.sexo AND p.faixa_etaria = b.faixa_etaria AND p.motivo_evasao = b.motivo;

-- =========================================================
-- FIM DO PROCESSO ETL
-- =========================================================