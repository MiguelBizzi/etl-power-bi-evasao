-- =============================================
-- 10_staging.sql — DDL staging e cargas via \copy
-- =============================================

\set ON_ERROR_STOP on

-- Limpeza idempotente
DROP TABLE IF EXISTS staging.stg_inep_rendimento CASCADE;
DROP TABLE IF EXISTS staging.stg_inep_distorcao CASCADE;
DROP TABLE IF EXISTS staging.stg_ibge_analfabetismo CASCADE;
DROP TABLE IF EXISTS staging.stg_ibge_motivos CASCADE;

-- DDL das tabelas de staging
CREATE TABLE staging.stg_inep_rendimento (
    ano               INT,
    regiao            VARCHAR(50),
    uf                VARCHAR(2),
    dependencia       VARCHAR(50),
    nivel             VARCHAR(20),
    taxa_aprovacao    DOUBLE PRECISION,
    taxa_reprovacao   DOUBLE PRECISION,
    taxa_abandono     DOUBLE PRECISION
);

CREATE TABLE staging.stg_inep_distorcao (
    ano               INT,
    regiao            VARCHAR(50),
    uf                VARCHAR(2),
    dependencia       VARCHAR(50),
    nivel             VARCHAR(20),
    taxa_distorcao    DOUBLE PRECISION
);

CREATE TABLE staging.stg_ibge_analfabetismo (
    ano                 INT,
    regiao              VARCHAR(50),
    uf                  VARCHAR(2),
    taxa_analfabetismo  DOUBLE PRECISION
);

CREATE TABLE staging.stg_ibge_motivos (
    ano           INT,
    motivo        VARCHAR(200),
    sexo          VARCHAR(20),
    faixa_etaria  VARCHAR(30),
    percentual    DOUBLE PRECISION
);

-- Cargas via \copy (lado do cliente)
\copy staging.stg_inep_rendimento FROM 'input-csv/processed/inep_rendimento.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

\copy staging.stg_inep_distorcao FROM 'input-csv/processed/inep_distorcao.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

\copy staging.stg_ibge_analfabetismo FROM 'input-csv/processed/ibge_analfabetismo.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');

\copy staging.stg_ibge_motivos FROM 'input-csv/processed/ibge_motivos.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8');


