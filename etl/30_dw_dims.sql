-- =================================================
-- 30_dw_dims.sql — DDL e carga das dimensões (Type 1)
-- Requer variável :batch_id definida em psql
-- =================================================

\set ON_ERROR_STOP on

-- DDL
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    id_tempo     SERIAL PRIMARY KEY,
    ano          INT UNIQUE,
    source_system TEXT NOT NULL DEFAULT 'CSV',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_localidade (
    id_localidade  SERIAL PRIMARY KEY,
    uf             VARCHAR(2) NOT NULL,
    regiao         VARCHAR(50),
    source_system  TEXT NOT NULL DEFAULT 'CSV',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_localidade UNIQUE (uf, regiao)
);

CREATE TABLE IF NOT EXISTS dw.dim_dependencia (
    id_dependencia  SERIAL PRIMARY KEY,
    tipo_dependencia VARCHAR(50) NOT NULL UNIQUE,
    source_system    TEXT NOT NULL DEFAULT 'CSV',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_nivel (
    id_nivel     SERIAL PRIMARY KEY,
    nivel        VARCHAR(20) NOT NULL UNIQUE,
    source_system TEXT NOT NULL DEFAULT 'CSV',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_perfil_socio (
    id_perfil      SERIAL PRIMARY KEY,
    sexo           VARCHAR(20),
    faixa_etaria   VARCHAR(30),
    motivo_evasao  VARCHAR(200),
    source_system  TEXT NOT NULL DEFAULT 'CSV',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_perfil UNIQUE (sexo, faixa_etaria, motivo_evasao)
);

-- Índices de apoio
CREATE INDEX IF NOT EXISTS idx_dim_localidade_uf ON dw.dim_localidade(uf);
CREATE INDEX IF NOT EXISTS idx_dim_dependencia_tipo ON dw.dim_dependencia(tipo_dependencia);
CREATE INDEX IF NOT EXISTS idx_dim_nivel_nivel ON dw.dim_nivel(nivel);

-- ==================
-- Carga (idempotente)
-- ==================

-- dim_tempo
INSERT INTO dw.dim_tempo(ano)
SELECT DISTINCT ano
FROM transformacao.mvw_inep_integrado
WHERE ano IS NOT NULL
ON CONFLICT (ano) DO NOTHING;

-- também incluir anos presentes no IBGE
INSERT INTO dw.dim_tempo(ano)
SELECT DISTINCT ano
FROM transformacao.mvw_ibge_integrado
WHERE ano IS NOT NULL
ON CONFLICT (ano) DO NOTHING;

-- dim_localidade
INSERT INTO dw.dim_localidade(uf, regiao)
SELECT DISTINCT i.uf, i.regiao
FROM transformacao.mvw_inep_integrado i
JOIN ctl.dom_uf du ON du.uf = i.uf
ON CONFLICT (uf, regiao) DO NOTHING;

-- dim_dependencia
INSERT INTO dw.dim_dependencia(tipo_dependencia)
SELECT DISTINCT dependencia FROM transformacao.mvw_inep_integrado
WHERE dependencia IS NOT NULL
ON CONFLICT (tipo_dependencia) DO NOTHING;

-- dim_nivel
INSERT INTO dw.dim_nivel(nivel)
SELECT DISTINCT nivel FROM transformacao.mvw_inep_integrado
WHERE nivel IS NOT NULL
ON CONFLICT (nivel) DO NOTHING;

-- dim_perfil_socio
-- Limpeza de registros fora do recorte (sexo/faixa não permitidos)
DELETE FROM dw.dim_perfil_socio
WHERE sexo NOT IN ('Masculino','Feminino')
   OR faixa_etaria NOT IN ('15 a 17 anos','18 a 24 anos','25 a 29 anos');

INSERT INTO dw.dim_perfil_socio(sexo, faixa_etaria, motivo_evasao)
SELECT DISTINCT sexo, faixa_etaria, motivo
FROM transformacao.mvw_ibge_integrado
WHERE sexo IS NOT NULL OR faixa_etaria IS NOT NULL OR motivo IS NOT NULL
ON CONFLICT (sexo, faixa_etaria, motivo_evasao) DO NOTHING;


