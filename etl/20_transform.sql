-- =======================================================
-- 20_transform.sql — Materialized Views de transformação
-- =======================================================

\set ON_ERROR_STOP on

-- Drop MVs para rebuild limpo
DROP MATERIALIZED VIEW IF EXISTS transformacao.mvw_inep_integrado;
DROP MATERIALIZED VIEW IF EXISTS transformacao.mvw_ibge_integrado;

-- INEP integrado: rendimento + distorção
CREATE MATERIALIZED VIEW transformacao.mvw_inep_integrado AS
SELECT
    r.ano,
    upper(r.uf)              AS uf,
    initcap(r.regiao)        AS regiao,
    initcap(r.dependencia)   AS dependencia,
    initcap(r.nivel)         AS nivel,
    ROUND(COALESCE(r.taxa_aprovacao, 0)::numeric, 2)   AS taxa_aprovacao,
    ROUND(COALESCE(r.taxa_reprovacao, 0)::numeric, 2)  AS taxa_reprovacao,
    ROUND(COALESCE(r.taxa_abandono, 0)::numeric, 2)    AS taxa_abandono,
    ROUND(COALESCE(d.taxa_distorcao, 0)::numeric, 2)   AS taxa_distorcao
FROM staging.stg_inep_rendimento r
LEFT JOIN staging.stg_inep_distorcao d
  ON r.ano = d.ano
 AND r.uf = d.uf
 AND r.dependencia = d.dependencia
 AND r.nivel = d.nivel
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mvw_inep_integrado_idx
ON transformacao.mvw_inep_integrado(ano, uf, dependencia, nivel);

-- IBGE integrado: analfabetismo + motivos (motivos de abrangência nacional cruzados por UF via analfabetismo)
CREATE MATERIALIZED VIEW transformacao.mvw_ibge_integrado AS
SELECT
    a.ano,
    upper(a.uf)                        AS uf,
    initcap(a.regiao)                  AS regiao,
    ROUND(COALESCE(a.taxa_analfabetismo, 0)::numeric, 2) AS taxa_analfabetismo,
    m.motivo,
    initcap(m.sexo)                    AS sexo,
    m.faixa_etaria,
    ROUND(COALESCE(m.percentual, 0)::numeric, 2) AS percentual_motivo
FROM staging.stg_ibge_analfabetismo a
LEFT JOIN staging.stg_ibge_motivos m
  ON a.ano = m.ano
WHERE m.sexo IN ('Masculino','Feminino')
  AND m.faixa_etaria IN ('15 a 17 anos','18 a 24 anos','25 a 29 anos')
WITH NO DATA;

CREATE INDEX IF NOT EXISTS mvw_ibge_integrado_idx
ON transformacao.mvw_ibge_integrado(ano, uf);


