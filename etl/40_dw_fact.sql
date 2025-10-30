-- ==============================================================
-- 40_dw_fact.sql — DDL fato_evasao e carga vinculada ao :batch_id
-- ==============================================================

\set ON_ERROR_STOP on

-- DDL da fato
CREATE TABLE IF NOT EXISTS dw.fato_evasao (
    id_fato            BIGSERIAL PRIMARY KEY,
    id_tempo           INT NOT NULL REFERENCES dw.dim_tempo(id_tempo),
    id_localidade      INT NOT NULL REFERENCES dw.dim_localidade(id_localidade),
    id_dependencia     INT NOT NULL REFERENCES dw.dim_dependencia(id_dependencia),
    id_nivel           INT NOT NULL REFERENCES dw.dim_nivel(id_nivel),
    id_perfil          INT NOT NULL REFERENCES dw.dim_perfil_socio(id_perfil),
    taxa_aprovacao     DOUBLE PRECISION,
    taxa_reprovacao    DOUBLE PRECISION,
    taxa_abandono      DOUBLE PRECISION,
    taxa_distorcao     DOUBLE PRECISION,
    taxa_analfabetismo DOUBLE PRECISION,
    percentual_motivo  DOUBLE PRECISION,
    batch_id           BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_system      TEXT NOT NULL DEFAULT 'CSV'
);

-- Índices das FKs
CREATE INDEX IF NOT EXISTS idx_fato_tempo ON dw.fato_evasao(id_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_localidade ON dw.fato_evasao(id_localidade);
CREATE INDEX IF NOT EXISTS idx_fato_dependencia ON dw.fato_evasao(id_dependencia);
CREATE INDEX IF NOT EXISTS idx_fato_nivel ON dw.fato_evasao(id_nivel);
CREATE INDEX IF NOT EXISTS idx_fato_perfil ON dw.fato_evasao(id_perfil);
CREATE INDEX IF NOT EXISTS idx_fato_batch ON dw.fato_evasao(batch_id);

-- =============
-- Carga da Fato
-- =============

-- Remoção idempotente de registros do mesmo batch (se reexecutado)
DELETE FROM dw.fato_evasao WHERE batch_id = :batch_id;

INSERT INTO dw.fato_evasao (
    id_tempo, id_localidade, id_dependencia, id_nivel, id_perfil,
    taxa_aprovacao, taxa_reprovacao, taxa_abandono,
    taxa_distorcao, taxa_analfabetismo, percentual_motivo,
    batch_id
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
    b.percentual_motivo,
    :batch_id
FROM transformacao.mvw_inep_integrado i
JOIN (
    -- Último ano disponível do IBGE por UF, motivo, sexo e faixa
    SELECT ano, uf, regiao, taxa_analfabetismo, motivo, sexo, faixa_etaria, percentual_motivo
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY uf, motivo, sexo, faixa_etaria ORDER BY ano DESC) AS rn
        FROM transformacao.mvw_ibge_integrado
    ) s
    WHERE rn = 1
) b
  ON b.uf = i.uf
JOIN dw.dim_tempo t ON t.ano = i.ano
JOIN dw.dim_localidade l ON l.uf = i.uf
JOIN dw.dim_dependencia d ON d.tipo_dependencia = i.dependencia
JOIN dw.dim_nivel n ON n.nivel = i.nivel
JOIN dw.dim_perfil_socio p ON p.sexo = b.sexo AND p.faixa_etaria = b.faixa_etaria AND p.motivo_evasao = b.motivo;


