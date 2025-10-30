-- ======================================================
-- 50_quality.sql — Data Quality checks para o :batch_id
-- ======================================================

\set ON_ERROR_STOP on

-- Helper para registrar resultado
CREATE OR REPLACE FUNCTION ctl.add_check(
    p_batch_id BIGINT,
    p_name TEXT,
    p_severity TEXT,
    p_passed BOOLEAN,
    p_details TEXT
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO ctl.data_quality(batch_id, check_name, severity, passed, details)
    VALUES (p_batch_id, p_name, p_severity, p_passed, p_details);
END;$$;

-- Completude em staging
SELECT ctl.add_check(:batch_id, 'staging.rendimento.uf_not_null', 'ERROR',
       (SELECT COUNT(*) FROM staging.stg_inep_rendimento WHERE uf IS NULL) = 0,
       (SELECT 'nulos='||COUNT(*) FROM staging.stg_inep_rendimento WHERE uf IS NULL));

SELECT ctl.add_check(:batch_id, 'staging.distorcao.uf_not_null', 'ERROR',
       (SELECT COUNT(*) FROM staging.stg_inep_distorcao WHERE uf IS NULL) = 0,
       (SELECT 'nulos='||COUNT(*) FROM staging.stg_inep_distorcao WHERE uf IS NULL));

-- Domínio de UF nas MVs
SELECT ctl.add_check(:batch_id, 'transform.uf_domain_valid', 'ERROR',
       (SELECT COUNT(*) FROM (
           SELECT uf FROM transformacao.mvw_inep_integrado
           UNION ALL
           SELECT uf FROM transformacao.mvw_ibge_integrado
       ) s LEFT JOIN ctl.dom_uf d ON d.uf = s.uf WHERE d.uf IS NULL) = 0,
       (SELECT 'fora_dominio='||COUNT(*) FROM (
           SELECT uf FROM transformacao.mvw_inep_integrado
           UNION ALL
           SELECT uf FROM transformacao.mvw_ibge_integrado
       ) s LEFT JOIN ctl.dom_uf d ON d.uf = s.uf WHERE d.uf IS NULL));

-- Chaves órfãs na fato (após carga)
SELECT ctl.add_check(:batch_id, 'fact.orphans_after_load', 'ERROR',
       (
        SELECT COUNT(*)
        FROM dw.fato_evasao f
        LEFT JOIN dw.dim_tempo t ON t.id_tempo = f.id_tempo
        LEFT JOIN dw.dim_localidade l ON l.id_localidade = f.id_localidade
        LEFT JOIN dw.dim_dependencia d ON d.id_dependencia = f.id_dependencia
        LEFT JOIN dw.dim_nivel n ON n.id_nivel = f.id_nivel
        LEFT JOIN dw.dim_perfil_socio p ON p.id_perfil = f.id_perfil
        WHERE f.batch_id = :batch_id
          AND (t.id_tempo IS NULL OR l.id_localidade IS NULL OR d.id_dependencia IS NULL OR n.id_nivel IS NULL OR p.id_perfil IS NULL)
       ) = 0,
       (
        SELECT 'orfas='||COUNT(*)
        FROM dw.fato_evasao f
        LEFT JOIN dw.dim_tempo t ON t.id_tempo = f.id_tempo
        LEFT JOIN dw.dim_localidade l ON l.id_localidade = f.id_localidade
        LEFT JOIN dw.dim_dependencia d ON d.id_dependencia = f.id_dependencia
        LEFT JOIN dw.dim_nivel n ON n.id_nivel = f.id_nivel
        LEFT JOIN dw.dim_perfil_socio p ON p.id_perfil = f.id_perfil
        WHERE f.batch_id = :batch_id
          AND (t.id_tempo IS NULL OR l.id_localidade IS NULL OR d.id_dependencia IS NULL OR n.id_nivel IS NULL OR p.id_perfil IS NULL)
       )
);

-- Resumo como função para evitar variáveis do psql dentro de DO
CREATE OR REPLACE FUNCTION ctl.print_dq_summary(p_batch_id BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_total INT;
    v_errors INT;
    v_warns INT;
BEGIN
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE severity='ERROR' AND NOT passed),
           COUNT(*) FILTER (WHERE severity='WARN'  AND NOT passed)
      INTO v_total, v_errors, v_warns
      FROM ctl.data_quality WHERE batch_id = p_batch_id;

    RAISE NOTICE 'DQ batch %: total=% errors=% warns=%', p_batch_id, v_total, v_errors, v_warns;
END;$$;

SELECT ctl.print_dq_summary(:batch_id);


