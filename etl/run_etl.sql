-- ===============================================
-- run_etl.sql — Orquestração end-to-end do ETL
-- Executar com: psql -h localhost -U postgres -d evasao -f etl/run_etl.sql
-- ===============================================

\set ON_ERROR_STOP on

-- 0) Init e batch
\i etl/00_init.sql

-- Inicia batch e captura variável :batch_id
SELECT ctl.start_batch() AS batch_id \gset
\echo Iniciando batch :batch_id

-- 1) Staging (rebuild + COPY)
\i etl/10_staging.sql

-- 2) Transformação (MVs + refresh)
\i etl/20_transform.sql
REFRESH MATERIALIZED VIEW transformacao.mvw_inep_integrado;
REFRESH MATERIALIZED VIEW transformacao.mvw_ibge_integrado;

-- 3) DW Dimensões (DDL + carga idempotente)
\i etl/30_dw_dims.sql

-- 4) Fato (DDL + carga do batch)
\i etl/40_dw_fact.sql

-- 5) Data Quality (usar :batch_id)
\i etl/50_quality.sql

-- Finaliza batch
SELECT ctl.end_batch(:batch_id, 'SUCCESS', 'ETL concluido com sucesso');
\echo Batch :batch_id finalizado com sucesso


