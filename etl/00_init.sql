-- =============================================
-- 00_init.sql — schemas, controle e utilidades
-- =============================================

\set ON_ERROR_STOP on

-- Schemas
CREATE SCHEMA IF NOT EXISTS ctl;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS transformacao;
CREATE SCHEMA IF NOT EXISTS dw;

-- =============================
-- Tabelas de controle/auditoria
-- =============================
CREATE TABLE IF NOT EXISTS ctl.batch_exec (
    batch_id      BIGSERIAL PRIMARY KEY,
    start_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_ts        TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING|SUCCESS|FAILED
    message       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ctl.data_quality (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT REFERENCES ctl.batch_exec(batch_id) ON DELETE CASCADE,
    check_name    TEXT NOT NULL,
    severity      TEXT NOT NULL, -- INFO|WARN|ERROR
    passed        BOOLEAN NOT NULL,
    details       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================
-- Funções utilitárias de batch
-- =============================
CREATE OR REPLACE FUNCTION ctl.start_batch()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id BIGINT;
BEGIN
    INSERT INTO ctl.batch_exec(status)
    VALUES ('RUNNING')
    RETURNING batch_id INTO v_batch_id;
    RETURN v_batch_id;
END;
$$;

CREATE OR REPLACE FUNCTION ctl.end_batch(p_batch_id BIGINT, p_status TEXT, p_message TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE ctl.batch_exec
       SET end_ts = now(),
           status = COALESCE(p_status, 'SUCCESS'),
           message = p_message
     WHERE batch_id = p_batch_id;
END;
$$;

-- =============================
-- Listas de domínio auxiliares
-- =============================
CREATE OR REPLACE VIEW ctl.dom_uf AS
SELECT unnest(ARRAY[
  'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
]) AS uf;

CREATE OR REPLACE VIEW ctl.dom_nivel AS
SELECT unnest(ARRAY['Fundamental','Medio']) AS nivel;

CREATE OR REPLACE VIEW ctl.dom_sexo AS
SELECT unnest(ARRAY['Total','Masculino','Feminino']) AS sexo;


