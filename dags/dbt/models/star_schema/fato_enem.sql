{{ config(
    materialized="table"
) }}

WITH DIM_PARTICIPANTE AS (
    SELECT
        *
    FROM {{ ref('dim_participante') }}
),
DIM_ESCOLA AS (
    SELECT 
        *
    FROM {{ ref('dim_escola') }}
),
DIM_REDACAO AS (
    SELECT
        *
    FROM {{ ref('dim_redacao') }}
),
DIM_Q_SE AS (
    SELECT
        *
    FROM {{ ref('dim_q_se') }}
),
DIM_PROVA_OBJETIVA AS (
    SELECT
        *
    FROM {{ ref('dim_prova_objetiva') }}
),
DIM_APLICACAO_PROVA AS (
    SELECT
        *
    FROM {{ ref('dim_aplicacao_prova') }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY de.ID_ESCOLA) AS ID_FATO,
    de.ID_ESCOLA,
    dp.ID_PARTICIPANTE,
    dr.ID_REDACAO,
    dqs.ID_Q_SE,
    dpo.ID_PROVA_OBJETIVA,
    dap.ID_APLICACAO_PROVA,
    dp.NU_INSCRICAO
FROM DIM_PARTICIPANTE dp
JOIN DIM_ESCOLA de ON dp.ID_PARTICIPANTE = de.ID_ESCOLA
JOIN DIM_REDACAO dr ON dp.ID_PARTICIPANTE = dr.ID_REDACAO
JOIN DIM_Q_SE dqs ON dp.ID_PARTICIPANTE = dqs.ID_Q_SE
JOIN DIM_PROVA_OBJETIVA dpo ON dp.ID_PARTICIPANTE = dpo.ID_PROVA_OBJETIVA
JOIN DIM_APLICACAO_PROVA dap ON dp.ID_PARTICIPANTE = dap.ID_APLICACAO_PROVA