{{ config(
    materialized="table"
) }}

with DIM_ESCOLA AS (
    SELECT
        *
    FROM {{ ref('enem_data') }}
)
SELECT
    ID AS ID_ESCOLA, 
    CO_MUNICIPIO_ESC,
    NO_MUNICIPIO_ESC,
    CO_UF_ESC,
    SG_UF_ESC,
    CASE
        WHEN TP_DEPENDENCIA_ADM_ESC = '1' THEN 'Federal'
        WHEN TP_DEPENDENCIA_ADM_ESC = '2' THEN 'Estadual'
        WHEN TP_DEPENDENCIA_ADM_ESC = '3' THEN 'Municipal'
        WHEN TP_DEPENDENCIA_ADM_ESC = '4' THEN 'Privada'
        ELSE TP_DEPENDENCIA_ADM_ESC
    END AS TP_DEPENDENCIA_ADM_ESC,
    CASE
        WHEN TP_LOCALIZACAO_ESC = '1' THEN 'Urbana'
        WHEN TP_LOCALIZACAO_ESC = '2' THEN 'Rural'
        ELSE TP_LOCALIZACAO_ESC
    END AS TP_LOCALIZACAO_ESC,
    CASE
        WHEN TP_SIT_FUNC_ESC = '1' THEN 'Em atividade'
        WHEN TP_SIT_FUNC_ESC = '2' THEN 'Paralisada'
        WHEN TP_SIT_FUNC_ESC = '3' THEN 'Extinta'
        ELSE TP_SIT_FUNC_ESC
    END AS TP_SIT_FUNC_ESC
FROM DIM_ESCOLA