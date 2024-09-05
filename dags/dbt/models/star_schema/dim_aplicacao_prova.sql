{{ config(
    materialized="table"
) }}

with DIM_APLICACAO_PROVA as (
    SELECT
        *
    FROM {{ ref('enem_data') }}
    )
SELECT
    ID AS ID_APLICACAO_PROVA,
    CO_MUNICIPIO_PROVA,
    NO_MUNICIPIO_PROVA,
    CO_UF_PROVA,
    SG_UF_PROVA
FROM DIM_APLICACAO_PROVA