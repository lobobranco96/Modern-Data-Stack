{{ config(
    materialized="table"
) }}

WITH DIM_REDACAO AS (
    SELECT
        * 
    FROM {{ ref('enem_data') }}
)

SELECT 
    ID AS ID_REDACAO,
    CASE
        WHEN TP_STATUS_REDACAO = '1' THEN 'Sem problemas'
        WHEN TP_STATUS_REDACAO = '2' THEN 'Anulada'
        WHEN TP_STATUS_REDACAO = '3' THEN 'Cópia Texto Motivador'
        WHEN TP_STATUS_REDACAO = '4' THEN 'Em Branco'
        WHEN TP_STATUS_REDACAO = '6' THEN 'Fuga ao tema'
        WHEN TP_STATUS_REDACAO = '7' THEN 'Não atendimento ao tipo textual'
        WHEN TP_STATUS_REDACAO = '8' THEN 'Texto insuficiente'
        WHEN TP_STATUS_REDACAO = '9' THEN 'Parte desconectada'
        ELSE TP_STATUS_REDACAO
    END AS TP_STATUS_REDACAO,
    NU_NOTA_COMP1,
    NU_NOTA_COMP2,
    NU_NOTA_COMP3,
    NU_NOTA_COMP4,
    NU_NOTA_COMP5,
    NU_NOTA_REDACAO
FROM DIM_REDACAO