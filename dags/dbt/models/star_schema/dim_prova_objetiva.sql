{{ config(
    materialized="table"
) }}

WITH DIM_PROVA_OBJETIVA AS (
    SELECT
        *
    FROM {{ ref('enem_data') }}
)
SELECT 
    ID AS ID_PROVA_OBJETIVA,
    CASE
        WHEN TP_PRESENCA_CN = '0' THEN 'Faltou à prova'
        WHEN TP_PRESENCA_CN = '1' THEN 'Presente na prova'
        WHEN TP_PRESENCA_CN = '2' THEN 'Eliminado na prova'
        ELSE TP_PRESENCA_CN
    END AS TP_PRESENCA_CN,
    CASE
        WHEN TP_PRESENCA_CH = '0' THEN 'Faltou à prova'
        WHEN TP_PRESENCA_CH = '1' THEN 'Presente na prova'
        WHEN TP_PRESENCA_CH = '2' THEN 'Eliminado na prova'
        ELSE TP_PRESENCA_CH
    END AS TP_PRESENCA_CH,
    CASE
        WHEN TP_PRESENCA_LC = '0' THEN 'Faltou à prova'
        WHEN TP_PRESENCA_LC = '1' THEN 'Presente na prova'
        WHEN TP_PRESENCA_LC = '2' THEN 'Eliminado na prova'
        ELSE TP_PRESENCA_LC
    END AS TP_PRESENCA_LC,
    CASE
        WHEN TP_PRESENCA_MT = '0' THEN 'Faltou à prova'
        WHEN TP_PRESENCA_MT = '1' THEN 'Presente na prova'
        WHEN TP_PRESENCA_MT = '2' THEN 'Eliminado na prova'
        ELSE TP_PRESENCA_MT
    END AS TP_PRESENCA_MT,
    CASE
        WHEN CO_PROVA_CN = '597' THEN 'Azul'
        WHEN CO_PROVA_CN = '598' THEN 'Amarela'
        WHEN CO_PROVA_CN = '599' THEN 'Cinza'
        WHEN CO_PROVA_CN = '600' THEN 'Rosa'
        WHEN CO_PROVA_CN = '601' THEN 'Rosa - Ampliada'
        WHEN CO_PROVA_CN = '602' THEN 'Rosa - Superampliada'
        WHEN CO_PROVA_CN = '604' THEN 'Laranja - Adaptada Ledor'
        WHEN CO_PROVA_CN = '605' THEN 'Verde - Videoprova - Libras'
        WHEN CO_PROVA_CN = '677' THEN 'Azul (Reaplicação)'
        WHEN CO_PROVA_CN = '678' THEN 'Amarela (Reaplicação)'
        WHEN CO_PROVA_CN = '679' THEN 'Cinza (Reaplicação)'
        WHEN CO_PROVA_CN = '680' THEN 'Rosa (Reaplicação)'
        WHEN CO_PROVA_CN = '684' THEN 'Laranja - Adaptada Ledor (Reaplicação)'
        WHEN CO_PROVA_CN = '699' THEN 'Azul (Digital)'
        WHEN CO_PROVA_CN = '700' THEN 'Amarela (Digital)'
        WHEN CO_PROVA_CN = '701' THEN 'Rosa (Digital)'
        WHEN CO_PROVA_CN = '702' THEN 'Cinza (Digital)'
        ELSE 'Não encontrado'
    END AS CO_PROVA_CN,
    CASE
        WHEN CO_PROVA_CH = '567' THEN 'Azul'
        WHEN CO_PROVA_CH = '568' THEN 'Amarela'
        WHEN CO_PROVA_CH = '569' THEN 'Cinza'
        WHEN CO_PROVA_CH = '570' THEN 'Rosa'
        WHEN CO_PROVA_CH = '571' THEN 'Rosa - Ampliada'
        WHEN CO_PROVA_CH = '572' THEN 'Rosa - Superampliada'
        WHEN CO_PROVA_CH = '574' THEN 'Laranja - Adaptada Ledor'
        WHEN CO_PROVA_CH = '575' THEN 'Verde - Videoprova - Libras'
        WHEN CO_PROVA_CH = '647' THEN 'Azul (Reaplicação)'
        WHEN CO_PROVA_CH = '648' THEN 'Amarela (Reaplicação)'
        WHEN CO_PROVA_CH = '649' THEN 'Cinza (Reaplicação)'
        WHEN CO_PROVA_CH = '650' THEN 'Rosa (Reaplicação)'
        WHEN CO_PROVA_CH = '654' THEN 'Laranja - Adaptada Ledor (Reaplicação)'
        WHEN CO_PROVA_CH = '687' THEN 'Azul (Digital)'
        WHEN CO_PROVA_CH = '688' THEN 'Amarela (Digital)'
        WHEN CO_PROVA_CH = '689' THEN 'Rosa (Digital)'
        WHEN CO_PROVA_CH = '690' THEN 'Cinza (Digital)'
        ELSE 'Não encontrado'
    END AS CO_PROVA_CH,
    CASE
        WHEN CO_PROVA_LC = '577' THEN 'Azul'
        WHEN CO_PROVA_LC = '578' THEN 'Amarela'
        WHEN CO_PROVA_LC = '579' THEN 'Rosa'
        WHEN CO_PROVA_LC = '580' THEN 'Branca'
        WHEN CO_PROVA_LC = '581' THEN 'Rosa - Ampliada'
        WHEN CO_PROVA_LC = '582' THEN 'Rosa - Superampliada'
        WHEN CO_PROVA_LC = '584' THEN 'Laranja - Adaptada Ledor'
        WHEN CO_PROVA_LC = '585' THEN 'Verde - Videoprova - Libras'
        WHEN CO_PROVA_LC = '657' THEN 'Azul (Reaplicação)'
        WHEN CO_PROVA_LC = '658' THEN 'Amarela (Reaplicação)'
        WHEN CO_PROVA_LC = '659' THEN 'Rosa (Reaplicação)'
        WHEN CO_PROVA_LC = '660' THEN 'Branca (Reaplicação)'
        WHEN CO_PROVA_LC = '664' THEN 'Laranja - Adaptada Ledor (Reaplicação)'
        WHEN CO_PROVA_LC = '691' THEN 'Azul (Digital)'
        WHEN CO_PROVA_LC = '692' THEN 'Amarela (Digital)'
        WHEN CO_PROVA_LC = '693' THEN 'Branca (Digital)'
        WHEN CO_PROVA_LC = '694' THEN 'Rosa (Digital)'
        ELSE 'Não encontrado'
    END AS CO_PROVA_LC,
    CASE
        WHEN CO_PROVA_MT = '587' THEN 'Azul'
        WHEN CO_PROVA_MT = '588' THEN 'Amarela'
        WHEN CO_PROVA_MT = '589' THEN 'Rosa'
        WHEN CO_PROVA_MT = '590' THEN 'Cinza'
        WHEN CO_PROVA_MT = '591' THEN 'Rosa - Ampliada'
        WHEN CO_PROVA_MT = '592' THEN 'Rosa - Superampliada'
        WHEN CO_PROVA_MT = '594' THEN 'Laranja - Adaptada Ledor'
        WHEN CO_PROVA_MT = '595' THEN 'Verde - Videoprova - Libras'
        WHEN CO_PROVA_MT = '667' THEN 'Azul (Reaplicação)'
        WHEN CO_PROVA_MT = '668' THEN 'Amarela (Reaplicação)'
        WHEN CO_PROVA_MT = '669' THEN 'Rosa (Reaplicação)'
        WHEN CO_PROVA_MT = '670' THEN 'Cinza (Reaplicação)'
        WHEN CO_PROVA_MT = '674' THEN 'Laranja - Adaptada Ledor (Reaplicação)'
        WHEN CO_PROVA_MT = '695' THEN 'Azul (Digital)'
        WHEN CO_PROVA_MT = '696' THEN 'Amarela (Digital)'
        WHEN CO_PROVA_MT = '697' THEN 'Rosa (Digital)'
        WHEN CO_PROVA_MT = '698' THEN 'Cinza (Digital)'
        ELSE 'Não encontrado'
    END AS CO_PROVA_MT,
    NU_NOTA_CN,
    NU_NOTA_CH,
    NU_NOTA_LC,
    NU_NOTA_MT,
    TX_RESPOSTAS_CN,
    TX_RESPOSTAS_CH,
    TX_RESPOSTAS_LC,
    TX_RESPOSTAS_MT,
    CASE
        WHEN TP_LINGUA = '0' THEN 'Inglês'
        WHEN TP_LINGUA = '1' THEN 'Espanhol'
        ELSE 'Não encontrado'
    END AS TP_LINGUA,
    TX_GABARITO_CN,
    TX_GABARITO_CH,
    TX_GABARITO_LC,
    TX_GABARITO_MT
FROM DIM_PROVA_OBJETIVA