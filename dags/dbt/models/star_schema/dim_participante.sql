{{ config(
    materialized="table"
) }}

WITH DIM_PARTICIPANTE AS (
    SELECT
    *
    FROM {{ ref('enem_data') }}
)
SELECT 
    ID AS ID_PARTICIPANTE,
    NU_INSCRICAO,
    CASE
        WHEN TP_FAIXA_ETARIA = '1' THEN 'Menor de 17 anos'
        WHEN TP_FAIXA_ETARIA = '2' THEN '17 anos'
        WHEN TP_FAIXA_ETARIA = '3' THEN '18 anos'
        WHEN TP_FAIXA_ETARIA = '4' THEN '19 anos'
        WHEN TP_FAIXA_ETARIA = '5' THEN '20 anos'
        WHEN TP_FAIXA_ETARIA = '6' THEN '21 anos'
        WHEN TP_FAIXA_ETARIA = '7' THEN '22 anos'
        WHEN TP_FAIXA_ETARIA = '8' THEN '23 anos'
        WHEN TP_FAIXA_ETARIA = '9' THEN '24 anos'
        WHEN TP_FAIXA_ETARIA = '10' THEN '25 anos'
        WHEN TP_FAIXA_ETARIA = '11' THEN 'Entre 26 a 30 anos'
        WHEN TP_FAIXA_ETARIA = '12' THEN 'Entre 31 a 35 anos'
        WHEN TP_FAIXA_ETARIA = '13' THEN 'Entre 36 a 40 anos'
        WHEN TP_FAIXA_ETARIA = '14' THEN 'Entre 41 a 45 anos'
        WHEN TP_FAIXA_ETARIA = '15' THEN 'Entre 46 a 50 anos'
        WHEN TP_FAIXA_ETARIA = '16' THEN 'Entre 51 a 55 anos'
        WHEN TP_FAIXA_ETARIA = '17' THEN 'Entre 56 a 60 anos'
        WHEN TP_FAIXA_ETARIA = '18' THEN 'Entre 61 a 65 anos'
        WHEN TP_FAIXA_ETARIA = '19' THEN 'Entre 66 a 70 anos'
        WHEN TP_FAIXA_ETARIA = '20' THEN 'Maior de 70 anos'
        ELSE TP_FAIXA_ETARIA
    END AS TP_FAIXA_ETARIA,
    CASE
        WHEN TP_SEXO = 'F' THEN 'Feminino'
        WHEN TP_SEXO = 'M' THEN 'Masculino'
        ELSE 'Não encontrado'
    END AS TP_SEXO,
    CASE
        WHEN TP_ESTADO_CIVIL = '0' THEN 'Não informado'
        WHEN TP_ESTADO_CIVIL = '1' THEN 'Solteiro(a)'
        WHEN TP_ESTADO_CIVIL = '2' THEN 'Casado(a) Mora com companheiro(a)'
        WHEN TP_ESTADO_CIVIL = '3' THEN 'Divorciado(a)/Desquitado(a)/Separado(a)'
        WHEN TP_ESTADO_CIVIL = '4' THEN 'Viúvo(a)'
        ELSE TP_ESTADO_CIVIL
    END AS TP_ESTADO_CIVIL,
    CASE
        WHEN TP_COR_RACA = '0' THEN 'Não declarado'
        WHEN TP_COR_RACA = '1' THEN 'Branca'
        WHEN TP_COR_RACA = '2' THEN 'Preta'
        WHEN TP_COR_RACA = '3' THEN 'Parda'
        WHEN TP_COR_RACA = '4' THEN 'Amarela'
        WHEN TP_COR_RACA = '5' THEN 'Indígena'
        ELSE TP_COR_RACA
    END AS TP_COR_RACA,
    CASE
        WHEN TP_NACIONALIDADE = '0' THEN 'Não informado'
        WHEN TP_NACIONALIDADE = '1' THEN 'Brasileiro(a)'
        WHEN TP_NACIONALIDADE = '2' THEN 'Brasileiro(a) Naturalizado(a)'
        WHEN TP_NACIONALIDADE = '3' THEN 'Estrangeiro(a)'
        WHEN TP_NACIONALIDADE = '4' THEN 'Brasileiro(a) Nato(a), nascido(a) no exterior'
        ELSE TP_NACIONALIDADE
    END AS TP_NACIONALIDADE,
    CASE
        WHEN TP_ST_CONCLUSAO = '1' THEN 'Já concluí o Ensino Médio'
        WHEN TP_ST_CONCLUSAO = '2' THEN 'Estou cursando e concluirei o Ensino Médio em 2020'
        WHEN TP_ST_CONCLUSAO = '3' THEN 'Estou cursando e concluirei o Ensino Médio após 2020'
        WHEN TP_ST_CONCLUSAO = '4' THEN 'Não concluí e não estou cursando o Ensino Médio'
        ELSE TP_ST_CONCLUSAO
    END AS TP_ST_CONCLUSAO,
    CASE
        WHEN TP_ANO_CONCLUIU = '0' THEN 'Não informado'
        WHEN TP_ANO_CONCLUIU = '1' THEN '2019'
        WHEN TP_ANO_CONCLUIU = '2' THEN '2018'
        WHEN TP_ANO_CONCLUIU = '3' THEN '2017'
        WHEN TP_ANO_CONCLUIU = '4' THEN '2016'
        WHEN TP_ANO_CONCLUIU = '5' THEN '2015'
        WHEN TP_ANO_CONCLUIU = '6' THEN '2014'
        WHEN TP_ANO_CONCLUIU = '7' THEN '2013'
        WHEN TP_ANO_CONCLUIU = '8' THEN '2012'
        WHEN TP_ANO_CONCLUIU = '9' THEN '2011'
        WHEN TP_ANO_CONCLUIU = '10' THEN '2010'
        WHEN TP_ANO_CONCLUIU = '11' THEN '2009'
        WHEN TP_ANO_CONCLUIU = '12' THEN '2008'
        WHEN TP_ANO_CONCLUIU = '13' THEN '2007'
        WHEN TP_ANO_CONCLUIU = '14' THEN 'Antes de 2007'
        ELSE TP_ANO_CONCLUIU
    END AS TP_ANO_CONCLUIU,
    CASE
        WHEN TP_ESCOLA = '1' THEN 'Não Respondeu'
        WHEN TP_ESCOLA = '2' THEN 'Pública'
        WHEN TP_ESCOLA = '3' THEN 'Privada'
        WHEN TP_ESCOLA = '4' THEN 'Exterior'
        ELSE TP_ESCOLA
    END AS TP_ESCOLA,
    CASE
        WHEN TP_ENSINO = '1' THEN 'Ensino Regular'
        WHEN TP_ENSINO = '2' THEN 'Educação Especial - Modalidade Substitutiva'
        WHEN TP_ENSINO = '3' THEN 'Educação de Jovens e Adultos'
        ELSE TP_ENSINO
    END AS TP_ENSINO,
    CASE
        WHEN IN_TREINEIRO = '0' THEN 'Sim'
        WHEN IN_TREINEIRO = '1' THEN 'Não'
        ELSE IN_TREINEIRO
    END AS IN_TREINEIRO
FROM DIM_PARTICIPANTE