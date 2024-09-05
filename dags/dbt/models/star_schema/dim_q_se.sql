{{ config(
    materialized="table"
) }}

WITH DIM_Q_SE AS (
    SELECT
        *
    FROM {{ ref('enem_data') }}
)

SELECT
    ID AS ID_Q_SE,
    Q001,
    Q002,
    Q003,
    Q004,
    Q005,
    Q006,
    Q007,
    Q008,
    Q009,
    Q010,
    Q011,
    Q012,
    Q013,
    Q014,
    Q015,
    Q016,
    Q017,
    Q018,
    Q019,
    Q020,
    Q021,
    Q022,
    Q023,
    Q024,
    Q025
FROM DIM_Q_SE
