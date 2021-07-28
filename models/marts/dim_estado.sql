WITH STG_ESTADO AS ( SELECT *
                         FROM {{ ref('stg_estado') }} )
SELECT * FROM STG_ESTADO