WITH STG_PESSOA AS ( SELECT *
                         FROM {{ ref('stg_pessoa') }} )
SELECT * FROM STG_PESSOA