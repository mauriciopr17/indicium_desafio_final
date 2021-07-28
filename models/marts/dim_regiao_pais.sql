WITH STG_REGIAO_PAIS AS ( SELECT *
                            FROM {{ ref('stg_regiao_pais') }} )
SELECT * FROM STG_REGIAO_PAIS