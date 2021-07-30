WITH STG_VENDEDOR AS ( SELECT *
                        FROM {{ ref('stg_vendedor') }} )
SELECT * FROM STG_VENDEDOR