WITH STG_VENDEDOR AS ( SELECT *
                        FROM {{ ref('stg_vendedor') }} )
SELECT * FROM STG_VENDEDOR
 WHERE ID_PESSOA IS NOT NULL