WITH STG_PRODUTO AS ( SELECT *
                       FROM {{ ref('stg_produto') }} )
SELECT * FROM STG_PRODUTO