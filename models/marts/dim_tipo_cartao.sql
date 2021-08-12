WITH STG_TIPO_CARTAO AS ( SELECT *
                        FROM {{ ref('stg_tipo_cartao') }} )
SELECT * FROM STG_TIPO_CARTAO