WITH STG_ENDERECO AS ( SELECT *
                      FROM {{ ref('stg_endereco') }} )
SELECT * FROM STG_ENDERECO