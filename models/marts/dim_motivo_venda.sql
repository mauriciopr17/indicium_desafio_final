WITH STG_MOTIVO_VENDA AS ( SELECT *
                         FROM {{ ref('stg_motivo_venda') }} )
SELECT * FROM STG_MOTIVO_VENDA