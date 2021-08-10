WITH STG_CLIENTE AS ( SELECT *
                        FROM {{ ref('stg_cliente') }} )
SELECT * FROM STG_CLIENTE 
 WHERE ID_PESSOA IS NOT NULL