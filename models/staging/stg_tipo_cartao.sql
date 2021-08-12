WITH TIPO_CARTAO AS (
					 --**-- tipo de cartão
						SELECT ROW_NUMBER() OVER ( ORDER BY C.CREDITCARDID ) SK_ID_TIPO_CARTAO
							  ,C.CREDITCARDID   AS ID_TIPO_CARTAO
							  ,C.CARDTYPE       AS TIPO_CARTAO
							  ,C.CARDNUMBER     AS NUMERO_CARTAO
							  ,C.EXPMONTH       AS MES_EXPIRACAO_CARTAO
							  ,C.EXPYEAR 	    AS ANO_EXPIRACAO_CARTAO
                          FROM {{ source('adventure_works', 'creditcard' )}} C ) 
SELECT * FROM TIPO_CARTAO