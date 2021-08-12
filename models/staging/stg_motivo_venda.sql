WITH MOTIVO_VENDA AS ( 
						  --**-- MOTIVO DE VENDA 
						 SELECT ROW_NUMBER() OVER ( ORDER BY S.SALESREASONID ) SK_MOTIVO_VENDA 
						 	   ,S.SALESREASONID       AS ID_MOTIVO_VENDA
						 	   ,CASE 
								  WHEN ( S.NAME	IS NOT NULL ) THEN
								    S.NAME
								  ELSE
								    'Other'
								  END DESCRICAO_MOTIVO_VENDA
						 	   ,S.REASONTYPE 		  AS TIPO_VENDA 
						 	   ,S.MODIFIEDDATE        AS DATA_MODIFICACAO_MOTIVO_VENDA
                           FROM {{ source('adventure_works', 'salesreason' )}} S ) 
SELECT * FROM MOTIVO_VENDA
WHERE SK_MOTIVO_VENDA IS NOT NULL
