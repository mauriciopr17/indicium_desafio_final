WITH CLIENTE AS ( 
				  -- CLIENTE
				  SELECT ROW_NUMBER() OVER ( ORDER BY C.CUSTOMERID ) SK_CLIENTE
				  	    ,C.CUSTOMERID            AS ID_CLIENTE
				  	    ,C.PERSONID 			 AS ID_PESSOA
				  	    ,C.TERRITORYID 	         AS ID_TERRORIO
				    FROM {{ source('adventure_works', 'customer' )}} C ) 
,PESSOA AS (      
			     SELECT *
			       FROM {{ ref('stg_pessoa' )}} P )
,CLIENTE_DETALHES AS ( SELECT C.SK_CLIENTE
                             ,P.SK_PESSOA

                             ,C.ID_CLIENTE
                             ,P.ID_PESSOA
                             ,C.ID_TERRORIO
                             
                             
                             ,P.TIPO_PESSOA
                             ,P.PRIMEIRO_NOME
                             ,P.SOBRENOME
                             ,P.ULTIMO_NOME
                             ,P.NOME_COMPLETO
                             ,P.EMAIL_PROMOCIONAL
                             ,P.DATA_MODIFICACAO DATA_MODIFICACAO_PESSOA
                        FROM CLIENTE C
                   LEFT JOIN PESSOA  P ON P.ID_PESSOA = C.ID_PESSOA ) 
SELECT * FROM CLIENTE_DETALHES