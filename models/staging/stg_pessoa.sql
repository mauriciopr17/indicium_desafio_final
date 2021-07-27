
WITH PESSOA AS (      
			     SELECT ROW_NUMBER( ) OVER ( ORDER BY P.BUSINESSENTITYID ) SK_PESSOA
			           ,P.BUSINESSENTITYID      AS ID_PESSOA
			     	   ,P.PERSONTYPE            AS TIPO_PESSOA
			     	   ,P.FIRSTNAME 		    AS PRIMEIRO_NOME
			     	   ,P.MIDDLENAME            AS SOBRENOME
			     	   ,P.LASTNAME 		        AS ULTIMO_NOME
			     	   ,P.FIRSTNAME || ' ' || P.MIDDLENAME  || ' ' || P.LASTNAME NOME_COMPLETO
			     	   ,P.EMAILPROMOTION        AS EMAIL_PROMOCIONAL
			     	   ,P.MODIFIEDDATE          AS DATA_MODIFICACAO
			       FROM {{ source('adventure_works', 'person' )}} p )
			       
SELECT * FROM PESSOA
			      