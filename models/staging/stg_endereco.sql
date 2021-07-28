WITH ENDERECO AS ( 
					--**-- ENDEREÇO
					SELECT ROW_NUMBER() OVER ( ORDER BY E.ADDRESSID ) SK_ENDERECO
					      ,E.ADDRESSID       AS ID_ENDERECO
					      ,E.ADDRESSLINE1    AS DESCRICAO_ENDERECO
					      ,E.ADDRESSLINE2    AS COMPLEMENTO_ENDERECO
					      ,E.CITY 		       AS CIDADE
					      ,E.STATEPROVINCEID AS ID_ESTADO
					      ,E.POSTALCODE 	   AS CEP
					  FROM {{ source('adventure_works', 'address' )}} E )
SELECT * FROM ENDERECO