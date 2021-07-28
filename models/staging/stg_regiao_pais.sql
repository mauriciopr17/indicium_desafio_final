
WITH REGIAO_PAIS AS ( 
						--**-- PAÍS
						SELECT  ROW_NUMBER() OVER ( ORDER BY C.COUNTRYREGIONCODE ) SK_PAIS_REGIAO
							,C.COUNTRYREGIONCODE   AS PAIS_REGIAO_ESTADO
							,C.NAME                AS NOME_PAIS
						FROM {{ source('adventure_works', 'countryregion' )}} C )
SELECT * FROM REGIAO_PAIS
