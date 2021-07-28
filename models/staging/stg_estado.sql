WITH ESTADO AS ( 
				--**-- ESTADO
				  SELECT ROW_NUMBER() OVER ( ORDER BY E.STATEPROVINCEID ) SK_ESTADO
				  		,E.STATEPROVINCEID     AS ID_ESTADO
				  		,E.NAME  		 	   AS NOME_ESTADO
				  		,E.STATEPROVINCECODE   AS UF_ESTADO
				  		,E.COUNTRYREGIONCODE   AS PAIS_REGIAO_ESTADO
				  		,E.TERRITORYID         AS ID_TERRITORIO
                    FROM {{ source('adventure_works', 'stateprovince' )}} E )
SELECT * FROM ESTADO