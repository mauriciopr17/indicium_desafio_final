WITH VENDEDOR AS (
					--**-- VENDEDOR
					SELECT ROW_NUMBER() OVER ( ORDER BY V.BUSINESSENTITYID ) SK_VENDEDOR
						  ,V.BUSINESSENTITYID        AS ID_VENDEDOR
						  ,V.TERRITORYID 		     AS ID_TERRITORIO
						  ,V.SALESQUOTA 	         AS COTA_VENDAS
						  ,V.BONUS    		         
						  ,V.COMMISSIONPCT           AS COMISSAO
						  ,V.SALESLASTYEAR 	         AS VENDAS_ULTIMO_ANO_VENDEDOR
						  ,E.NATIONALIDNUMBER 		 AS NUMERO_IDENTIDADE_VENDEDOR
						  ,E.JOBTITLE 			     AS CARGO
						  ,E.BIRTHDATE 	  	  	     AS DATA_NASCIMENTO
						  ,E.MARITALSTATUS           AS ESTADO_CIVIL
						  ,E.GENDER			   	     AS SEXO
						  ,E.HIREDATE 	             AS DATA_CONTRATACAO
						  ,E.SALARIEDFLAG 		     AS ASSALARIADO
						  ,E.VACATIONHOURS 		     AS HORAS_FERIAS
						  ,E.SICKLEAVEHOURS 	     AS HORAS_LICENCA_MEDICA
						  ,E.ORGANIZATIONNODE 	     AS UNIDADE_ORGANIZACIONAL
                      FROM {{ source('adventure_works', 'salesperson' )}} V 
                          ,{{ source('adventure_works', 'employee' )}}    E
					 WHERE E.BUSINESSENTITYID      = V.BUSINESSENTITYID )
,PESSOA AS (  SELECT *
			    FROM {{ ref('stg_pessoa' )}} P )                     
,VENDEDOR_DETALHES AS ( SELECT V.SK_VENDEDOR
                              ,P.SK_PESSOA
                              
                              ,V.ID_VENDEDOR
                              ,P.ID_PESSOA
                              ,V.ID_TERRITORIO

                              ,V.COTA_VENDAS
                              ,V.BONUS    		         
                              ,V.COMISSAO
                              ,V.VENDAS_ULTIMO_ANO_VENDEDOR

                              ,V.NUMERO_IDENTIDADE_VENDEDOR
                              ,V.CARGO
                              ,V.DATA_NASCIMENTO
                              ,V.ESTADO_CIVIL
                              ,V.SEXO
                              ,V.DATA_CONTRATACAO
                              ,V.ASSALARIADO
                              ,V.HORAS_FERIAS
                              ,V.HORAS_LICENCA_MEDICA
                              ,V.UNIDADE_ORGANIZACIONAL

                              ,P.TIPO_PESSOA
                              ,P.PRIMEIRO_NOME
                              ,P.SOBRENOME
                              ,P.ULTIMO_NOME
                              ,P.NOME_COMPLETO
                              ,P.EMAIL_PROMOCIONAL
                              ,P.DATA_MODIFICACAO DATA_MODIFICACAO_PESSOA
                        FROM VENDEDOR V
                   LEFT JOIN PESSOA   P ON P.ID_PESSOA = V.ID_VENDEDOR ) 
SELECT * FROM VENDEDOR_DETALHES VD
