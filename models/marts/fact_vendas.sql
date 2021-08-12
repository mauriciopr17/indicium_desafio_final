WITH PEDIDO            AS ( SELECT *
                              FROM {{ ref('stg_pedido_venda') }} )
,PEDIDO_VENDA_DETALHES AS ( SELECT *
                              FROM {{ ref('stg_pedido_venda_detalhes') }} )
,MOTIVO_VENDA_PEDIDO   AS ( SELECT *
                              FROM {{ ref('stg_motivo_venda_pedido') }} )
,MOTIVO_VENDA          AS ( SELECT *
                              FROM {{ ref('dim_motivo_venda') }} )
,PRODUTO               AS ( SELECT *
                              FROM {{ ref('dim_produto') }} )
,CLIENTE               AS ( SELECT *
                              FROM {{ ref('dim_cliente') }} )
,VENDEDOR              AS ( SELECT *
                              FROM {{ ref('dim_vendedor') }} )
,ENDERECO              AS ( SELECT *
                              FROM {{ ref('dim_endereco') }} )
,ESTADO                AS ( SELECT *
                              FROM {{ ref('dim_estado') }} )
,REGIAO_PAIS           AS ( SELECT *
                              FROM {{ ref('dim_regiao_pais') }} )
,TIPO_CARTAO           AS ( SELECT *
                              FROM {{ ref('dim_tipo_cartao') }} )


SELECT   P.SK_PEDIDO_VENDA    -- CHAVE SURROGATE
        ,MV.SK_MOTIVO_VENDA   -- CHAVE SURROGATE
        ,PDT.SK_PRODUTO       -- CHAVE SURROGATE
        ,C.SK_CLIENTE         -- CHAVE SURROGATE
        ,V.SK_VENDEDOR        -- CHAVE SURROGATE
        ,E.SK_ENDERECO        -- CHAVE SURROGATE
        ,ES.SK_ESTADO         -- CHAVE SURROGATE
        ,RP.SK_PAIS_REGIAO    -- CHAVE SURROGATE
        ,TC.SK_ID_TIPO_CARTAO -- CHAVE SURROGATE
        
        ,C.ID_CLIENTE
        ,C.ID_TERRORIO    AS ID_TERRITORIO_CLIENTE
        ,V.ID_VENDEDOR    
        ,V.ID_TERRITORIO  AS ID_TERRITORIO_VENDEDOR
        ,P.ID_PEDIDO_VENDA
        ,MV.ID_MOTIVO_VENDA
        ,P.ID_TERRITORIO
        ,P.ID_ENDERECO_ENTREGA
        ,P.ID_METODO_ENTREGA
        ,P.ID_TAXA_CAMBIO
        ,TC.ID_TIPO_CARTAO

        
        ,PD.ID_PEDIDO_VENDA_DETALHES
        ,PDT.ID_PRODUTO
        ,E.ID_ENDERECO
        ,E.ID_ESTADO
        ,RP.PAIS_REGIAO_ESTADO               
        
         -- PEDIDO
        ,P.NUMERO_REVISAO_PEDIDO
        ,P.DATA_PEDIDO
        ,P.DATA_VENCIMENTO_PEDIDO
        ,P.DATA_ENVIO_PEDIDO
        ,P.STATUS
        ,P.VENDA_ONLINE
        ,P.NUMERO_PEDIDO_COMPRA
        ,P.NUMERO_CONTA
        ,P.ENDERECO_CONTA
        ,P.CODIGO_APROVACAO_CARTAO
        ,P.SUBTOTAL 	   	         
        ,P.TAXA
        ,P.FRETE
        ,P.TOTAL_DEVIDO 
        
        -- PEDIDO DETALHE
        ,PD.NUMERO_RASTREAMENTO_OPERADORA
        ,PD.QUANTIDADE_POR_PRODUTO
        ,PD.OFERTA_ESPECIAL
        ,PD.PRECO_UNITARIO
        ,PD.PRECO_UNITARIO_DESCONTO
  /*      -- MOTIVO VENDA
        ,MV.DESCRICAO_MOTIVO_VENDA
        ,MV.TIPO_VENDA 
        ,MV.DATA_MODIFICACAO_MOTIVO_VENDA

        -- PRODUTO
        ,PDT.NOME_PRODUTO
        ,PDT.NUMERO_PRODUTO
        ,PDT.ENTREGA_DISPONIVEL
        ,PDT.ENTREGA_FINALZIADA
        ,PDT.COR_PRODUTO
        ,PDT.NIVEL_ESTOQUE_SEGURANCA
        ,PDT.PONTO_REORDENAGEM
        ,PDT.PRECO_LISTA
        ,PDT.TAMANHO_PRODUTO
        ,PDT.TAMANHO_UNIDADE_MEDIDA
        ,PDT.PESO_UNIDADE_MEDIDA
        ,PDT.PESO
        ,PDT.DIAS_FABRICACAO
        ,PDT.LINHA_PRODUCAO
        ,PDT.CLASSE
        ,PDT.ESTILO_PRODUTO
        ,PDT.CATEGORIA_PRODUTO
        ,PDT.MODELO_PRODUTO
        ,PDT.DATA_INICIO_VENDA
        ,PDT.DATA_FIM_VENDA
        ,PDT.DATA_MODIFICACAO DATA_MODIFICACAO_PRODUTO

        -- CLIENTE
        ,C.TIPO_PESSOA 
        ,C.NOME_COMPLETO           AS NOME_COMPLETO_CLIENTE
        ,C.EMAIL_PROMOCIONAL       AS EMAIL_PROMOCIONAL_CLIENTE
        ,C.DATA_MODIFICACAO_PESSOA AS DATA_MODIFICACAO_PESSOA_CLIENTE

        -- VENDEDOR
       ,V.COTA_VENDAS                 AS COTAS_VENDAS_VENDEDOR
       ,V.BONUS    		      AS BONUS_VENDEDOR   
       ,V.COMISSAO                    AS COMISSAO_VENDEDOR
       ,V.VENDAS_ULTIMO_ANO_VENDEDOR
       ,V.NUMERO_IDENTIDADE_VENDEDOR
       ,V.CARGO                       AS CARGO_VENDEDOR
       ,V.DATA_NASCIMENTO             AS DATA_NASCIMENTO_VENDEDOR
       ,V.ESTADO_CIVIL                AS ESTADO_CIVIL_VENDEDOR          
       ,V.SEXO                        AS SEXO_VENDEDOR
       ,V.DATA_CONTRATACAO            AS DATA_CONTRATACAO_VENDEDOR
       ,V.ASSALARIADO                 AS ASSALARIADO_VENDEDOR
       ,V.HORAS_FERIAS                AS HORAS_FERIAS_VENDEDOR
       ,V.HORAS_LICENCA_MEDICA        AS HORAS_LICENCA_MEDICA_VENDEDOR
       ,V.UNIDADE_ORGANIZACIONAL      AS UNIDADE_ORGANIZACIONAL_VENDEDOR

       ,V.TIPO_PESSOA                 AS TIPO_PESSOA_VENDEDOR
       ,V.PRIMEIRO_NOME               AS PRIMEIRO_NOME_VENEDOR
       ,V.SOBRENOME                   AS SOBRENOME_VENDEDOR
       ,V.ULTIMO_NOME                 AS ULTIMO_NOME_VENDEDOR
       ,V.NOME_COMPLETO               AS NOME_COMPLETO_VENDEDOR
       ,V.EMAIL_PROMOCIONAL           AS EMAIL_PROMOCIONAL_VENDEDOR
       ,V.DATA_MODIFICACAO_PESSOA     AS DATA_MODIFICACAO_PESSOA_VENDEDOR 

       -- ENDEREÇO
       ,E.DESCRICAO_ENDERECO
       ,E.COMPLEMENTO_ENDERECO
       ,E.CIDADE
       ,E.CEP

      -- ESTADO
      ,ES.NOME_ESTADO
      ,ES.UF_ESTADO
      
      -- PAÍS REGIÃO ESTADO
			,RP.NOME_PAIS     
*/

  FROM PEDIDO                      P 
  LEFT JOIN PEDIDO_VENDA_DETALHES  PD  ON PD.ID_PEDIDO_VENDA    = P.ID_PEDIDO_VENDA
  LEFT JOIN MOTIVO_VENDA_PEDIDO    MVP ON MVP.ID_PEDIDO_VENDA   = P.ID_PEDIDO_VENDA
  LEFT JOIN MOTIVO_VENDA           MV  ON MV.ID_MOTIVO_VENDA    = MVP.ID_MOTIVO_VENDA
  LEFT JOIN PRODUTO                PDT ON PDT.ID_PRODUTO        = PD.ID_PRODUTO
  LEFT JOIN CLIENTE                C   ON C.ID_CLIENTE          = P.ID_CLIENTE
  LEFT JOIN VENDEDOR               V   ON V.ID_VENDEDOR         = P.ID_VENDEDOR
  LEFT JOIN ENDERECO               E   ON E.ID_ENDERECO         = P.ID_ENDERECO_ENTREGA
  LEFT JOIN ESTADO                 ES  ON ES.ID_ESTADO          = E.ID_ESTADO
  LEFT JOIN REGIAO_PAIS            RP  ON RP.PAIS_REGIAO_ESTADO = ES.PAIS_REGIAO_ESTADO
  LEFT JOIN TIPO_CARTAO            TC  ON TC.ID_TIPO_CARTAO     = P.ID_TIPO_CARTAO
 
 