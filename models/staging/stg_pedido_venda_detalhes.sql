WITH PEDIDO_VENDA_DETALHES AS ( 
                                  --**-- PEDIDOS DETALHES
                                  SELECT S.SALESORDERID 	       AS ID_PEDIDO_VENDA
                                        ,S.SALESORDERDETAILID 	   AS ID_PEDIDO_VENDA_DETALHES
                                        ,S.CARRIERTRACKINGNUMBER   AS NUMERO_RASTREAMENTO_OPERADORA
                                        ,S.ORDERQTY 			   AS QUANTIDADE_POR_PRODUTO
                                        ,S.PRODUCTID 	   	       AS ID_PRODUTO
                                        ,S.SPECIALOFFERID 	       AS OFERTA_ESPECIAL
                                        ,S.UNITPRICE 	  	       AS PRECO_UNITARIO
                                        ,S.UNITPRICEDISCOUNT 	   AS PRECO_UNITARIO_DESCONTO
                                    FROM {{ source('adventure_works', 'salesorderdetail' )}} S )
SELECT * FROM PEDIDO_VENDA_DETALHES