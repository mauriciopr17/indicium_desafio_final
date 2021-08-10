WITH PEDIDO_VENDA AS(   --**-- PEDIDOS CABELAÇHO / VENDA / DATA DE VENDA
                      SELECT ROW_NUMBER() OVER ( ORDER BY PC.SALESORDERID ) SK_PEDIDO_VENDA
                            ,PC.SALESORDERID 	     	 AS ID_PEDIDO_VENDA
                            ,PC.REVISIONNUMBER     	     AS NUMERO_REVISAO_PEDIDO
                            ,PC.ORDERDATE 		         AS DATA_PEDIDO
                            ,PC.DUEDATE 		    	 AS DATA_VENCIMENTO_PEDIDO
                            ,PC.SHIPDATE 		    	 AS DATA_ENVIO_PEDIDO
                            ,PC.STATUS
                            ,PC.ONLINEORDERFLAG   	     AS VENDA_ONLINE
                            ,PC.PURCHASEORDERNUMBER 	 AS NUMERO_PEDIDO_COMPRA
                            ,PC.ACCOUNTNUMBER 	 	     AS NUMERO_CONTA
                            ,PC.CUSTOMERID  		  	 AS ID_CLIENTE
                            ,PC.SALESPERSONID        	 AS ID_VENDEDOR
                            ,PC.TERRITORYID	      	     AS ID_TERRITORIO
                            ,PC.BILLTOADDRESSID	  	     AS ENDERECO_CONTA
                            ,PC.SHIPTOADDRESSID 	  	 AS ID_ENDERECO_ENTREGA
                            ,PC.SHIPMETHODID 	      	 AS ID_METODO_ENTREGA
                            ,PC.CREDITCARDID 	         AS ID_CARTAO_CREDITO
                            ,PC.CREDITCARDAPPROVALCODE   AS CODIGO_APROVACAO_CARTAO
                            ,PC.CURRENCYRATEID           AS ID_TAXA_CAMBIO
                            ,PC.SUBTOTAL 	   	         
                            ,PC.TAXAMT 	                 AS TAXA
                            ,PC.FREIGHT 		         AS FRETE
                            ,PC.TOTALDUE   	             AS TOTAL_DEVIDO
                        FROM {{ source('adventure_works', 'salesorderheader' )}} PC )
SELECT * FROM PEDIDO_VENDA P     
           
                       