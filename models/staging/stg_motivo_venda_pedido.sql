WITH MOTIVO_VENDA_PEDIDO AS ( SELECT MV.SALESORDERID   AS ID_PEDIDO_VENDA
								    ,MV.SALESREASONID  AS ID_MOTIVO_VENDA
							    FROM {{ source('adventure_works', 'salesorderheadersalesreason' )}} MV )
SELECT * FROM MOTIVO_VENDA_PEDIDO							  