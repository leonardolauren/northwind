with 
    fonte_ordem_detalhes as (
        select 
            cast(order_id as int) as id_pedido
            , cast(product_id as int) as id_produto
            , cast(unit_price as int) as preco_da_unidade
            , cast(quantity as int) as quantidade
            , cast(discount as numeric) as desconto_perc

        from {{ source('erp', 'order_details') }}
    )

select * from fonte_ordem_detalhes